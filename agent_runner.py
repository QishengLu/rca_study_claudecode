#!/usr/bin/env python
"""agent_runner.py — the real Claude Code CLI as an RCA agent.

stdin:  JSON { question, system_prompt, user_prompt, compress_system_prompt, compress_user_prompt, data_dir }
        data_dir is a directory of telemetry tables: <name>.parquet files or <name>/ directories of parts (a
        prepared sandbox's tables/, or an older data_<hex> sandbox).
stdout: JSON { output (CausalGraph JSON), graph_valid, output_source, trajectory (OpenAI format), usage,
               cli_result, compactions, run_status, stop_reason, run_error, sandbox, claude_version }
exit:   0 with an answer (also a failed run: context too long, turn limit); 1 when the CLI or the model service
        failed (authentication, transport, a crash): the harness retries those.

Each case runs `claude -p` once, in a fresh workspace (data copy, HOME, CLAUDE_CONFIG_DIR, TMPDIR) that is removed
afterwards, so concurrent cases share no CLI state. The model is whatever Anthropic-protocol endpoint
ANTHROPIC_BASE_URL names: under Cairn's comparison gateway that is a translation to the vLLM backbone, and every
model alias the CLI might use for background work is pointed at RCA_MODEL. With CC_SANDBOX=bwrap the CLI runs in
a bubblewrap namespace that sees only system directories, the CLI itself, the Python with duckdb, and the case
workspace: the model's Bash cannot reach labels or other runs on the same machine.

Environment (all optional):
  RCA_MODEL                 model name sent to the endpoint (default qwen3.5-plus)
  CLAUDE_BIN                the CLI (default `claude` on PATH)
  CLAUDE_TIMEOUT            seconds; 0 = no own limit (default 1800)
  CC_TOOLS                  built-in tools offered to the model (default "Bash,Read"; empty: do not pass --tools)
  CC_MAX_TURNS              pass --max-turns (default: no limit)
  CC_MAX_OUTPUT_TOKENS      -> CLAUDE_CODE_MAX_OUTPUT_TOKENS (per-request output cap)
  CC_CONTEXT_WINDOW         -> CLAUDE_CODE_MAX_CONTEXT_TOKENS (the window the CLI compacts against)
  CC_MAX_THINKING_TOKENS    -> MAX_THINKING_TOKENS
  CC_COMPRESS_MAX_TOKENS    output cap of the synthesis call (default 8000)
  CC_SANDBOX                auto (bwrap when available on Linux) | bwrap | none (default auto)
  CC_REQUIRE_SANDBOX=1      refuse to run without bwrap
  CC_SANDBOX_RO_PATHS       extra read-only paths for the sandbox, colon-separated
  CC_WORK_ROOT              where case workspaces are created (default the system temp dir)
  CC_KEEP_WORKDIR=1         keep the workspace (debugging)
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:  # narrow: never swallow a real .env parse error
    print(
        "[ClaudeCode] python-dotenv is missing, so agents/ClaudeCode/.env will NOT be "
        "read. Run `uv sync` in agents/ClaudeCode/.",
        file=sys.stderr,
    )


# ── 配置 ────────────────────────────────────────────────────────────────────

CLAUDE_BIN = os.environ.get("CLAUDE_BIN", "claude")
RCA_MODEL = os.environ.get("RCA_MODEL", "qwen3.5-plus")
TIMEOUT = int(os.environ.get("CLAUDE_TIMEOUT", "1800")) or None
PURPOSE_HEADER = "X-Cairn-Purpose"
# Variables that could make the CLI authenticate somewhere else or load user configuration.
_DROPPED_ENV = ("ANTHROPIC_AUTH_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN", "CLAUDE_CODE_USE_BEDROCK",
                "CLAUDE_CODE_USE_VERTEX", "ANTHROPIC_CUSTOM_HEADERS", "CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT")


def base_url() -> str:
    """The Anthropic-protocol endpoint: the gateway's (ANTHROPIC_BASE_URL) or ANTHROPIC_BASE_URL_OVERRIDE."""
    return os.environ.get("ANTHROPIC_BASE_URL") or os.environ.get("ANTHROPIC_BASE_URL_OVERRIDE", "")


# ── 工具函数 ─────────────────────────────────────────────────────────────────


def strip_markdown_json(text: str) -> str:
    """剥离 LLM 返回的 ```json ... ``` 代码块，提取纯 JSON。"""
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        return m.group(1).strip()
    return text.strip()


def is_graph(text: str) -> bool:
    try:
        obj = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return False
    return isinstance(obj, dict) and ("root_causes" in obj or "nodes" in obj)


def _extract_causal_graph_from_markdown(text: str) -> str | None:
    """从 markdown 格式的分析报告中提取 root cause，构建最小合法 CausalGraph JSON。

    当模型输出 markdown 而非 JSON 时作为 fallback。只认 TrainTicket 的 ts-* 服务名和 mysql。
    """
    if is_graph(text):
        return text
    root_causes = []
    for m in re.finditer(
        r"[Rr]oot\s+[Cc]ause[^{}\n]{0,80}?[\*`]+(ts-[\w-]+|mysql|ts-rabbitmq)[\*`]+",
        text[:3000],
    ):
        svc = m.group(1).strip()
        if svc not in root_causes:
            root_causes.append(svc)
    if not root_causes:
        m = re.search(r"\*\*(ts-[\w-]+)\*\*", text[:2000])
        if m:
            root_causes.append(m.group(1))
    if not root_causes:
        return None
    all_services = set(root_causes)
    for m in re.finditer(r"(ts-[\w-]+)", text):
        all_services.add(m.group(1))
    all_services -= {"ts-ui-dashboard"}  # 网关，不是独立故障源
    nodes = [{"component": svc, "state": ["HIGH_ERROR_RATE"]} for svc in sorted(all_services)]
    edges = [{"source": rc, "target": svc} for rc in root_causes for svc in sorted(all_services) if svc != rc]
    graph = {"nodes": nodes, "edges": edges,
             "root_causes": [{"component": rc, "state": ["HIGH_ERROR_RATE"]} for rc in root_causes],
             "component_to_service": {}}
    return json.dumps(graph, ensure_ascii=False)


def parse_stream_events(jsonl_path: str):
    """stream-json -> (result text, OpenAI-format trajectory, CLI usage, result event, compactions).

    Tool results are kept whole: the trajectory is the record of what the model saw.
    """
    trajectory = []
    result_text = ""
    usage = {}
    result_event = None
    compactions = {"started": 0, "failed": 0}

    with open(jsonl_path, "r", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            event_type = event.get("type", "")

            if event_type == "assistant":
                for block in (event.get("message") or {}).get("content", []) or []:
                    block_type = block.get("type", "")
                    if block_type == "tool_use":
                        trajectory.append({
                            "role": "assistant", "content": "",
                            "tool_calls": [{"id": block.get("id", ""), "type": "function",
                                            "function": {"name": block.get("name", ""),
                                                         "arguments": json.dumps(block.get("input", {}),
                                                                                 ensure_ascii=False)}}],
                        })
                    elif block_type == "text":
                        text = block.get("text", "")
                        if text.strip():
                            trajectory.append({"role": "assistant", "content": text})
                    elif block_type == "thinking":
                        thinking = block.get("thinking", "")
                        if thinking.strip():
                            trajectory.append({"role": "assistant", "content": f"[thinking] {thinking}"})

            elif event_type == "user":
                for block in (event.get("message") or {}).get("content", []) or []:
                    if isinstance(block, dict) and block.get("type") == "tool_result":
                        content = block.get("content", "")
                        if isinstance(content, list):
                            content = "\n".join(b.get("text", "") for b in content if isinstance(b, dict))
                        trajectory.append({"role": "tool", "content": str(content),
                                           "tool_call_id": block.get("tool_use_id", "")})

            elif event_type == "system" and event.get("subtype") == "status":
                if event.get("status") == "compacting":
                    compactions["started"] += 1
                if event.get("compact_result") == "failed":
                    compactions["failed"] += 1

            elif event_type == "result":
                result_event = event
                result_text = event.get("result") or ""
                raw_usage = event.get("usage") or {}
                prompt = (raw_usage.get("input_tokens", 0) + raw_usage.get("cache_creation_input_tokens", 0)
                          + raw_usage.get("cache_read_input_tokens", 0))
                usage = {
                    "total_tokens": prompt + raw_usage.get("output_tokens", 0),
                    "prompt_tokens": prompt,
                    "completion_tokens": raw_usage.get("output_tokens", 0),
                    "reasoning_tokens": 0,
                    "llm_call_count": event.get("num_turns", 1),
                    "cost_usd": event.get("total_cost_usd", 0.0),
                    "source": "cli_reported",  # the gateway's counts are the measured ones
                }

    return result_text, trajectory, usage, result_event, compactions


_CLI_FAILURE_MARKERS = (
    "not logged in",
    "please run /login",
    "authenticationerror",
    "authentication failed",
    "authentication_failed",
    "api error 401",
    "api error 403",
    "invalid api key",
    "invalid access token",
    "token expired",
    "unauthorized",
)


def _looks_like_cli_failure(text: str) -> bool:
    lowered = (text or "").lower()
    if any(marker in lowered for marker in _CLI_FAILURE_MARKERS):
        return True
    if re.search(r'"error_status"\s*:\s*40[13]\b', lowered):
        return True
    return bool(
        re.search(
            r"\b(?:401|403)\b.{0,160}\b(?:auth(?:entication)?|token|unauthori[sz]ed)\b",
            lowered,
        )
    )


def _looks_like_api_failure(text: str) -> bool:
    """The model service failed (not the model's answer): a 5xx, overload or connection error the CLI gave up on."""
    lowered = (text or "").lower()
    return bool(re.search(r"api error:? ?(5\d\d|529)|overloaded|connection error|econnrefused|fetch failed|"
                          r"request timed out", lowered))


def _compress_to_json(result_text: str, trajectory: list, compress_sp: str, compress_up: str, usage: dict) -> str | None:
    """Ask the model, through the same endpoint, to turn the CLI's final text and the trajectory into CausalGraph JSON.

    Not part of Claude Code: the adapter's own extraction step when the CLI's answer is not a graph. It is an
    auxiliary call (purpose `synthesis`), counted with the rest. Returns None when it fails.
    """
    investigation_summary = []
    for msg in trajectory:
        role = msg.get("role", "")
        content = msg.get("content", "")
        if role == "assistant" and content and not content.startswith("[thinking]"):
            investigation_summary.append(f"[Assistant] {content[:1000]}")
        elif role == "assistant" and msg.get("tool_calls"):
            fn = msg["tool_calls"][0]["function"]
            investigation_summary.append(f"[Tool Call] {fn['name']} {fn['arguments'][:500]}")
        elif role == "tool":
            investigation_summary.append(f"[Tool Result] {str(content)[:500]}")
    investigation_summary.append(f"\n[Final Analysis]\n{result_text}")
    investigation_context = "\n\n".join(investigation_summary[-30:])
    compress_prompt = f"{investigation_context}\n\n---\n\n{compress_up}"

    import anthropic

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"], base_url=base_url(),
                                 default_headers={PURPOSE_HEADER: "synthesis"})
    try:
        print("[ClaudeCode] Compress: asking the model for CausalGraph JSON...", file=sys.stderr)
        resp = client.messages.create(
            model=RCA_MODEL, system=compress_sp,
            max_tokens=int(os.environ.get("CC_COMPRESS_MAX_TOKENS", "8000")),
            messages=[{"role": "user", "content": compress_prompt}],
        )
        raw = next((block.text for block in resp.content or [] if getattr(block, "type", "") == "text"), "")
        if resp.usage:
            usage["prompt_tokens"] = usage.get("prompt_tokens", 0) + resp.usage.input_tokens
            usage["completion_tokens"] = usage.get("completion_tokens", 0) + resp.usage.output_tokens
            usage["total_tokens"] = usage["prompt_tokens"] + usage["completion_tokens"]
            usage["llm_call_count"] = usage.get("llm_call_count", 0) + 1
        extracted = strip_markdown_json(raw)
        if is_graph(extracted):
            print("[ClaudeCode] Compress: success", file=sys.stderr)
            return extracted
        print("[ClaudeCode] Compress: no CausalGraph in the answer", file=sys.stderr)
    except Exception as e:  # noqa: BLE001 - the extraction is best effort; the run's own answer stays
        print(f"[ClaudeCode] Compress failed: {type(e).__name__}: {e}", file=sys.stderr)
    return None


# ── 工作区与沙箱 ──────────────────────────────────────────────────────────────


def _strip_tool_instructions(sp: str) -> str:
    """移除 system prompt 中的 <Available Tools> 段落和工具调用指引（CLI 用自己的工具）。"""
    sp = re.sub(r"<Available Tools>.*?</Available Tools>\s*", "", sp, flags=re.DOTALL)
    sp = re.sub(r".*(?:list_tables_in_directory|get_schema|query_parquet_files|think_tool).*\n?", "", sp)
    sp = re.sub(r".*Call this (?:FIRST|SECOND).*\n?", "", sp)
    sp = re.sub(r"\n{3,}", "\n\n", sp)
    return sp.strip()


def _strip_tool_instructions_up(up: str) -> str:
    """移除 user prompt 中的工具调用指引，保留调查策略和输出格式。"""
    up = re.sub(r".*(?:list_tables_in_directory|get_schema|query_parquet_files|think_tool).*\n?", "", up)
    up = re.sub(r"\s*- Call `[^`]+`.*\n?", "", up)
    up = re.sub(r"\n{3,}", "\n\n", up)
    return up.strip()


def _place(src: Path, dst: Path) -> None:
    """A file's content at dst: a hard link to the real file when possible, else a copy. Never a symlink, whose
    target would name the source case."""
    real = src.resolve()
    try:
        os.link(real, dst)
    except OSError:
        shutil.copy2(real, dst)


def build_workspace(data_dir: str) -> tuple[Path, Path, list[str]]:
    """(workspace, its data directory, table names). The data directory holds the case's tables only."""
    root = os.environ.get("CC_WORK_ROOT") or None
    if root:
        os.makedirs(root, exist_ok=True)
    workspace = Path(tempfile.mkdtemp(prefix="cc_case_", dir=root)).resolve()
    data = workspace / "data"
    data.mkdir()
    for sub in ("home", "tmp"):
        (workspace / sub).mkdir()
    tables = []
    source = Path(data_dir)
    for entry in sorted(source.iterdir()):
        if entry.name.endswith(".parquet") and entry.is_file():
            _place(entry, data / entry.name)
            tables.append(entry.name)
        elif entry.is_dir() and any(entry.glob("*.parquet")):
            (data / entry.name).mkdir()
            for part in sorted(entry.glob("*.parquet")):
                _place(part, data / entry.name / part.name)
            tables.append(entry.name + "/")
    if not tables:
        raise FileNotFoundError(f"no parquet tables in {data_dir}")
    return workspace, data, tables


def data_prompt(data: Path, tables: list[str]) -> str:
    listing = "\n".join(f"- `{t}`" + (" (a directory of parquet parts)" if t.endswith("/") else "") for t in tables)
    return (
        "\n\n## Data Location\n\n"
        f"The telemetry data for this incident is in `{data}`: {len(tables)} parquet tables.\n{listing}\n\n"
        "Query them with DuckDB SQL, for example "
        "`python3 -c \"import duckdb; print(duckdb.sql('SELECT * FROM read_parquet(\\'<file>\\') LIMIT 5'))\"`"
        + (" or the `duckdb` command" if shutil.which("duckdb") else "") + ".\n"
        "All your evidence must come from these files. Do NOT attempt to access any other directories or files."
    )


def sandbox_mode() -> str:
    mode = os.environ.get("CC_SANDBOX", "auto")
    if mode == "auto":
        mode = "bwrap" if sys.platform.startswith("linux") and shutil.which("bwrap") else "none"
    if mode == "bwrap" and not shutil.which("bwrap"):
        raise RuntimeError("CC_SANDBOX=bwrap but bwrap is not installed")
    if mode == "none" and os.environ.get("CC_REQUIRE_SANDBOX") == "1":
        raise RuntimeError("CC_REQUIRE_SANDBOX=1 but no sandbox is available (install bubblewrap)")
    return mode


def bwrap_prefix(workspace: Path, claude: Path) -> list[str]:
    """bubblewrap arguments: system directories, the CLI, this Python (with duckdb) read-only; the workspace
    read-write; a private /tmp, /proc and /dev; the network is shared so the CLI reaches the local gateway."""
    args = ["bwrap", "--die-with-parent", "--new-session", "--unshare-pid", "--unshare-ipc", "--unshare-uts",
            "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"]
    for path in ("/usr", "/bin", "/sbin", "/lib", "/lib64", "/lib32", "/etc", "/opt/uv"):
        args += ["--ro-bind-try", path, path]
    python = Path(sys.executable)
    read_only = {claude.parent, python.parent, Path(os.path.realpath(python)).parent.parent,
                 Path(sys.prefix), Path(sys.base_prefix)}
    duckdb_cli = shutil.which("duckdb")
    if duckdb_cli:
        read_only.add(Path(os.path.realpath(duckdb_cli)).parent)
    read_only.update(Path(p) for p in os.environ.get("CC_SANDBOX_RO_PATHS", "").split(":") if p)
    for path in sorted(read_only):
        args += ["--ro-bind-try", str(path), str(path)]
    args += ["--bind", str(workspace), str(workspace), "--chdir", str(workspace), "--"]
    return args


def claude_env(workspace: Path) -> dict:
    env = {k: v for k, v in os.environ.items() if k not in _DROPPED_ENV}
    env.update({
        "ANTHROPIC_BASE_URL": base_url(),
        "ANTHROPIC_MODEL": RCA_MODEL,
        # Every alias the CLI may use for background work answers with the same model.
        "ANTHROPIC_DEFAULT_HAIKU_MODEL": RCA_MODEL, "ANTHROPIC_DEFAULT_SONNET_MODEL": RCA_MODEL,
        "ANTHROPIC_DEFAULT_OPUS_MODEL": RCA_MODEL, "ANTHROPIC_SMALL_FAST_MODEL": RCA_MODEL,
        "CLAUDE_CODE_SUBAGENT_MODEL": RCA_MODEL,
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1", "DISABLE_TELEMETRY": "1", "DISABLE_AUTOUPDATER": "1",
        "DISABLE_ERROR_REPORTING": "1",
        "HOME": str(workspace / "home"), "CLAUDE_CONFIG_DIR": str(workspace / "home" / ".claude"),
        "TMPDIR": str(workspace / "tmp"),
        # `python3` in the model's Bash is this environment's Python, which has duckdb.
        "PATH": os.path.dirname(sys.executable) + os.pathsep + os.environ.get("PATH", ""),
    })
    for ours, theirs in (("CC_MAX_OUTPUT_TOKENS", "CLAUDE_CODE_MAX_OUTPUT_TOKENS"),
                         ("CC_CONTEXT_WINDOW", "CLAUDE_CODE_MAX_CONTEXT_TOKENS"),
                         ("CC_MAX_THINKING_TOKENS", "MAX_THINKING_TOKENS")):
        if os.environ.get(ours):
            env[theirs] = os.environ[ours]
    return env


def claude_version(claude: Path) -> str | None:
    try:
        done = subprocess.run([str(claude), "--version"], capture_output=True, text=True, timeout=60)
        return done.stdout.strip() or None
    except (OSError, subprocess.TimeoutExpired):
        return None


# ── 主流程 ────────────────────────────────────────────────────────────────────


def main():
    if not (os.environ.get("ANTHROPIC_API_KEY") or "").strip():
        print("[ClaudeCode] ANTHROPIC_API_KEY is empty; fill agents/ClaudeCode/.env or run behind Cairn's gateway",
              file=sys.stderr)
        sys.exit(1)
    found = shutil.which(CLAUDE_BIN)
    if not found:
        print(f"[ClaudeCode] {CLAUDE_BIN} not found on PATH", file=sys.stderr)
        sys.exit(1)
    claude = Path(os.path.realpath(found))
    try:
        mode = sandbox_mode()
    except RuntimeError as exc:
        print(f"[ClaudeCode] {exc}", file=sys.stderr)
        sys.exit(1)

    payload = json.loads(sys.stdin.read())
    system_prompt = _strip_tool_instructions(payload["system_prompt"])
    user_prompt = _strip_tool_instructions_up(payload["user_prompt"])
    data_dir = payload.get("data_dir", "")
    workspace, data, tables = build_workspace(data_dir)
    # No path of the case reaches the model: the prompt names the workspace copy only.
    system_prompt = system_prompt.replace(data_dir, str(data))
    user_prompt = user_prompt.replace(data_dir, str(data)) + data_prompt(data, tables)

    log_root = Path(os.environ.get("CAIRN_TRACE_DIR") or os.environ.get("RCA_EVENTS_DIR") or workspace)
    log_root.mkdir(parents=True, exist_ok=True)
    stream_file = log_root / "claude_stream.jsonl"
    stderr_file = log_root / "claude_stderr.txt"

    cmd = [str(claude), "-p", user_prompt, "--model", RCA_MODEL, "--system-prompt", system_prompt,
           "--output-format", "stream-json", "--verbose", "--bare", "--no-session-persistence",
           "--dangerously-skip-permissions", "--add-dir", str(data)]
    tools = os.environ.get("CC_TOOLS", "Bash,Read")
    if tools:
        cmd += ["--tools", tools]
    else:
        cmd += ["--allowedTools", "Bash(*)", "Read(*)", "--disallowedTools", "Write(*)", "Edit(*)"]
    if os.environ.get("CC_MAX_TURNS"):
        cmd += ["--max-turns", os.environ["CC_MAX_TURNS"]]
    if mode == "bwrap":
        cmd = bwrap_prefix(workspace, claude) + cmd
    version = claude_version(claude)
    print(f"[ClaudeCode] Starting: model={RCA_MODEL}, {version}, sandbox={mode}, tables={len(tables)}",
          file=sys.stderr)

    start = time.time()
    try:
        with open(stream_file, "w") as out_f, open(stderr_file, "w") as err_f:
            proc = subprocess.run(cmd, stdout=out_f, stderr=err_f, stdin=subprocess.DEVNULL, cwd=workspace,
                                  env=claude_env(workspace), timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        print(f"[ClaudeCode] Timed out after {TIMEOUT}s", file=sys.stderr)
        sys.exit(1)
    finally:
        if os.environ.get("CC_KEEP_WORKDIR") != "1":
            shutil.rmtree(workspace, ignore_errors=True)
    elapsed = time.time() - start
    cli_stderr = stderr_file.read_text(errors="replace")
    print(f"[ClaudeCode] Finished in {elapsed:.1f}s, exit={proc.returncode}", file=sys.stderr)

    result_text, trajectory, usage, result_event, compactions = parse_stream_events(str(stream_file))
    stream_text = stream_file.read_text(errors="replace")
    if result_event is None:
        # No result at all: the CLI could not run (authentication, the endpoint, a crash). Infrastructure.
        if _looks_like_cli_failure(cli_stderr) or _looks_like_cli_failure(stream_text):
            print("[ClaudeCode] CLI authentication/API failure; refusing to store it as model output",
                  file=sys.stderr)
        print(f"[ClaudeCode] stderr: {cli_stderr[-1000:]}", file=sys.stderr)
        print(f"[ClaudeCode] stdout (stream-json) tail: {stream_text[-500:]}", file=sys.stderr)
        sys.exit(1)

    subtype = result_event.get("subtype")
    is_error = bool(result_event.get("is_error"))
    if is_error and (_looks_like_cli_failure(result_text) or _looks_like_cli_failure(cli_stderr)):
        print("[ClaudeCode] CLI authentication failure; refusing to store it as model output", file=sys.stderr)
        sys.exit(1)
    if is_error and _looks_like_api_failure(result_text) and int(usage.get("llm_call_count") or 0) <= 1:
        print(f"[ClaudeCode] model service failed before any answer: {result_text[:300]}", file=sys.stderr)
        sys.exit(1)

    # Why the CLI stopped: its own finish, its context window, a turn limit, or an execution error.
    if not is_error and subtype == "success":
        stop_reason = "finish"
    elif "prompt is too long" in (result_text or "").lower():
        stop_reason = "context_length"
    elif subtype == "error_max_turns":
        stop_reason = "max_turns"
    elif _looks_like_api_failure(result_text):
        stop_reason = "api_error"
    else:
        stop_reason = subtype or "error"
    run_error = None if stop_reason == "finish" else (result_text or "; ".join(result_event.get("errors") or []))[:1000]

    # The causal graph: the CLI's answer as JSON, else the extraction call, else the markdown fallback.
    cleaned = strip_markdown_json(result_text) if stop_reason == "finish" else ""
    output, source = (cleaned, "direct") if is_graph(cleaned) else (None, None)
    if output is None and payload.get("compress_system_prompt") and payload.get("compress_user_prompt"):
        compressed = _compress_to_json(result_text if stop_reason == "finish" else "", trajectory,
                                       payload["compress_system_prompt"], payload["compress_user_prompt"], usage)
        if compressed:
            output, source = compressed, "synthesis"
    if output is None and stop_reason == "finish":
        fallback = _extract_causal_graph_from_markdown(result_text)
        if fallback:
            print("[ClaudeCode] Fallback: regex extraction from markdown", file=sys.stderr)
            output, source = fallback, "regex_fallback"
    if output is None:
        output, source = cleaned, "raw"
    graph_valid = is_graph(output)

    print(json.dumps({
        "output": output,
        "graph_valid": graph_valid,
        "output_source": source,
        "trajectory": trajectory,
        "usage": usage,
        "cli_result": {k: result_event.get(k) for k in ("subtype", "is_error", "num_turns", "terminal_reason",
                                                         "stop_reason", "duration_ms", "duration_api_ms",
                                                         "total_cost_usd", "modelUsage", "errors")},
        "compactions": compactions,
        "run_status": "completed" if graph_valid and stop_reason in ("finish", "context_length") else (
            "completed" if stop_reason == "finish" else "failed"),
        "stop_reason": stop_reason,
        "run_error": run_error,
        "sandbox": mode,
        "claude_version": version,
        "cli_returncode": proc.returncode,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
