# ClaudeCode

用真实的 `claude` CLI 做 RCA agent 的适配层：`agent_runner.py` 每个 case 调一次 `claude -p`，模型由 `ANTHROPIC_BASE_URL` 指向的 Anthropic 协议端点提供，不是 Anthropic 官方模型。CLI 用自己的 Bash / Read 工具查遥测（本环境的 Python 带 duckdb）。

- 本项目自有代码，无上游，MIT（`LICENSE`）。

## harness 如何驱动

两份配置，都在父仓库 `harness/configs/agents/`：

- `claudecode-cmp.yaml`：框架对比实验（父仓库 `docs/framework_comparison.md`）。prepared 数据（`data_view` 默认的 `tables/`）、对比用 prompt `rca_compare.yaml`，每个请求经过 harness 的录制网关（`harness/scripts/gateway_run.py`），网关把 Anthropic Messages 转成 vLLM 的 chat completions，并强制主干模型的名字、采样参数和单次输出上限。不设轮次上限。
- `claudecode.yaml`：早期的原始 case 路径（`prepare_samples.py --question-data-path`），端点和 key 来自本目录 `.env`。

- stdin：JSON，字段 `question`、`system_prompt`、`user_prompt`、`compress_system_prompt`、`compress_user_prompt`、`data_dir`。`data_dir` 是一个表目录：`<name>.parquet` 或 `<name>/` 分片目录。
- stdout 最后一行：`output`（CausalGraph JSON）、`graph_valid`、`output_source`（direct / synthesis / regex_fallback / raw）、`trajectory`（OpenAI 格式，工具结果不截断）、`usage`（CLI 自报，`source: cli_reported`；实测值在网关）、`cli_result`（result 事件的 subtype、num_turns、terminal_reason、耗时、modelUsage）、`compactions`（CLI 自己的压缩次数）、`run_status`、`stop_reason`、`run_error`、`sandbox`、`claude_version`。
- 退出码：有答案就是 0，包括失败的 run（上下文超限、轮次上限）；CLI 没能运行（认证、端点、崩溃）为 1，harness 重试。

流程：

1. 从 prompt 中去掉自定义工具说明（`list_tables_in_directory` 等），CLI 用自己的工具。
2. 每个 case 建一个新工作区 `cc_case_*`（`CC_WORK_ROOT`，默认系统临时目录）：`data/` 是表文件的硬链接或副本（从不用软链接，软链接目标会暴露原 case 名），另有独立的 `home/`（`HOME`、`CLAUDE_CONFIG_DIR`）和 `tmp/`（`TMPDIR`）。prompt 里只出现工作区路径，并列出实际的表。运行结束后删除工作区（`CC_KEEP_WORKDIR=1` 保留）。
3. `claude -p ... --output-format stream-json --verbose --bare --no-session-persistence --dangerously-skip-permissions --tools Bash,Read --add-dir <data>`，cwd 是工作区（不在仓库内，`--bare` 也不读 CLAUDE.md）。CLI 可能用于后台工作的所有模型别名（haiku/sonnet/opus、subagent）都指向 `RCA_MODEL`；非必要流量、遥测、自动更新关闭。
4. `CC_SANDBOX=bwrap`（`auto` 在装了 bubblewrap 的 Linux 上即为 bwrap）时，CLI 在 bubblewrap 命名空间里运行：只读挂载系统目录、CLI 本身、带 duckdb 的 Python，读写挂载工作区，独立的 `/tmp`、`/proc`、`/dev`；网络共享，以便访问本机网关。模型的 Bash 因此读不到标签、prepared、其他框架的结果。`CC_REQUIRE_SANDBOX=1` 时没有 bwrap 就拒绝运行。
5. stream-json 完整写到 `$CAIRN_TRACE_DIR/claude_stream.jsonl`（网关下）或 `$RCA_EVENTS_DIR`，CLI 的 stderr 写到同目录 `claude_stderr.txt`。按 result 事件判断停止原因：`finish`、`context_length`（CLI 报 "Prompt is too long"）、`max_turns`、`api_error` 等，而不是按退出码（CLI 在这些情况下退出码为 1）。
6. CLI 的回答不是 CausalGraph JSON 时，用 `anthropic` SDK 经同一端点调一次提取（`X-Cairn-Purpose: synthesis`，计入辅助调用）；再失败就用正则从 markdown 提取（只认 TrainTicket 的 `ts-*` 和 mysql）。

## 环境

```bash
cd agents/ClaudeCode && uv sync --frozen --python 3.13   # 与 scripts/build_envs.sh 一致；含 duckdb
cp .env.example .env                                     # 只有 claudecode.yaml 需要：填 ANTHROPIC_API_KEY
```

另需 `claude` CLI 在 `PATH` 上（或用 `CLAUDE_BIN` 指定），对比实验另需 bubblewrap（`apt install bubblewrap`）。

变量（都可选）：`RCA_MODEL`、`CLAUDE_BIN`、`CLAUDE_TIMEOUT`（秒，0 不限，默认 1800）、`CC_TOOLS`（默认 `Bash,Read`；空则改用 `--allowedTools`/`--disallowedTools`）、`CC_MAX_TURNS`（默认不传 `--max-turns`）、`CC_MAX_OUTPUT_TOKENS`（→ `CLAUDE_CODE_MAX_OUTPUT_TOKENS`）、`CC_CONTEXT_WINDOW`（→ `CLAUDE_CODE_MAX_CONTEXT_TOKENS`，CLI 按它主动压缩）、`CC_MAX_THINKING_TOKENS`（→ `MAX_THINKING_TOKENS`）、`CC_COMPRESS_MAX_TOKENS`（默认 8000）、`CC_SANDBOX`、`CC_REQUIRE_SANDBOX`、`CC_SANDBOX_RO_PATHS`（额外只读路径，冒号分隔）、`CC_WORK_ROOT`、`CC_KEEP_WORKDIR`。`claudecode.yaml` 另用 `.env` 的 `ANTHROPIC_API_KEY`、`ANTHROPIC_BASE_URL_OVERRIDE`。

## 在 2.1.323 上实测的 CLI 行为（经网关，脚本化后端）

- 请求带 `thinking: adaptive`、`context_management`（只有 `clear_thinking keep=all`，等于无操作）和 `output_config.effort`，网关不转发后两者；`--bare` 加关闭非必要流量后，没有后台模型调用，也不调用 `count_tokens`。
- `CLAUDE_CODE_MAX_OUTPUT_TOKENS=4096` 使每个请求的 `max_tokens` 为 4096；`CLAUDE_CODE_MAX_CONTEXT_TOKENS` 设定 CLI 认为的窗口（`modelUsage.contextWindow`）。声明 32768 时，CLI 在约 17.5k 输入 token 处压缩（声明 24576 时约 9.4k，两次都比声明窗口低约 15k，是固定余量），压缩后在约 1.5k 的上下文上继续；这些数是脚本化后端的计数（JSON 长度 / 4），不是 Qwen 的真实 token，真实触发点要在服务器上量。压缩请求最后一条 user 消息以 "CRITICAL: Respond with TEXT ONLY" 开头，网关把它记为 `compaction`。
- 服务端拒绝超长请求（"prompt is too long"）时，CLI 先尝试压缩；单轮对话无法压缩时以 `is_error` 的 result 结束，退出码 1。
- `--max-turns` 不在 `--help` 里但可用；用尽时 result 的 subtype 为 `error_max_turns`，退出码 1。
- 工具参数不是合法 JSON 时，网关把它转成 `{"__invalid_arguments__": ...}`，CLI 以 InputValidationError 回给模型。

## 注意事项

- runner 固定使用 `--dangerously-skip-permissions`，模型能执行任意 Bash：对比实验必须用 bwrap；否则应在容器里运行。root 下 CLI 还要求 `IS_SANDBOX=1`（父仓库 `docker-compose.yml` 已设置）。容器里用 bwrap 需要允许创建命名空间（例如 seccomp/apparmor 放开）。
- CLI 的认证失败可能写在 stdout 的 stream-json 里；`_looks_like_cli_failure` 负责识别，命中时退出码非 0，不会当作模型输出保存。
- 本目录不依赖 `_rca_shared`。
