# ClaudeCode

用 `claude` CLI 做 RCA agent 的适配层：`agent_runner.py` 调 `claude -p`，模型由一个 Anthropic 协议端点提供（默认 `qwen3.5-plus`），不是 Anthropic 官方模型。CLI 自带的 Bash / Read 工具用 `duckdb` 查 parquet。

- 本项目自有代码，无上游，MIT（`LICENSE`）。

## harness 如何驱动

配置在父仓库 `harness/configs/agents/claudecode.yaml`：`.venv/bin/python agent_runner.py`，cwd 为本目录，`question_data_path: true`，单例超时 1800 秒，并发 5。

- stdin：JSON，字段 `question`、`system_prompt`、`user_prompt`、`compress_system_prompt`、`compress_user_prompt`、`data_dir`。
- stdout 最后一行：JSON，字段 `output`（CausalGraph JSON）、`trajectory`（OpenAI 格式）、`usage`。
- `data_dir` 是 harness 从 question 末尾 "The telemetry data is stored in: `<sandbox>`" 解析出的目录，形如 `data_<hex>`，里面只有遥测表的硬链接。

流程：

1. 从 prompt 中去掉自定义工具说明（`list_tables_in_directory` 等），CLI 用自己的工具。
2. 若 `data/<data_hex 目录名>` 存在就直接用；否则把 10 张白名单 parquet 复制到临时目录 `rca_sandbox_*`，并把 prompt 里的路径替换成它。
3. 运行 `claude -p ... --output-format stream-json --bare --dangerously-skip-permissions --add-dir <sandbox>`，禁用 Write / Edit，`CLAUDE_CONFIG_DIR` 按进程隔离。
4. 解析 stream-json 得到 trajectory 和 usage。结果不是 CausalGraph JSON 时，用 `anthropic` SDK 调一次 compress；再失败就用正则从 markdown 提取。

## 环境

```bash
cd agents/ClaudeCode && uv sync --frozen --python 3.13   # 与 scripts/build_envs.sh 一致
cp .env.example .env                                     # 填 ANTHROPIC_API_KEY
```

另需 `claude` CLI 在 `PATH` 上（或用 `CLAUDE_BIN` 指定）。

`.env` 字段：

- `ANTHROPIC_API_KEY`：缺失时 runner 直接退出。
- `ANTHROPIC_BASE_URL_OVERRIDE`：Anthropic 协议端点，必须能提供 `qwen3.5-plus`。默认百炼 Coding Plan 端点，也可用 `https://dashscope.aliyuncs.com/apps/anthropic` 配 Model Studio key。
- `RCA_MODEL`：默认 `qwen3.5-plus`。
- `CLAUDE_BIN`、`CLAUDE_TIMEOUT`：可选。

## 数据集覆盖

只跑 RCABench（`prepare_samples.py --question-data-path`）。复制白名单固定为 `{normal,abnormal}_{logs,traces,metrics,metrics_histogram,metrics_sum}` 共 10 张表，追加的 prompt 也写明 10 个文件；markdown 兜底的正则只认 TrainTicket 的 `ts-*` 服务名。RCA100 不适用。

## 注意事项

- runner 固定使用 `--dangerously-skip-permissions`，模型能在本机执行任意 Bash，应在容器里运行；root 下 CLI 还要求 `IS_SANDBOX=1`（父仓库 `docker-compose.yml` 已设置）。
- 只有路径前缀是 `/tmp/rca_sandbox_` 的临时目录才会被清理。macOS 的临时目录不在 `/tmp` 下，每个 case 的副本会残留，需要手动清理或设 `TMPDIR=/tmp`。
- CLI 的认证失败可能写在 stdout 的 stream-json 里；`_looks_like_cli_failure` 负责识别，命中时退出码非 0，不会当作模型输出保存。
- 本目录不依赖 `_rca_shared`。
