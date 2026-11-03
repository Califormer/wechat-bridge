# 贡献指南 / Contributing

## 中文

- 先读 README.md 与 docs/OPERATIONS.md，了解桥的边界（平台限制、
  凭据处理、备份口径）再动手。
- 改动前先跑一次主测试：`python3 tests/test_v5.py`。
  改动后跑日常回归集：`test_v5`、`test_v4`、`test_offline`、
  `test_resume`、`test_round2`、`test_hook`、`test_watchdog`。
  全部测试都是离线的，不连真实微信。
- `bridge.py` 目前是单文件设计：改动尽量保持单文件结构，
  不要为了拆分而拆分；执行者读代码时用 `bridge.py outline` 查索引。
- 不要提交任何运行时数据：`state/`、收件箱、历史、备份、二维码
  都在 `.gitignore` 里，提交前用 `git status` 核对。
- 文档与代码一起改：改了行为就改 README 里对应的命令或环境变量说明。

## English

- Read README.en.md and docs/OPERATIONS.en.md first, especially the
  boundaries (platform limits, credential handling, backup rules).
- Before changing anything, run the main suite once:
  `python3 tests/test_v5.py`. After your change, run the daily set:
  `test_v5`, `test_v4`, `test_offline`, `test_resume`, `test_round2`,
  `test_hook`, `test_watchdog`. All tests are offline and never touch
  real WeChat.
- `bridge.py` is intentionally a single file: keep that structure and
  do not split it just for the sake of splitting. Use
  `bridge.py outline` to navigate it.
- Never commit runtime data: `state/`, inbox, history, backups, and QR
  codes are all in `.gitignore`; check `git status` before committing.
- Change docs together with code: if behaviour changes, update the
  matching command or environment variable documentation.
