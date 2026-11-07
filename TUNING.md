# 线上调参（TUNING.md）

这是线上桥 `state/tuning.env` 在 2026-11-05 的内容快照，**不是凭证**，只有两条调参。
包里不要原样放 `state/tuning.env`（那是 state/ 路径），也不要把这两个值写死进代码——
tuning 仍是唯一的配置源，代码读不到它时用默认值（warm 45 秒、linger 0）。

```ini
# Step 3 experiment, 2026-11-05: single variable only.
# Hook linger 0 -> 3000ms. Warm wait stays 90s (judged in test 1, Step 2 done).
# Echo stays observe (no resolve, no auto-resend).
WECHAT_BRIDGE_WARM_WAIT_S=90
WECHAT_BRIDGE_HOOK_LINGER_MS=3000
```

## 每条是什么意思（2026-11-05 线上实测定案）

- `WECHAT_BRIDGE_WARM_WAIT_S=90`：热等待 90 秒。测试 1 验证过边界——70 秒的追问被 warm 接住、
  100 秒的走了 cold，正好卡在 90 秒边界上；warm 唤醒 p50 1203ms，比 cold 快约 3.2 秒。保留 90 秒，不要改回 45 秒。
- `WECHAT_BRIDGE_HOOK_LINGER_MS=3000`：hook 停留 3000 毫秒。Step 3 单变量实验后保留——
  5 条冷消息唤醒中位数 2632ms（基线 4731ms），4 条在收到后 50–103ms 触发 rerun，周期仍是 5 秒没变 8 秒。
- 回显 `observe`：只观察，不 resolve、不自动重发（线上 41 发、0 回显，这个账号不回显）。

## 注意：简报里的 Step 3 已过期

包根目录的任务简报 `Claude-WeChatBridge.md` 是 2026-11-05 10:34 的版本，
其中 Step 3 写的是“包里只加开关、默认仍为 0（关），部署后在微信里测”。
那是实验前的计划；实验当天已做完，linger 3000 已定案保留（简报 §8 开头也写了这一句）。
**以本文件（线上当前值）为准**，不要按简报 Step 3 的“默认 0”改回去。
同理，简报里 Step 2 写的 hook 提示词是 1824 字版（sha `3930ce58…`），
包里 `hooks/wechat-bridge-inbox.prompt.txt` 已是 Step01 部署后的 2134 字版（sha `668c201e…`），以包里文件为准。

## 2026-11-05 §8 审计包（本包）

- 本包**不改 tuning**：线上仍是热等待 90 秒、停留 3000 ms；代码默认仍是 45 秒 / 0。包里不放 `state/tuning.env`。
- EXECUTOR.md 第 9 行和 hook 提示词第 11 行现在写“代码默认 45 秒、线上 90 秒”。`tests/test_audit_20261105.py` 会拿 `state/tuning.env`（没有时用本文件）里的 `WECHAT_BRIDGE_WARM_WAIT_S` 核对这句话：以后改热等待，要同时改这两处文字（提示词还要按 OPS.md 末节同步三处 + 线上定义），否则这项测试会提醒。
- 停留 3000 → 4000（简报 §8 第 6 条）是可选的单变量实验，不在本包：只改 `state/tuning.env` 一个值，先看 `hook_ticks.skipped_ticks` 有没有涨，再拿新样本判断。

## 2026-11-06 三项新功能包（批处理 / 可打断 / 失败文本补发）

- 本包**不改 tuning**：`state/tuning.env` 仍是上面两条。新功能的默认值写在代码里，就是需求定的数：停顿 3 秒、上限 60 秒。
- 新键（需要时才写进 tuning.env；serve 重启生效，CLI 立即生效）：`WECHAT_BRIDGE_BATCH_QUIET_MS`（默认 3000）、`WECHAT_BRIDGE_BATCH_MAX_MS`（默认 60000）、`WECHAT_BRIDGE_BATCH=0`（应急关掉批处理和打断，逐条立即派发、没有任何合并）。每批记住开批时的值，改了只影响新开的批。
- 删掉的键：`WECHAT_BRIDGE_SETTLE_SHORT_MS`（claim 合并窗口，已删除；写在 tuning.env 里也不再起作用，留着无害）。

## 长任务接力与 §8（2026-11-07）

包里不改 `state/tuning.env`；下面是代码默认值。要改就在 tuning.env 加一行，每次只改一个变量。

| 变量 | 默认 | 作用 |
|---|---|---|
| `WECHAT_BRIDGE_RELAY` | 1 | 0 = 关掉整个接力（无 long_hint / plan，--plan / --continue 被拒，回到 5.0.5 行为） |
| `WECHAT_BRIDGE_RELAY_STEP_MAX_MIN` | 7 | 每步、每棒的分钟预算（600 秒回合留 3 分钟余量） |
| `WECHAT_BRIDGE_RELAY_MAX_LEGS` | 8 | 一条任务最多接力几棒 |
| `WECHAT_BRIDGE_RELAY_MAX_MIN` | 60 | 一条任务接力的总时长上限（分钟） |
| `WECHAT_BRIDGE_RELAY_NOTIFY_GAP_MIN` | 10 | 交棒时进度通知的最小间隔（分钟；0 = 不发） |
| `WECHAT_BRIDGE_PLAN_KEEP_DAYS` | 14 | 计划和 .d 工作目录保留天数 |
| `WECHAT_BRIDGE_LONG_HINT_WORDS` | 打包,脱敏,去敏,开源,全套测试,全量测试,run_all,批量,逐项,逐个文件,扫描,所有文件,全部文件,每个文件 | 判长词表（逗号分隔） |
| `WECHAT_BRIDGE_LONG_HINT_MIN_FILES` | 5 | 「N 个文件」N 达到多少算长 |
| `WECHAT_BRIDGE_LONG_HINT_FILE_MB` | 20 | 收到的文件多大算长（压缩包一律算长） |
| `WECHAT_BRIDGE_PEEK` | 1 | 0 = claim 不带 file_peek |
| `WECHAT_BRIDGE_PEEK_CHARS` | 1500 | file_peek 每个文件最多带多少字（另限 40 行） |
| `WECHAT_BRIDGE_PEEK_ROOTS` | （空） | 桥目录之外还允许 peek 的目录，冒号分隔 |
| `WECHAT_BRIDGE_CMD_STATS` | 1 | 0 = 「统计」不由桥回答，照常交给 Muse |
| `WECHAT_BRIDGE_CLI_LOG` | 1 | 0 = 不写 logs/cli_calls.log（stats 就没有 cli_calls） |

热等待要不要从 90 秒加到 120 / 180 秒：先看线上 `python3 wb stats` 的 `evidence.followup_gaps.whatif`（各窗口能接住的追问比例 `caught`、每条回复要多等的秒数 `wait_per_reply_s`），再只改热等待这一个值。

## 后台长任务（2026-11-07 v1.4）

| 变量 | 默认 | 作用 |
|---|---|---|
| `WECHAT_BRIDGE_BACKGROUND` | 1 | 0 = 关掉后台路径（长任务回到 v1.3 接力） |
| `WECHAT_BRIDGE_BG_WATCH_FRESH_S` | 120 | watch hook 心跳多旧以内才把任务交给后台 |
| `WECHAT_BRIDGE_BG_TAKE_MIN` | 10 | 排队多久没人接就退回前台接力 |
| `WECHAT_BRIDGE_BG_REWAKE_MIN` | 3 | 没人接时隔多久再叫一次主代理（最多叫 3 次） |
| `WECHAT_BRIDGE_BG_STALE_MIN` | 30 | 接手后多久没有 progress 就重新排队（最多接 3 次） |
| `WECHAT_BRIDGE_BG_MAX_MIN` | 240 | 接手后的动手时长上限（v1.6：不含排队；多次接手累加） |
| `WECHAT_BRIDGE_BG_KEEP_DAYS` | 14 | 已结束的队列记录和产物目录保留天数 |
| `WECHAT_BRIDGE_BG_MAX_RUNNING` | 1 | v1.5：同时最多跑几个后台任务（1–8；长任务常改同一批文件，默认串行） |

watch hook 的轮询间隔在 hook 定义里（`poll_interval_secs: 10`，最低 5 秒）。inbox hook 的 5 秒轮询不变。

## v1.7（2026-11-07）

包里仍不改 `state/tuning.env`。新增一个开关，其余是代码常量：

| 变量 / 常量 | 默认 | 作用 |
|---|---|---|
| `WECHAT_BRIDGE_TASK_INDEX` | 1 | 0 = 不用 `state/task_index.json`（每个 CLI 进程重新解析全部任务文件，即 v1.6 的做法）。这个文件只是缓存，删掉无害，也不进备份 |
| `QUOTE_TEXT_MATCH_MIN`（常量） | 6 | 按引用文字找后台任务时，引用文字归一化后至少几个字 |
| `QUOTE_TEXT_MATCH_HOURS`（常量） | 48 | 只和本用户最近多少小时的往来消息比 |
| `TASK_INDEX_SAVE_AFTER`（常量） | 64 | 一次扫描解析了这么多文件就立即写索引；serve 另外每分钟写一次 |
