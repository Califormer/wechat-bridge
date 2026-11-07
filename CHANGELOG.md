# wechat-bridge 待发布：v1.7 多轮迭代（2026-11-07，版本号仍 5.0.5，由机主定）

在 v1.6 上做 4 轮迭代；参考 Tencent/openclaw-weixin（2.4.9 引用重建、类型定义）、tingly-dev/weixin（对照官方 inbound.ts 的引用解析）、ilink-hub（按回显登记 msg_id 的引用路由）和 git index（按签名校验的持久化缓存）。数字来自隔离测试。

| 项 | 改动 |
|---|---|
| R1-1 | 引用的 id 不只读 `ref_msg.svr_id`：再按 message_item.msg_id / message_id / svr_id、extra.msg_id、ref.msg_id 逐个查；入站 item msg_id 登记为别名 |
| R1-2 | 发出的每段都进引用库（没有服务端 id 时用 `cid:<client_id>`）；id 都找不到时按引用文字匹配本用户 48 小时内的往来，唯一指向一条进行中的后台任务才并入（`via=text`） |
| R1-3 | 回显台账记 client_id → 任务，回显登记进引用库时带上任务 |
| R2-1 | complete 关闭后台任务时在同一把锁里再查未读补充：finish 拒绝并返回 `unread_notes`，fail 点名 |
| R2-2 | 退回前台时补充随任务带过去，claim 输出 `followups` |
| R2-3 | 退回过的任务 `complete --background` 被拒；判长提示按任务本身 |
| R3-1 | essential 通知不受、也不占每小时通知上限 |
| R3-2 | doctor：排队时长从 `requeued_at` 算；新增 `quote_routing`（只提示） |
| R4-1 | 后台队列条目按 (inode, mtime, size) 缓存；14 天清理每小时最多一次 |
| R4-2 | 任务摘要缓存只信任沉淀的签名（mtime 早于一个时钟周期），目录快速路径同理 |
| R4-3 | `state/task_index.json`：持久化摘要缓存，CLI 进程只解析变过的任务文件；`WECHAT_BRIDGE_TASK_INDEX=0` 关闭；不进备份 |
| 测试 | 新增 `tests/test_v17_20261107.py`（29 项；同一份在 v1.6 上 14 项失败）；`tests/bench_v17_inproc.py`（新旧对比基准，不在 run_all 里） |

# wechat-bridge 待发布：后台长任务 v1.6（2026-11-07，版本号仍 5.0.5，由机主定）

在 v1.5 上按审查单（Claude-wechat-bridge-v15-fix.md）修 5 个 bug + 7 项优化；数字来自隔离测试。

| 项 | 改动 |
|---|---|
| Bug 1 | notes 带序号，接手者记已读序号（同 Kafka offset / Redis Streams last-delivered-id）；只截断已读的，未读的上限 30 条，满了明确告诉用户 |
| Bug 2 | finish 时有未读补充：返回 `unread_notes`、不发送；fail 在失败消息里点名没做的补充 |
| Bug 3 | 每次引用查找记一笔（`long-tasks show` / `list` 的 `last_quote_lookup`），查不到写日志；按原消息 id 兜底保留并测试；信号发不出时记 `signal: suppressed`，take 提示主代理 |
| Bug 4 | 只有轮到的任务（队头）才计“没人接”，从轮到那一刻算 |
| Bug 5 | 240 分钟只算动手时间（接手起算，多次接手累加） |
| 优化 1/7 | 轮到时重新给满叫醒次数；叫醒次数用完的不再堵住后面的任务（重叫间隔内的不另叫，避免刷屏） |
| 优化 2 | doctor 排队超 11 分钟一律 WARN，注明是否在等别的任务 |
| 优化 3 | 批处理关着时，非批的领取路径也做引用补充 |
| 优化 4 | 通知统一追加登记，“中断了一下”“退回前台”也登记 |
| 优化 5 | 补充只回执第一条，「进度」显示补充条数 |
| 优化 6 | 敏感补充明确告知没并入；秘密规则加自由文本（“密码是…”）和常见 token 形态（gitleaks / detect-secrets 规则）及 iLink `ilb_` 令牌前缀 |
| 顺带 | `create_notice(essential=True)`：信号、首条回执、拒收说明不受每小时通知上限；修正 v1.5 读通知 `about` 的位置 |
| 测试 | `tests/test_bg_v15_20261107.py` 加 7 组（run_all 仍 32 套） |

# wechat-bridge 待发布：后台长任务 v1.5（2026-11-07，版本号仍 5.0.5，由机主定）

在 v1.4 后台包上改；对照官方 openclaw-weixin 插件、ilink-hub、OpenClaw 子代理文档逐项核对后的改进。下表数字来自隔离测试，没有线上实测。

| 项 | 改动 | 依据 |
|---|---|---|
| 后台车道 | `WECHAT_BRIDGE_BG_MAX_RUNNING`（默认 1）：take 在车道满时返回 busy；hook 只为能开始的任务叫醒主代理（先来先做）；等空位不算“没人接”；信号 /「进度」报排位 | OpenClaw 子代理的 `subagent` 车道与 `maxConcurrent` |
| 引用即补充 | 引用后台任务消息的回复写进队列 notes（take / progress 交给主代理），原消息并入该任务，回一句确认；像秘密的内容不写入 | ilink-hub 按 client_id / 引用路由；OpenClaw 线程绑定会话 |
| 续做通知 | 重新排队时告诉用户“中断了一下，会接着做” | OpenClaw 网关重启后的续做通知 |
| 核对后不改 | 长轮询、-14 暂停、longpolling_timeout_ms、errcode、请求头与 base_info、游标先落盘、原子写入（含目录 fsync）、收到即“正在输入”——已与官方插件一致或更稳 | openclaw-weixin、weixin-agent、python-atomicwrites |
| 测试 | 新增 `tests/test_bg_v15_20261107.py`（run_all 31 → 32 套） | |

# wechat-bridge 待发布：后台长任务（2026-11-07 v1.4，版本号仍 5.0.5，由机主定）

在 v1.3 接力包上改。下表数字全部来自隔离测试（Claude 的沙箱），没有线上实测。

| 项 | 改动 | 说明 |
|---|---|---|
| 交接 | 桥在批派发时（`_batch_dispatch`，task_lock 内）把 long_hint 判长的整批交给后台：先写队列并读回，再把任务改为 `background` 状态，再发信号；执行者路径保留 `complete --background` | 只在 watch hook 心跳（`state/long-tasks/.watch_tick`）2 分钟内新鲜时才入队；提到凭据的任务不入队 |
| 队列 | `state/long-tasks/queue/<id>.json`（0600，目录 0700）：message_id、同批消息、用户、原文、long_hint、收到时间、点名文件、state（queued / running / done / failed / cancelled / returned）、接手 token、阶段 | `ops/backup.sh` 不带 state/long-tasks；14 天后清理 |
| hook | 新增 `hooks/wechat-long-task-watch.sh` / `.json` / `.prompt.txt`：每 10 秒，delivery 主聊天；只在有排队任务时调 `wb long-tasks wake-check`，每条任务叫醒一次，没人接时 3 分钟后再叫，最多 3 次；叫醒记在 `state/long-tasks/watch.log` | 注册由 Muse 在主环境做；inbox hook 不变 |
| 主代理 | 新命令 `wb long-tasks [list / show / take / progress / finish / fail]`；`finish` / `fail` 走这条任务自己的回复通道（不用 send-direct），token 防止过期接手者误发 | 新手册 BACKGROUND.md |
| 兜底 | 排队超 10 分钟退回前台接力；接手后 30 分钟没进展重新排队（最多接 3 次）后如实失败；超 240 分钟如实超时；「取消」「进度」看得到后台任务 | serve 每 15 秒、`long-tasks` 每次调用都检查 |
| 顺带 | v1.3 接力交棒时 bump `.inbox_signal`，停留中的 inbox hook 立刻唤醒下一棒（原来等下一次 5 秒轮询） | |
| 测试 | 新增 `tests/test_bg_20261107.py`、`tests/test_bg_e2e_20261107.py`（run_all 29 → 31 套） | 旧测试未改 |
| 不变 | inbox hook 脚本与 5 秒轮询、600 秒、v1.3 接力（后台关时、退回时用）、退出码 4、批处理、取消、`--hold-minutes`、热等待、回复条数、BRIDGE_VERSION 5.0.5 | |

上线、注册 hook、回滚、验收见 `CHANGES-claude-v1.4.md`。

# wechat-bridge 待发布：长任务接力 + 更快更智能（2026-11-07，版本号仍 5.0.5，由机主定）

在 feat3 包（2026-11-06 三项新功能）基础上。下表数字全部来自隔离测试（Claude 的沙箱），没有线上实测。

| 项 | 改动 | 说明 |
|---|---|---|
| 接力 1 判长 | `long_hint`：纯规则（词表 / N 个文件 / 全部所有 + 文件 / 压缩包或 ≥20 MB / 一批 ≥3 个文件；只问不做的问句不算），约 4 µs 一条 | 只在命中时出现；短任务的 claim 与 5.0.5 逐字节相同（隔离对比 3 条） |
| 接力 2 计划 | `progress --plan / --step-done / --artifact / --plan-from`；`state/plans/<id>.json` 0600、目录 0700；每步 ≤7 分钟；拒收凭据 | 带 --notify 时桥按计划写大纲；`ops/backup.sh` 不带 state/plans |
| 接力 3 交棒 | `complete --continue`：同一任务回 pending，wake_key `<id>~r<n>`，hook 不用改；下一棒 claim 带 plan（done / next / todo / 产物）；旧棒的 claim 被拒（退出码 3）；热等待的 claim 不接棒 | 同批其他消息并入这条链；交棒时最多每 10 分钟一条进度，保留最终回复要用的条数 |
| 接力 4 兜底 | 被掐断的棒照旧 10 分钟后收回，但从计划续做；做完过一步的棒不算“试了 3 次”；上限 8 棒 / 60 分钟 / 连续两棒没进展；到上限如实告诉用户，回「继续」接回计划 | `stats --task-id` 每棒一行；doctor 新增 `relay` 检查 |
| §8 | `file_peek`、「统计」、「进度」看整条链、`stats` 的 whatif 与 `cli_calls`；提示词加一条失败换路（最多 1 次） | 各自一个开关，见 TUNING.md |
| 测试 | 新增 `tests/test_relay_20261107.py`、`tests/test_relay_e2e_20261107.py`、`tests/test_s8_20261107.py`（run_all 26 → 29 套）和 `tests/relay_fixtures.py` | 旧测试一条未改 |
| 不变 | hook 脚本、轮询 5 秒、600 秒、退出码 4、批处理、取消、`--hold-minutes`、热等待 90 秒、每条消息约 10 条回复、BRIDGE_VERSION 5.0.5 | |

上线、回滚、验收见交付包根目录的 `CHANGES-claude-v1.3.md`。

# wechat-bridge 待发布：批处理 / 可打断回复 / 失败文本补发（2026-11-06，三项新功能，版本号仍 5.0.5，由机主定）

在 s8fix 包（2026-11-05 §8 审计修正）基础上只做这三项。下表数字全部是隔离测试（Claude 的沙箱），没有线上实测。

| 项 | 改动 | 说明 |
|---|---|---|
| 1 批处理 | 新增 `state/pending.json` 与 serve 的 batcher 线程：发给 Muse 的消息先入对方的当前批（任务文件照旧先落盘，另标 `batch_open`），停顿 3 秒或开批满 60 秒（先到为准）整批派发；一次 claim 领一整批，merge_hint = 同一批；hook 跳过未派发的任务；派发时才 bump `.inbox_signal` | 删掉 v5 的 claim 合并窗口（`_settle`/`_settled_pending`/`_reads_complete`、`CLAIM_SETTLE_*`、`WECHAT_BRIDGE_SETTLE_SHORT_MS`），只留一种口径；`--settle-ms` 接受但不起作用。入批/派发/打断同一把 task_lock，边界不丢；孤儿任务由 `batch_recover()` 补派。一批 = 一个执行单元：心跳、长步骤租约、自动进度按批算 |
| 2 可打断 | 新消息到达时，已派发未交付的旧消息并入新批；旧 claim 记为 superseded；complete / progress / heartbeat / send-media / send-* 对它返回退出码 4（`EXIT_SUPERSEDED`），什么都不发；被领走过的任务换 wake_key，hook 立即唤醒新执行者 | 已交给桥的回复、指令、主动消息、带长步骤租约的任务不打断；被打断过的任务只认当前 `--claim-id`。提示词加一句（2144 → 2248 字，sha256 `ffa1e4a8…b6c8`），EXECUTOR.md 改两行（7977 字节，媒体相关的“引用”一条移到 EXECUTOR-media.md） |
| 3 补发 | 新增 `state/outbox.json`：确定没送达的回复文字（parked / retrying / exhausted）入队，送达、取消、可能已送达、72 小时后移出；对方下一条消息让他的条目立即到期 | `_resume_deferred` 的文字部分改走这里（条数/窗口挂起的照旧每条消息续发）；失败/用完重试的每条最多被消息触发 3 次、每次一试、沿用原计数与退避、不重复提示；媒体队列 `outbox/queue/` 不变、互不读写 |
| 运维 | `ops/backup.sh` 快照不带 pending.json、outbox.json；`status` 新增 `batch`、`text_outbox`；「进度」显示“合批中”；新调参 `WECHAT_BRIDGE_BATCH`、`WECHAT_BRIDGE_BATCH_QUIET_MS`、`WECHAT_BRIDGE_BATCH_MAX_MS` | 包里不改 `state/tuning.env`，默认就是 3000 / 60000 |
| 测试 | 新增 `tests/test_batch_20261106.py`、`tests/test_batch_e2e_20261106.py`（run_all 24 → 26 套）；按新行为改 3 处旧断言：`test_perf` 的自适应窗口（改为 `t_claim_has_no_window`）、`test_v5` 连发合并那段（改为开着批处理跑）、`test_v5_e2e` 热执行者“3 秒内”（改为 3–5 秒，含 3 秒合批）；`tests/jqshim.py` 支持新 jq 程序；`run_all.py` 登记两个新临时目录前缀 | |
| 不变 | 回显 observe、BRIDGE_VERSION 5.0.5、hook 轮询 5 秒、tuning（热等待 90、停留 3000）、服务单元、媒体队列、发送分片与重试策略、指令 | §12 机主待定的 5 项按现有做法继续，本包不涉及 |

上线、回滚、验收和未决事项见交付包根目录的 `CHANGES-claude-v1.2.md`。

# wechat-bridge 待发布：2026-11-05 审计修正（§8 第 1–5 条，版本号仍 5.0.5，由机主定）

依据 Muse 2026-11-05 Step 3 之后的线上审计（简报 §8）。线上实测证据：5 条冷消息里 4 条在收到后 50–103 ms 触发 rerun，但 `linger_wakes` 仍是 0；全日志 4769 行里 woke=1 是 0 行。下表数字除注明外都是隔离测试。

| 条 | 改动 | 说明 |
|---|---|---|
| 1 woke 度量 | hook 每处 `wake` 之前先写 `woke=1` 和 `wake_sig`（5 处：任务、送达不确定、登录告警 ×2、服务不健康）；三个 hook 测试的运行时桩 `wake()` 改为会退出 | 根因：线上 `wake()` 结束脚本，原来写在 wake 之后的 `woke=1` 永远执行不到；测试桩的 `wake()` 不退出，所以测试一直过。复现脚本在“会退出”的桩下重现了线上症状（rerun 1、linger_wakes 0） |
| 2 日志保留 | tick 日志保留 18000 行（约 25 小时，1 MiB 轮转），原 2000 行（2.8 小时）；`hook_tick_stats` 读取上限 5000 → 40000 行，窗口封顶 24 小时；新增 `window_h`、`span_h`、`reruns`；`ops/backup.sh` 不再把 tick 日志放进每小时快照 | 原来三处对不上：保留 2.8–9.4 小时、读取约 6.9 小时、窗口 24 小时（`stats` 传 72 小时）。不排除备份的话，每小时快照各存一份，state 副本又不自动清，每天约多 15–20 MB |
| 3 部署缺口 | 上线步骤加一条：提示词有变时，同一段文字同时写进 prompt.txt、json 的 prompt 字段（只改这一个字段）和 sha256 文件；json 的 delivery 保持 main（线上是 side_chat，不要混）；OPS.md 末节同样写明 | `test_audit` 新增 delivery 检查 |
| 4 临时目录 | `tests/run_all.py`：每个测试文件单独 TMPDIR，跑完即删；开跑前清过期测试目录（不碰 vframes-*、hook-*、tmp*、wb-repro-*）；`test_v3.py` 的无前缀临时目录加 `v3-` 前缀 | 原包一遍全套在 /tmp 留 166 个目录、276.7 MB（du）；改后 0 个。只改测试，不改桥 |
| 5 措辞 | EXECUTOR.md 第 9 行、提示词第 11 行：“默认 45 秒” → “代码默认 45 秒、线上 90 秒” | EXECUTOR.md 仍 7990 字节（同一行里删掉重复的 `--then-wait auto` 和一个“后”字抵消）；提示词 2134 → 2144 字，sha256 `cbf5e585…2314` |
| 测试 | 新增 `tests/test_audit_20261105.py`（run_all 23 → 24 套）、`tests/repro_audit_20261105.py`（不在 run_all 里）与 `tests/fixtures/before-audit-20261105/`（修复前的 hook 和 run_all，复现用） | |
| 不变 | 回显 observe、不自动重发、BRIDGE_VERSION 5.0.5、hook 轮询 5 秒、tuning（热等待 90、停留 3000）、服务单元、桥的业务逻辑 | 停留 3000 → 4000（§8 第 6 条）是单变量实验，不在本包；执行者冷启动（第 7 条）不是桥的事 |

上线、回滚、验收和未决事项见交付包根目录的 `CHANGES-claude-v1.1.md`。

# wechat-bridge 一键恢复接进 tbtest 重建（2026-11-05，机主定）

`ops/recover.sh` 新增第 3 步：tbtest 用户或 `/home/tbtest/.local/bin/terminal-browser` 缺失时，自动跑 `~/workspace/terminal-browser-headless/rebuild-tbtest.sh` 重建（VM 替换会丢 `/home/tbtest`，微信抓网址 / KaTeX 渲染链路跟着断）；已存在时跳过。重建失败只告警、不让恢复流程失败（桥轮询是核心，tb-fetch 只是辅助）。OPS.md 同步更新。改前快照 `backups/pre-tbtest-recover-20261105/`。实测：本次 VM 上 tbtest 已丢，跑 `bash ops/recover.sh` 走通重建分支，`tb-fetch.py https://example.com` 抓到 Example Domain。

# wechat-bridge 热等待 90 秒定案（Step 2，2026-11-05 测试1，只改文档，版本号仍 5.0.5）

线上实测（Muse，2026-11-05 测试1，追问间隔约 10/40/70/100 秒）：warm claims 5 → 8，cold claims 21 → 22；约 10、40、70 秒的追问被热接住，约 100 秒的超出 90 秒走冷领取；唤醒 p50 warm 1203 ms，cold 约 4443 ms。结论：热等待 90 秒，已验证，2026-11-05 测试1；不改回 45。配置仍只在 state/tuning.env（WECHAT_BRIDGE_WARM_WAIT_S=90），代码默认不变。停留（linger）本轮未测，不在此结论内。

# wechat-bridge 待发布：第一次进度提前、长尾可见（2026-11-05 第二轮，Step 0 + Step 1，版本号仍 5.0.5，由机主定）

依据 Muse 2026-11-05 线上实测（测试2）：2-3 跑整套测试，第一次 progress 在领取后 132.3 秒，离完成只剩 9.4 秒；`stats` 的 `to_first_call` p95（n=15）取排序后第 14 个值 5521 ms，132300 ms 被排除在外。

| 类别 | 改动 | 说明 |
|---|---|---|
| 度量（Step 0） | `stats` 的 exec 证据每列加 `max`；新增 `to_first_call_top`、`exec_no_calls_top`（最慢 3 条，带任务 ID） | `_pct` 不变：p50/p95 与已记录的数字可比 |
| 度量（Step 0） | `stats --task-id ID[,ID]`、`stats --recent N`：单条任务的 calls（时间戳与领取后毫秒）、to_first_call、执行时长、用户收到的进度通知（progress / auto_progress）及发出时间 | 只读，不写任何状态 |
| 规则（Step 1，按机主 2026-11-05 的决定） | EXECUTOR.md 与 hook 提示词：任何单步预计超过 60 秒的先报；改代码、跑测试、重启服务、打包、等外部结果、耗时脚本、下载/上传大文件、编译/安装、批量处理、网络抓取这类（举例，不是穷举）不看预计时长也先报；任务文字写明要做的，领取后第一个动作就是它；每条任务只在开头带一次 `--notify`，之后换步骤只报 `--stage`；查询类不算 | 3 分钟捷径只留给没有长步骤的任务；提示词 1824 → 2134 字；EXECUTOR.md 7990 字节（上限 8000，原“长步骤租约”一条并入长步骤一条） |
| doctor | 新增信息行 `latency_tail`：to_first_call 的 max（和任务 ID）、总计/处理 p95、样本数；与 `scale` 共用一次 `stats` | 只显示，永远 PASS：延迟是模型决定的，不是桥的健康指标 |
| 测试 | 新增 `tests/test_firstcall_20261105.py`（27 项）；`tests/repro_first_progress.py` 复现 2-3（真 serve + 本地假微信，不在 run_all 里）；修 `test_v4_e2e`「状态」偶发失败：等文本出现且 inbox status 为 sent 再判（15 秒内轮询），只改测试不改桥 | run_all 由 22 套变为 23 套 |
| 不变 | 回显 observe、不自动重发、BRIDGE_VERSION 5.0.5、hook 轮询 5 秒、hook 脚本、tuning.env、服务单元 | 热等待写进文档（Step 2）、停留实验（Step 3）按顺序留到下一轮 |

上线、回滚、验收和未决事项见交付包根目录的 `CHANGES-claude-v1.0.md`。

# wechat-bridge 待发布：延迟、执行效率与证据（2026-11-05，版本号由机主定）

在 2026-11-04 审查修正之上，按 Muse 的线上实测（v5.0.5，65 条中位数：接手 5.9 秒，其中等领取 5.5 秒；执行 47.8 秒；送达 0.68 秒；合计 60.1 秒）调整方向：大头是模型执行，所以优先减少模型往返，并把"调不调"建立在数据上。

| 类别 | 改动 | 依据 / 默认 |
|---|---|---|
| 少一轮往返 | `complete --then-wait auto`：发完回复原地等追问，下一批任务在 `next` 里 | 每轮对话省一次调用；hook prompt 与执行手册已改用 |
| 少一轮往返 | 预计 3 分钟内的任务不再强制先报 progress（claim 即第一次心跳） | 10 分钟收回从 claim 起算，规则不变 |
| 少一轮往返 | 媒体预处理：大图 ≤1568 px 预览、≤60 MB 视频的关键帧 + 音轨、文本内联（UTF-8/GB18030）、PDF 文字 | `WECHAT_BRIDGE_PREDIGEST=0` 可关 |
| 少 token | `--context-since <context_cursor>`：热等待的追问只带新增上下文 | then-wait 自动使用 |
| 一次答全 | 紧跟在下载中媒体后的提问与媒体一起领取（≤15 秒） | `CLAIM_MEDIA_HOLD_MS` |
| 桥更快 | 整句 0.3 秒 / 碎片 1.2 秒的自适应合并窗口（防抖 + 快照领取，上限 4 秒） | 基准：整句被热执行者领走 1266 → 365 ms |
| 桥更快 | `wb` 启动器复用字节码缓存 | 每条命令 156 → 78 ms；complete 到发出 158 → 67 ms（离线基准，不含网络） |
| 桥更快 | hook 在媒体下载时就唤醒执行者，claim 等下载完成（≤60 秒） | 冷启动与下载重叠 |
| 可选实验 | hook 停留：`WECHAT_BRIDGE_HOOK_LINGER_MS`，停留期间新消息立即唤醒 | 默认关；效果取决于平台 5 秒从哪儿算，见 evidence.hook_ticks |
| 证据 | `stats.evidence`：CLI 调用时间线、热/冷领取、送达拆分、追问间隔分布、hook 运行时间线、回显台账、当前调节项 | 只记录，不改行为 |
| 回显 | 默认 observe：只记台账与引用；resolve 才确认"不确定"分片并延后提醒；任何模式都不自动重发 | 按 Muse 意见：先攒数据 |
| 调节 | `state/tuning.env`（只认 `WECHAT_BRIDGE_*`），`--wait auto` / `--then-wait auto` 读 `WARM_WAIT_S` | 不改单元、不碰 proxy.env |
| 测试 | 新增 `tests/test_perf_20261105.py`（32 项）与 `tests/bench_latency.py`（基准，不在 run_all 里） | 迭代中抓到并修掉：合并窗口竞态（50 ms 循环暴露）、提前唤醒发送线程导致的回复/收尾抢锁 |

部署：新 prompt 用 `python3 wb …`，`wb` 必须随包部署；已安装的 hook 定义需同步 prompt；`state/tuning.env` 可选。回滚：快照还原代码与 hooks/、ops/、tests/、文档（state/ 格式兼容，新增字段旧代码忽略）。

# wechat-bridge 待发布修正（2026-11-04 代码审查，版本号由机主定）

按 2026-11-04 全量审查逐项落地；BRIDGE_VERSION 仍是 5.0.5、wechat-bridge.service 未改（CLAUDE.md 约定，由机主定）。每项都在 `tests/test_review_20261104.py` 里有对应用例（v5.0.5 上这些用例全部失败或无法表达）。

| 类别 | 改了什么 | 原来的问题（已复现的标 ✓） |
|---|---|---|
| 丢消息 | 入库遇存储错误不推进游标、重拉同一批；磁盘满类错误一直重试，其他存储错误 30 轮后跳过那一条；心跳 `ingest_fail_streak` | ✓ 吞掉异常照推游标，磁盘满时消息永久丢失 |
| 健康判定 | 长轮询客户端超时打标；连续 3 次起计轮询失败，`last_poll_ok_at` 停止前进，清空空闲连接并参与代理切换 | ✓ "连得上但不回包"（含 keepalive 的 ETIMEDOUT，Python 里是 TimeoutError）一直被记成健康空轮询 |
| watchdog | `longpolling_timeout_ms` 夹在 1–60 秒 | 服务端给 >175 秒时一轮长轮询就超过 WatchdogSec=180 |
| 停服务 | SIGTERM 不打断正在入库的一批；最多等 12 秒让工作线程做完手上那一步；发到一半的回复记 send_failed（不占重试次数）、重启后续发 | 守护线程在请求中途被杀，留下 sending → 误报"不确定送达" |
| 路径穿越 | `safe_name()`：所有落盘名（任务、队列、媒体、.part/.plain）统一清洗，普通 id 不变，异常 id 加哈希；旧命名仍可读并自动迁移 | ✓ message_id `../../x` 让 root 进程把媒体写到目录外；`a.b`/`a_b` 撞同一任务文件 |
| 权限 | `atomic_write_json/text` 共用 O_EXCL\|O_NOFOLLOW 0600 创建 + fchmod + 目录 fsync；umask 077（`WECHAT_BRIDGE_UMASK`）；启动收紧目录；历史/二维码/媒体/proxy.env 一律私有写 | ✓ v5.0.5 只修了 text 版，credentials.json 的临时文件在 umask 002 下是 0664 |
| 令牌去向 | baseurl、扫码 redirect_host 只认 https `*.weixin.qq.com`、回环、`WECHAT_BRIDGE_API_ORIGINS` | bot token 发往登录响应给的任意地址 |
| 误拦截 | 「设置…」只拦明确的设置指令；「查记录」需带空格/冒号 | ✓ 「设置一个明天早上8点的提醒」「查记录一下…」被桥接吞掉，Muse 收不到 |
| 协议体积 | 请求体 `ensure_ascii=False` + 紧凑分隔符（孤立代理字符仍转义） | ✓ 中文 4000 单位分片 24 KB，实际应为 12 KB |
| 登录 | `_start_login()` 不打印；wait-login 只输出一个 JSON，带 `qr_refreshes`/新二维码路径 | ✓ 二维码刷新后 stdout 两段 JSON，json.loads 失败 |
| 历史 | 轮转按字节（≤600 行且 ≤上限一半）；私有追加；「查记录」改为从新到旧、有上限的检索；归档保留期可选 | ✓ 900 次追加触发 575 次全量重写，文件卡在 1.85 MB |
| 决策清单 | claim 时按节取舍（进行中/已决定/规矩节优先，已完成/回灌节其次，注明省略）；`--add` 清洗全部换行类字符与控制/双向字符、限长 500、加锁 | 截前 3000 字，新规矩写在文件末尾会最先被截掉；U+2028 等可注入标题 |
| 领取令牌 | claim 输出 `claim_id`；complete/progress/heartbeat `--claim-id` 防过期执行者覆盖；回收后尚无人接手时原执行者心跳可要回任务 | 回收后旧执行者仍可 complete，与新执行者的回复互相覆盖 |
| 自动附件 | 拒绝隐藏路径、桥接私有目录（state/inbox/history/backups/logs/outbox/vendor）、更多密钥类文件名；可选白名单目录；complete 输出 `auto_attach_refused` | ✓ ~/.netrc、~/.docker/config.json、~/.kube/config、gh hosts.yml、.pgpass、聊天记录、任务记录都会被附上 |
| 下载完整性 | 206 的 Content-Range 起点不对就从 0 重下；文件带 md5 时软校验（`file_md5_ok`） | 错位续传会拼出"能解密"的坏文件（ECB 只校验末块） |
| 视频 | 超过 50 MB 的视频（≤300 MB）先压缩再发 | 压缩路径本为此而设，却在入队时就被拒绝 |
| 分类 | 本地加密失败记 business；typing 同时看 errcode；未带服务端错误码的业务错误不再触发全局分片降级 | 本地失败被当网络问题重试 6 轮 |
| 供应链 | vendor/ 追加到 sys.path 末尾，任何人可写或属主不对的目录不加载 | 插在最前面，可遮蔽标准库，root 服务加载 |
| proxy.env | write-proxy-env 拒绝含换行/引号/反斜杠的值；doctor 发现非代理变量判 FAIL | 换行可向 root 单元注入 LD_PRELOAD 等变量 |
| 损坏任务 | hook 隔离不覆盖旧副本；桥接对新的 .corrupt 文件给机主发一次提醒 | 消息静默丢失 |
| 「状态」「备份状态」 | 备份大小统计有文件数/时间上限、硬链接只算一次 | 轮询线程里无上限 rglob |
| ffmpeg | `-nostdin`、stdin=DEVNULL、CLI 输入输出路径转绝对路径 | 交互 stdin 下可能卡住；相对路径可被当成协议 URL |
| hook | 同一次故障内重启退避（30 秒→10 分钟）；重启前写 serve_restart_reason（hook_inline）；模板 poll_interval_secs 2→5，prompt 增加 claim_id | 故障期间每 30 秒重启；重启原因全是 unknown；模板与平台限制不符 |
| ops | recover/repair 的目录按脚本位置推导；repair 不再把路径拼进 Python 代码；backup 未变化的 state/ 文件硬链接、时区可配；doctor 直连模式不再误报、新增 pipeline/decisions 两项；send-direct 用私有 umask | 路径写死、重复的凭据副本无限增长、直连部署 doctor 永远 FAIL |
| 测试 | 新增 test_review_20261104.py、tests/run_all.py；test_slim/test_tail 在没有 backups/ 的仓库里跳过部署专属检查 | 干净解包里这两个测试必失败，"CI 全绿"无法复现 |
| 开源卫生 | 根目录 DECISIONS.md 换成不含个人信息的模板（线上以 state/DECISIONS.md 为准，不受影响） | 公开仓库带着个人决策记录，新部署还会把它当成自己的决定 |

验证：`python3 tests/run_all.py` 全部通过（见本次交付说明）。部署前：`ops/backup.sh`；覆盖代码与 hooks/、ops/、tests/、文档；hook 定义里只需更新 prompt（poll 已是 5 秒）；`systemctl restart wechat-bridge`；`python3 ops/doctor.py`。回滚：用快照还原 bridge.py、hooks/、ops/、tests/ 和文档后重启（state/ 格式未变，旧代码可直接读新写的文件）。CODEMAP.md / CODEMAP.json / .codegraph 的行号已过期，需要重建索引。

# wechat-bridge v5.0.5 回灌（2026-11-03，机主定 GitHub 和线上桥都更新）

开源包 v5.0.5 的修正回灌线上桥：兜底路径 POST 重定向不再静默重放
成 GET（实测会丢消息体却返回 200），现对非 GET/HEAD 直接抛
HTTPError，与池路径一致，测试补两模式 POST 用例；BRIDGE_VERSION
5.0 → 5.0.5（状态输出可区分发布版本）；atomic_write_text 创建时
即 0600、写前 fchmod；decisions --add 换行清洗。线上与开源包的
唯一差别仍是 bridge.py 里一行 OPS.md 注释。快照
backups/pre-v505-20261103/ 可回滚。

# wechat-bridge v5.0.4 回灌（2026-11-03，机主定 GitHub 和线上桥都更新）

开源包 v5.0.4 的修正回灌线上桥：重定向守卫补测试（池与回退两模式
实测剥凭据、拒非白名单、同域保留、降级拒绝）、跨域重定向被拒统一
为 MediaError（回退路径此前包成 URLError、会被当网络错误重试 6 次）、
state/DECISIONS.md 改 0600 原子写（旧文件已 chmod 0600）、坏密文改
4097 字节（长度本身即错，与密钥无关）、CI 矩阵断言与后端循环（开源
包侧，工作流改由网页端更新）。线上决策文件与旧文件内容一致，本次
回灌不迁移文件。快照 backups/pre-v504-20261103/ 可回滚。

# wechat-bridge v5.0.3 回灌（2026-11-03，机主定 GitHub 和线上桥都更新）

开源包 v5.0.3 的修正回灌线上桥：决策读取统一成 _decisions_read()
（claim、显示、--add 三处共用，旧文件作回退）、urllib 回退路径装
安全重定向处理器（拒降级、跨域剥凭据、跨域重过白名单）、迁移测试
与 SIGHUP 测试同步。线上决策文件早已在 state/（与旧文件内容一致），
本次回灌不迁移文件；开源包侧的 CI 工作流与线上无关，改由网页端更新。
快照 backups/pre-v503-20261103/ 可回滚。

# wechat-bridge v5.0.2 回灌（2026-11-03，机主定 GitHub 和线上桥都更新）

开源包 v5.0.2 的修正回灌线上桥：文件解密失败统一转 MediaError("decrypt")
（cryptography 后端漏裸 ValueError 的问题；线上当前 ctypes 后端未实际触发，
属潜伏风险）、跨域重定向重过媒体白名单、决策文件迁移到 state/DECISIONS.md
（线上原 DECISIONS.md 已原样复制过去，旧文件留作回退读取，不丢决定）。
开源包侧的 .gitignore 锚定、SECURITY 渠道、CI 工作流与线上无关，不回灌。
注意：GitHub 连接器令牌建 workflow 树会 403，CI 文件改由网页端单独创建，
后续连接器推送须继续排除 .github/workflows/test.yml。

# wechat-bridge v5.0.1 回灌（2026-11-03，机主定 GitHub 和线上桥都更新）

开源包 v5.0.1 的修正回灌线上桥：代理池只认 http://、重定向跨域剥离敏感头并拒绝 https→http 降级、
服务端媒体/上传 URL 只放行腾讯 CDN 域名（*.qq.com、*.qpic.cn、*.qlogo.cn）、AES 改成 cryptography→libcrypto→openssl
回退（不再默认把密钥放进 ps）、「最近文件」归档目录环境变量化；backup.sh/doctor.py 路径改成环境变量可覆盖
（线上默认值不变：项目目录、hook 目录、clawbot cron 定义）；EXECUTOR.md 精简到 8000 字节以下；
服务单元只改了 MemoryMax 1G→2G、RestartSec 2→5，仍跑在 root 下、WantedBy 不变（hook 重启需要，见 OPS）。
线上实测 CDN 域名只有 novac2c.cdn.weixin.qq.com 与 ilinkai.weixin.qq.com，均在白名单内。

# wechat-bridge v5 补丁（2026-11-02，22:46 媒体默认追问 + 声音转写 + 压缩丢细节）

按机主 22:46 那条“发送图片/视频/文件时默认会追问；媒体声音转写、压缩丢细节可以尝试去改”落地，版本号仍是 5.0（只改行为，不改协议）。Hook 轮询**没有动，仍是 5 秒**（平台最低就是 5 秒，2 秒会让 hook 失效，见 MEMORY 2026-11-02 教训）：

| 改了什么 | 说明 |
|---|---|
| 无文字媒体默认先追问（EXECUTOR.md 新节） | 图片/视频/文件没附文字、又没引用时，不再马上给完整描述：先实际看/读媒体，再 `claim --wait 30` 等最多 30 秒；追问到了就合成一条回，没到就只回一句简短追问（确认是什么 + 问想做什么），不展开、不猜问题。例外：已约定只看一眼/先存着的按约定；用户说“媒体不用等”即刻作废 |
| 出站压缩阈值提高（bridge.py `_maybe_compress`） | 图片 2 MB → 8 MB、视频 20 MB → 40 MB 才压，以下尺寸原样发出；压时质量也提高：图片最长边 2560 → 4000 像素、JPEG q 3 → 2，视频 720p crf 28/96k → 1080p crf 23/128k。代价：大文件上传更依赖约 18 分钟的队列重试，细节优先于省流量。阈值可用 `WECHAT_BRIDGE_IMAGE_COMPRESS_OVER_MB` / `WECHAT_BRIDGE_VIDEO_COMPRESS_OVER_MB` 改，`WECHAT_BRIDGE_COMPRESS=0` 仍可全关 |
| 视频声音：新增 `bridge.py video-audio` | 有音轨的视频抽成 16 kHz 单声道 WAV，无声视频明确报 has_audio=false，不编台词；EXECUTOR.md 视频一节已改成先抽音频再分开标注画面/声音转写 |
| 没改 / 改不了的，要说清 | 入站以“图片”形式发来的，微信客户端发送前已压缩，桥接拿到的是压缩版、无法还原；要保原图细节请以“文件”形式发原图。语音识别引擎本身：本机仍没有 STT 引擎，video-audio 只给了可转写的 WAV，转写仍需引擎或人工；没有假装已转写。语音转文字的同音错字仍靠回复里确认关键信息（EXECUTOR.md 原有规则） |

验证：`test_v5` 全部通过（含新增的压缩阈值上下限、video-audio 有声/无声两项），`test_offline --bench`、`test_v5_e2e` 通过。部署：备份快照 20261102-145847，改代码和测试后重启服务复查；回滚用该快照还原 bridge.py / tests / EXECUTOR.md 再重启。

---

# wechat-bridge v5 补丁（2026-11-02，22:15 后按优先级改）

按上一条“v5 还有其他要改进的地方”的优先级顺序落地，代码版本号仍是 5.0（只改行为，不改协议）：

| 优先级 | 改了什么 | 验证 |
|---|---|---|
| 一-1 | Hook 轮询 5 秒 → 2 秒：只改已安装定义 `~/hooks/definitions/wechat-bridge-inbox.json` 的 `poll_interval_secs`，`delivery`（side_chat）原样保留；仓库模板本来就是 2 秒，不用覆盖安装定义 | `ops/doctor.py`：hook 一行已显示 poll=2s |
| 一-2 | 去重金丝雀已发出：`bridge.py canary-dedupe` 用同一 client_id 连发两条，两次都 ok，占了当前消息 2 条额度（剩余 8 条）。**未确认**：需机主在微信里确认只看到 1 条还是 2 条，1 条 → `canary-dedupe --confirm yes`，2 条 → `--confirm no`，确认前送达不确定的分片仍不自动重发 | canary 输出 `sends: ["ok", "ok"]`，`dedupe_confirmed` 仍 false，等用户确认 |
| 一-3 | 真实额度未改：`bridge.py stats` 的 `observed_limits` 仍是空（服务端还没提前拒绝过），10 条仍是社区经验值，没有凭空调 `WECHAT_BRIDGE_CTX_QUOTA`；跑一两天后再看 stats，有实测值再调 | stats 实测 |
| 二 | 自动进度分级：单次 180 秒 → 60/180/420 秒三档（`WECHAT_BRIDGE_AUTO_PROGRESS_SECS` 可配，旧 `WECHAT_BRIDGE_AUTO_PROGRESS_SEC` 仍作单档别名，`=0` 仍关闭）。执行 p50 约 60 秒，所以多数任务在第一档前就已答完、不会多一条通知；已按旧版发过 180 秒通知的任务不再重复发 180 秒档；进度仍不占用最后几条额度；一次只发最高已到档，不会把过期档一次补发完 | test_v5 新增 `g_auto_progress_stages`：60/180/420 各发 1 条、同档不重复，全部通过 |
| 未改 | 连发攒消息（“稍等，还有”/图片多等十几秒）：会拖慢单条回复，需机主选默认行为，不宜默认全开；媒体声音转写、图片/文件压缩丢细节：超出本轮范围，也未动 | 留待机主决定 |

部署：先 `ops/backup.sh`（快照 20261102-142457），改代码和测试后 `systemctl restart wechat-bridge`，重启后 `status` + `doctor` 复查。回滚：用该快照还原 bridge.py / tests / 定义文件，再重启即可。

---

# wechat-bridge v5 优化（2026-11-02）

v5 基于 v4 包，目标是让接入微信桥的 AI 的能力完整地到达用户：回复不再因为微信的条数和时间限制无声丢失，格式按微信客户端实际能渲染的样子发出，追问不用等新执行者冷启动，链路抖动时恢复得更快。依据是腾讯官方插件 @tencent-weixin/openclaw-weixin 2.4.10 的协议文档、types.ts、send.ts、引用缓存文档和 CHANGELOG，以及社区实现的真机报告。所有测试都跑在本地假 iLink（TLS）、假 CDN 和真 CONNECT 代理上，**不是真实链路的数据**。

## 关键发现（v4 没处理；其中两条参考分析也判断错了）

| # | 发现 | 依据 | v4 的表现 | v5 的处理 | 测试 |
|---|---|---|---|---|---|
| 1 | 用户发消息后，ClawBot 只接收 24 小时内的回复，超出返回 ret=-2 | ClawBot 设置页原文；官方仓库 issue #204 腾讯协作者的答复 | 把这个 -2 当成限流，每 90 秒重试、最多 40 次（约 1 小时）；send-direct 的报警就这样无声丢失 | 发送前先查窗口；窗口关闭时不发、挂起，用户下次发消息立即补发；主动消息直接返回 `context_expired`，执行者改在 App 里报告；不再生成发不出去的提醒 | test_v5 窗口 |
| 2 | 每条用户消息之后，微信最多接受约 10 次 sendmessage，第 11 次起 ret=-2 | 社区真机报告（dsh-im #484、cc2wx、tian-agent）。参考分析提的“5 分钟 ≤6 条”滑动窗口不是这个模型 | 第 11 段及以后被当成限流挂着，用户不知道后面还有内容 | 按 context_token 记账（默认 10，可配）；进度和提醒不能占用最后几条；放不下时最后一条末尾带一句说明，其余挂起，用户下一条消息（或「继续」）带来新额度后续发；-2 分成窗口/额度/真限流三类，并记录服务端实际允许的条数（`bridge.py stats`） | test_v5 额度×2、附件额度、e2e |
| 3 | 当前微信客户端能渲染标准 Markdown：表格、带语言名的代码块（高亮+复制按钮）、列表、引用、链接 | cc-connect #1766 真机测 12 类元素（2026-08-28）；官方 StreamingMarkdownFilter 的规则。参考分析建议“转成纯文本”，会把表格和代码块这类手机上最好用的格式毁掉 | Markdown 原样发出（这一点没问题）；但分片可能把代码块切成两半，后一条开头的 ``` 会开一个新代码块，把后面的正文吞进去 | 按官方规则轻过滤（只去掉中文斜体标记、五六级标题、图片语法；任务列表改 ☐/☑）；分片识别代码块和表格：跨片自动闭合并带语言名重开、表格重复表头；长度按 UTF-16 计数，与官方 textChunkLimit 一致（单条最多 12000 字节，不采用参考分析的 16 KiB——没有依据） | test_v5 markdown、分片，e2e |
| 4 | 新版客户端引用消息时可能只带服务端 ID（svr_id），或只引用其中一段（partial_text） | 官方 types.ts、2.4.9-beta.0、quote-cache 文档 | 这类引用静默变空，执行者看不到用户指的是什么 | SQLite 引用库（30 天、1 万条，同官方默认），入站和出站消息都记录；部分引用按 md5 校验，对不上只给客户端自己的首尾片段，绝不编造；引用以前发过的图片/文件时直接复用本地文件，不用重新下载 | test_v5 引用，e2e |
| 5 | Python 的 http.client `read(n)` 在连接提前断开时返回空而不报错 | 写 v5 测试时发现（CPython 源码注释也承认这一点） | 截断的下载被当成完整文件；加密文件解密失败后，用户被告知“微信 CDN 解密问题”——诊断错误，而且不重试 | 按 Content-Length 校验，截断视为网络错误：重试，并用 HTTP Range 从断点续传 | test_v5 续传 |

## 其他改动

| 方面 | 改动 | 效果（本地测试） |
|---|---|---|
| 唤醒延迟 | `claim --wait N`：执行者处理完后原地等追问，拿到的消息它已有上下文；hook 看到等待者（state/claim_waiter.txt）就不再唤醒新执行者 | e2e：消息发出后 1.3 秒被领取（含 1.2 秒连发合并窗口），没有冷启动 |
| 连发 | hook：刚唤醒的执行者还没领取时，新到的消息留给它（它一次领 5 条）；claim 输出 `merge_hint`，`complete --merge-ids` 用一条回复回答多条 | 两条连发合并成一条回复，只占 1 个额度 |
| 输入状态 | 新消息到达立即触发；启动时用已有的 context token 预取 ticket | e2e：0.12 秒（v4 最多等 1 秒再加一次 getconfig） |
| hook 轮询 | 5 秒 → 2 秒（hook 定义的 poll_interval_secs） | 平均唤醒等待约 2.5 秒 → 1 秒 |
| 上下文 | claim 带的对话 8 轮/2400 字 → 10 轮/4000 字，单轮 300 → 400 字；「新话题」之后不再带之前的内容 | |
| 连接池 | 没有代理时也走 keep-alive（v4 退回 urlopen，每个请求都重新握手；而 proxy.env 在单元里是可选的） | 6 个请求 1 条连接 |
| 发送重试 | 请求没发出就失败（connect）时，进程内用同一个 client_id 在 0.3/1.5/5 秒快速重试，再落到任务级重试 | v4 首次重试要等 10 秒 |
| 套接字 | TCP keepalive（20 秒起探测）+ NODELAY；DNS 失败时用上次成功的地址；长轮询读超时 +10 → +5 秒 | |
| 代理切换 | SIGHUP 重读 proxy.env，进程内切换；hook 遇到轮询失败先发 SIGHUP，20 秒没恢复才重启（只对声明了 hup_pid 的 v5 serve） | e2e：切到代理后继续收消息，同一进程 |
| systemd | Type=notify + WatchdogSec=180（轮询循环每 ≤40 秒报一次活）；ExecReload=SIGHUP；NoNewPrivileges、ProtectSystem=full 等不影响桥接的加固 | hook 也失效时，卡死的进程由 systemd 自己重启 |
| 限流 | 真限流从 90 秒起指数退避（90 秒、3 分、6 分，上限 10 分）；用户来消息立即解除并重发被挂起的回复 | |
| 媒体 | 下载断点续传（Range；服务器不支持时自动整段重下）；图片 >2 MB 缩到 2560 像素、视频 >20 MB 转 720p 后再上传；媒体队列首轮重试 15 → 8 秒 | 3 MB 下载在 2 MB 处断开：只补下剩下的 1 MB |
| 附件 | 回复里写 `![说明](/本地路径)` 或 `[名字](/本地路径)`，文件自动作为附件发出；凭据、state/、.ssh 等永远不会被附上 | |
| 指令 | 新增「取消」「进度」「新话题」「继续」，零 LLM；「取消」和「继续」只在有对象时拦截，否则照常交给 Muse | |
| 入站 | 文字旁边带未知类型的 item 时，文字照常交给执行者（v4 回“处理不了”） | |
| 自动进度 | 执行 3 分钟、执行者没报过进度时，桥接发一句“还在处理”（额度够才发） | |
| 观测 | 「状态」增加窗口剩余、回复额度、传输模式、分段延迟；`bridge.py stats`（p50/p95、服务端实际额度）；doctor 增加 reply_window、transport；report 增加分段延迟和 -2 分类 | |
| 协议对齐 | bot_agent 带 `(env=…;transport=…)` 注释（官方 2.3.1 格式）；扫码时 local_token_list 带最近 10 个 token | |
| 去重自检 | `bridge.py canary-dedupe`：同一 client_id 发两次，用户确认只收到一条后，不确定送达的分片才会自动用原 client_id 重发 | 默认关闭，需真机确认 |

## 兼容性与行为变化

- 新任务状态 `cancelled`。新字段：任务 `unsupported_items`、`quote_from_store`；send `deferred`（quota / window / rate_limit / session）、`parked`、`context_expired`、`tail_note`；execution `merged_into`、`cancelled_at`、`auto_progress_at`。
- 新状态文件：`state/send_quota.json`、`state/claim_waiter.txt`、`state/dedupe.json`、`state/token_history.json`（旧 token，0600，和 credentials.json 一样随 state/ 进备份）；`history/quotes.db`（0600，不进备份）。
- 用户来消息时，被限流挂起的回复立即重发（v4 要等满 90 秒）。
- 下载残片 `.part` 保留用于续传，成功后或 24 小时后清理。
- complete 默认会调整 Markdown 并自动附上本地文件引用；`--raw` 原样发送。
- 旧测试调整了 4 处断言，都对应上面的有意变化：`.part` 保留（test_offline）、用户来消息时挂起的回复先发（test_v3）、版本号 5.0（test_v4_e2e）、连发合并（test_hook）。
- 开关：`WECHAT_BRIDGE_CTX_QUOTA`（默认 10，0 关闭本地记账）、`WECHAT_BRIDGE_MARKDOWN=filter|off|plain`、`WECHAT_BRIDGE_AUTO_ATTACH=0`、`WECHAT_BRIDGE_COMPRESS=0`、`WECHAT_BRIDGE_QUOTES=0`、`WECHAT_BRIDGE_AUTO_PROGRESS_SEC`（默认 180，0 关闭）、`WECHAT_BRIDGE_FAST_RETRY_MS`、`WECHAT_BRIDGE_BOT_AGENT`、`WECHAT_BRIDGE_ENV`。

## 部署（按顺序）

1. **不要和正在运行的那套同时用同一个微信号测试**（getupdates 同一时间只允许一个进程轮询）。
2. `cd ~/workspace/wechat-bridge && ops/backup.sh`。
3. 解包覆盖代码文件：bridge.py、bridge.py.v4、wechat-bridge.service、wechat-bridge.service.v4、ops/、tests/、hooks/、文档。state/、inbox/ 不受影响。
4. 跑测试，全部应通过：
   ```
   for t in "test_offline.py --bench" test_watchdog.py "test_proxy_failover.py bridge.py" test_v3.py \
            test_serve_e2e.py test_hook.py test_v4.py test_v4_e2e.py test_v5.py test_v5_e2e.py; do
     python3 tests/$t || echo "FAILED: $t"; done
   ```
5. **先换代码、再换单元**：`cp wechat-bridge.service /etc/systemd/system/ && systemctl daemon-reload`。新单元是 Type=notify，需要 v5 的 bridge.py 报 READY。
6. 安装新 hook：`cp hooks/wechat-bridge-inbox.sh ~/hooks/scripts/`，并把 hooks/wechat-bridge-inbox.json 的 prompt 和 `poll_interval_secs: 2` 更新到 hook 定义。
7. `systemctl restart wechat-bridge`，约 40 秒后 `python3 bridge.py status`、`python3 ops/doctor.py`；在微信里发「状态」，应看到“回复额度”和“主动消息窗口”两行。
8. 可选：`python3 bridge.py canary-dedupe`，按用户看到一条还是两条，执行 `canary-dedupe --confirm yes|no`。
9. 跑一两天后看 `python3 bridge.py stats`：`observed_limits` 是服务端实际允许的条数，和 10 不一致时设 `WECHAT_BRIDGE_CTX_QUOTA`。

## 回滚

`cp bridge.py.v4 bridge.py && cp wechat-bridge.service.v4 /etc/systemd/system/wechat-bridge.service && cp wechat-bridge.service.v4 wechat-bridge.service && systemctl daemon-reload && systemctl restart wechat-bridge`。

**单元必须一起回滚**：v4 不发 READY，留着 Type=notify 单元会启动超时、反复重启。新 hook 与 v4 兼容（v4 不写 hup_pid，就跳过 SIGHUP 阶段；不写 claim_waiter.txt，就按原来的方式唤醒），不必回滚。回滚前先看 `python3 bridge.py status` 的 `parked_sends`：v4 不会自动续发 v5 挂起的回复，需要时用 `bridge.py send --task-id` 补发。

## 验证范围

通过的测试：test_v5 27 项（新）、test_v5_e2e 8 项（新：真实 serve 和 CLI 进程、强制每条消息 10 条额度的假 iLink、真 CONNECT 代理、systemd notify 套接字），以及 v4 的全部 8 组（含上面 4 处有意调整）。

沙箱里验证不了、需要上线看的：
- 每条消息的真实额度（社区报告约 10；`stats` 会记录服务端第一次提前拒绝时允许的条数）。
- 真实 -2 的 errmsg（用来区分真限流；没有 errmsg 时按额度/窗口处理）。
- svr_id 与入站 message_id、sendmessage 返回的 message_id 是否同一命名空间（官方插件按这个假设实现引用缓存）。
- 客户端对 Markdown 的渲染（依据第三方 2026-08-28 的真机测试；`WECHAT_BRIDGE_MARKDOWN=plain` 可退回纯文本）。
- 微信 CDN 是否支持 Range（不支持时自动整段下载，行为同 v4）。
- 真实 systemd 下的 Type=notify 和看门狗。

---

# wechat-bridge v4 优化（2026-11-02）

v4 基于 v3 包。出发点是 2026-11-01 的微信对话截图和 Muse 的反馈（新建文本文档 14）。所有改动都有离线测试；表中“测试”一列来自 tests/test_v4.py 和 tests/test_v4_e2e.py，运行在本地的假 iLink（TLS）、假 CDN 和假代理上，**不是真实链路的数据**。语音条相关的协议细节来自社区实现（weixin-ilink、wechat-ilink 使用笔记等），官方插件本身只发图片、视频、文件。

## 对症：截图里的问题

| # | 现象 | 根因 | v4 的处理 | 测试 |
|---|---|---|---|---|
| 1 | 生成的 18 秒 MP3 发到微信时“三次都因网络连接中断失败（CDN upload RemoteDisconnected）”，要用户“网络恢复后说一声” | 这条出口到腾讯节点的连接会成簇被切断（OPS.md）。v2/v3 的 CDN 重试都在约 5 秒内连试 3 次，落在同一簇里；失败后文件只在执行者手里，没人负责再发 | 出站持久化队列：6 轮约 18 分钟后台重试，每轮重新 getuploadurl；文件副本放在 ~/workspace 下（/tmp 扛不过 VM 替换）；最终失败时桥接直接说明原因，回复「重发」即可 | 5 次断连：v3 第 3 次即失败，v4 第 2 轮送达；真实 serve 上 8 次断连，第 3 轮送达，文字在附件之前 |
| 2 | “你在 App 发给我”和 11:39 那条，截至 12:58 微信里都没有回复 | 截图本身定不了原因。v3 里能造成“静默无回复”的路径有四条：发送只看 ret 不看 errcode（`{"errcode":-14}` 被记为已发送）；缺 message_type 的消息被丢弃；执行者只在 App 交付、不 complete 时任务每 10 分钟被回收重跑，没有上限，微信里始终没回复；complete 结果为空时发出一条空消息 | 四条都修了。另外：消息 15 分钟无人接手时桥接主动说明；回收 2 次后第 3 次停下并如实告知；EXECUTOR.md 要求每条任务都在微信里 complete，结果在 App 也要说一句 | 4 条都有 v3 对照用例 |
| 3 | “可以在微信里给我发语音消息吗？”——Muse 回答只能发音频文件 | 协议上可以发语音条（item type 3，SILK 编码，encode_type 6，需要 playtime），但标准库和 ffmpeg 都不支持 SILK | `--attach-as voice` / `send-media --as voice`：ffmpeg 转 24 kHz PCM，用可选的 silk-python/pilk 编码；没装编码库、超过 60 秒、或服务端拒收语音条时自动改发音频文件 | 18 秒 MP3：voice_item 字段正确；服务端拒收：改发文件并记住 24 小时 |

## Muse 提的需求

| 项 | 实现 | 说明 |
|---|---|---|
| 1. 微信内多轮上下文 | 对话历史 history/chat.jsonl（用户消息、已发出的回复和文件）；claim 输出带 `conversation`：之前最近 8 轮（最多 2400 字）+ 执行者自己维护的 memo | 不依赖 App 同步；被领取的那条消息本身不重复出现；指令类消息不计入 |
| 2. 状态自查 | 微信发「状态」，serve 直接回复：运行时长、24 小时启动次数、登录、最近轮询、近 1 小时轮询失败、连接复用、限流、积压、文件队列、整体结论 | 不唤醒执行者、不耗 token，执行者链路坏了也能用；不含 token、用户 ID、代理地址 |
| 3. 生成物直发 | `complete --attach FILE`（可多个，文字先发）、`send-media`（自动判断图片/视频/文件，不指定收件人时发给绑定用户） | 附件校验不过时整个 complete 不生效，什么都不发 |
| 4. 失败原因明示 | 入站：超限（报实际大小和上限，文件按 len 字段在下载前判断）、微信 CDN 解密问题、网络重试耗尽（约 2 分钟 6 次）、缺下载地址、md5 不符，桥接直接回复原因和办法；出站：文件最终失败、送达不确定、回复重试耗尽，同样直接说 | 带用户文字的消息和未归类的错误仍交给执行者（附 media_error_hint） |
| 5. 语音 | 见上表第 3 行；收：没有转写的语音下载后交给执行者，有解码库时转 WAV；`voice-to-wav` 命令 | 社区记录的坑（pysilk 要 4 个参数、0x02 前缀不能加两次、缺 playtime 显示 1 秒）都已处理 |
| 6. 微信与 App 共享记忆 | `bridge.py history`：在 App 里查看微信对话；memo 由执行者维护；MEMORY.md 仍归 Muse 管 | 两边实时同步做不到，这一点同意 Muse 的判断 |
| 7. 换 VM 不掉线 | `ops/recover.sh` 一键恢复；SILK 库装在 vendor/py | hook 本来就会在 VM 替换后自动重装单元并重启（OPS 实测 24 秒）。会丢的是 /usr、/tmp 下的东西，所以依赖检查和 vendor/py 才是重点；“不承诺不断线”的判断不变 |

## v3 代码审查发现的其他问题（都已修，都有测试）

| # | 问题 | 影响 | 修复 |
|---|---|---|---|
| 1 | CLI 命令的日志打印到 stdout | complete 等命令自己发送时，日志行混进执行者要解析的 JSON | 只有 serve 打到 stdout（journald），CLI 打到 stderr |
| 2 | 超过 120 秒的 CLI 发送被 serve 判成“中断” | 长回复在慢链路上还在发，就报“结果不明” | 记录发送进程 pid 和进度时间；进程活着且有进展时不动 |
| 3 | `sending` 时调用 complete 有竞态；已部分送达的任务再 complete 会从第一段重发 | 重复消息 | 两种情况都拒绝，`send` 续发，`--force` 才替换 |
| 4 | CLI 兜底发送无视 -2 限流 | 越发越限 | 限流期间交给 serve |
| 5 | 重试耗尽后人工 `send` 只有 1 次机会 | 不稳定时人工补发也常失败 | 人工补发重新计重试次数 |
| 6 | -2 / -14 消耗发送重试次数；-14 还会被当成“分片太长”触发降级 | 会话过期一小时就把回复耗成失败 | 只等待、不计次数（最多 40 次等待） |
| 7 | 媒体发送不识别 ret/errcode 和 -2；getuploadurl 不检查返回码 | -14 只表现为“没有上传地址” | 统一的返回码检查 |
| 8 | 收文件整份进内存 | 16 MB 文件 Python 堆峰值 48 MB | 与视频一样流式：0.1 MB |
| 9 | 用户引用图片/文件提问时只看到“[图片]” | 执行者看不到内容 | 引用的媒体按官方逻辑下载 |
| 10 | 长任务 10 分钟后被回收，第二个执行者重做 | 重复劳动、重复回复 | `progress` 心跳；hook 同样认心跳 |
| 11 | hook 的已报警列表只增不减 | hook 状态无限增长 | 只保留仍为 uncertain 的 |
| 12 | 任务锁不可重入 | 新代码嵌套加锁会死锁 | 线程内可重入 |
| 13 | 发图/发文件/发视频三份几乎相同的代码 | 改一处漏两处 | 合并为一套 |

## 兼容性与行为变化

- 新任务种类：`command`（快捷指令）、`notice`（桥接提醒）、`voice`（无转写语音）。新任务字段：`media_item`、`quoted_media`、`media_error_code`、`media_error_hint`、`voice_*`、`attachments`、`overdue_notified`；execution 新增 `heartbeat_at`、`reclaims`；send 新增 `sender_pid`、`progress_at`、`notified`、`deferred`、`deferrals`、`manual_resends`。
- claim 输出新增顶层 `conversation`（有内容时）。complete 输出新增 `attachments`。send-image/file/video 的输出改为队列项格式（`ok`、`status`、`queue_id`、`file_name`……），失败时可能是 `queued: true`。
- `UNSUPPORTED_TEXT` 改为说明支持哪些形式；无转写又没有音频的语音有单独说明。
- 回复确定未送达时，自动重试的间隔改为 10 秒、30 秒、2 分钟、5 分钟、15 分钟（次数仍是 6），跨度从约 9 分钟拉长到约 23 分钟。
- 环境开关：`WECHAT_BRIDGE_NOTICES=0`、`WECHAT_BRIDGE_COMMANDS=0`、`WECHAT_BRIDGE_MEDIA_FAIL_DIRECT=0`、`WECHAT_BRIDGE_HISTORY=0`、`WECHAT_BRIDGE_CONTEXT_TURNS`、`WECHAT_BRIDGE_CONTEXT_CHARS`、`WECHAT_BRIDGE_TZ`、`WECHAT_BRIDGE_SILK=off`、`WECHAT_BRIDGE_QUEUE_DELAYS_MS`（测试用）。
- tests/test_offline.py 有一处断言随 v4 行为调整：中断的发送现在会多出一条给用户的提醒。

## 部署（按顺序）

1. **不要和正在运行的那套同时用同一个微信号测试**（getupdates 同一时间只允许一个进程轮询）。
2. `cd ~/workspace/wechat-bridge && ops/backup.sh`。
3. 解包覆盖代码文件：bridge.py、bridge.py.v3、ops/、tests/、hooks/、文档。state/、inbox/ 不受影响。
4. 跑测试，全部应通过：
   ```
   python3 tests/test_offline.py --bench
   python3 tests/test_watchdog.py
   python3 tests/test_proxy_failover.py bridge.py
   python3 tests/test_v3.py
   python3 tests/test_serve_e2e.py
   python3 tests/test_hook.py
   python3 tests/test_v4.py
   python3 tests/test_v4_e2e.py
   ```
5. **安装新 hook**：`cp hooks/wechat-bridge-inbox.sh ~/hooks/scripts/`，并把 hooks/wechat-bridge-inbox.json 里的 prompt 更新到 hook 定义。不装的话，同一次不确定送达会被桥接和执行者各提醒一次。
6. 可选：`pip install --target ~/workspace/wechat-bridge/vendor/py silk-python`（语音条和无转写语音的解码）。
7. `systemctl restart wechat-bridge`，约 40 秒后 `python3 bridge.py status`、`python3 ops/doctor.py`。
8. 在微信里发「状态」，应由桥接直接回复。
9. 跑一天后看 `ops/report.py` 的 media queue、bridge notices 两行。

## 回滚

`cp bridge.py.v3 bridge.py && systemctl restart wechat-bridge`。回滚前先看 `python3 bridge.py queue`：v3 不认识出站队列，队列里没发出去的文件不会再发；`status` 的 tasks_by_status 里不要有 kind=voice 的 downloading 任务。新 hook 与 v3 兼容（v3 不写 `send.notified`、`heartbeat_at`，hook 退回 v3 行为），不必回滚。

## 验证范围

通过的测试（合计 91 项）：test_offline 8 组（含 --bench）、test_watchdog 10、test_proxy_failover 5、test_v3 17、test_serve_e2e 8（v3 的测试原样通过，除上面提到的一处断言），test_hook 17（新增 3）、test_v4 19、test_v4_e2e 7（真实 serve 进程 + CLI 进程，本地 TLS、代理和会断连的假 CDN；SILK 编码库用的是测试替身）。

沙箱里验证不了、需要上线实测的：
- 语音条在真实服务端和客户端的表现（社区实现标为实验性）；服务端拒收时会自动改发文件，但“收下了却显示异常”这种情况桥接看不到，需要你看一眼。
- 真实的 silk-python / pilk 接口（测试用的是同接口的替身模块）。
- sendmessage 的 errcode 语义，以及引用媒体（ref_msg）是否总带可下载的 CDN 引用。
- 截图里两条消息没有回复的真正原因：可以用 `grep -n "<message_id>\|ignored\|without message_id" logs/bridge.log` 和 inbox 里对应的任务文件确认。

---

# wechat-bridge v3 优化（2026-11-02）

v3 基于 v2（2026-11-01 包）和上一轮的代理故障转移补丁。修改前逐项对照了官方插件 @tencent-weixin/openclaw-weixin 2.4.10 的源码，以及社区实现 jeffkit/ilink-hub、wechat-ilink、wechat-clawbot、epiral/weixin-bot、lib-x/ilink。

所有改动都有离线测试。表中“测试结果”一列来自 tests/test_v3.py 和 tests/test_serve_e2e.py，运行环境是本地的假 iLink（TLS）加假代理，**不是真实链路的数据**。

## 修了什么

| # | 问题（v2） | 影响 | 修复 | 测试结果 |
|---|---|---|---|---|
| 1 | 每个请求都新建连接（urllib 还显式发 `Connection: close`）：连代理 → CONNECT → TLS 握手 | 长轮询、每段回复、在线状态都要多付几个跨境往返 | 用标准库实现 keep-alive 连接池，复用前检测对端是否已关闭；幂等请求遇到陈旧连接自动换新连接重试；sendmessage 只复用 8 秒内用过的热连接 | 8 次调用的隧道数 8 → 1；80 ms RTT 下单次耗时 325 → 150 ms；真实 serve 进程 12 次调用只用 2 条隧道 |
| 2 | 连接与读取共用 45 秒超时，连接阶段超时被当成“长轮询正常超时” | 代理黑洞时 get_updates 返回空结果，心跳照常刷新，hook 看不到故障 | 连接阶段（TCP+CONNECT+TLS）单独 12 秒超时，记为 `connect` 并计入轮询失败 | v2 报“健康的空轮询”；v3 在 1 秒（测试值）内判为失败 |
| 3 | 发送遇到网络错误一律 send_uncertain | 连都没连上的回复也要等人工决定 | 区分出 connect（请求没发出，确定未送达），自动重试 6 次，间隔 10/30/60/120/300 秒 | v2 → send_uncertain；v3 → send_failed，10 秒后重试 |
| 4 | EXECUTOR.md 里的补发命令 `send` 对 send_uncertain 直接拒绝；部分送达的任务无法补发；补发会从第一段重来 | 人工确认要补发却发不出去，或者重复已送达的内容 | 按字符偏移记录每段分片的送达进度，并记下在途分片的 client_id；`send` 视为人工确认，从第一段未确认的分片续发，在途分片沿用原 client_id | v2 拒绝执行；v3 只补未送达的部分 |
| 5 | 重试耗尽的 send_failed 一直停在原地 | 回复丢了也没人知道 | 耗尽后转为 send_uncertain，并标记 exhausted、not_delivered，由 hook 一次性报警 | ✓ |
| 6 | 分片上限 1800 字 | 长回复要多次请求，多出几个可能“部分送达”的断点 | 改为官方 textChunkLimit 的 4000 字；服务端若拒收长分片，自动按 1800 重切并记住 24 小时 | 9029 字的回复：6 次 → 3 次 |
| 7 | 不识别 -2 | 限流时每 2–15 秒重试一次，越重试越限流，还会计入“游标卡死”重置 | 发送遇 -2 推迟 90 秒（wechat-ilink 的默认值），其余发送一起等，用户发来消息即解除；轮询遇 -2 等 30 秒，不计入游标重置 | ✓ |
| 8 | CDN 上传只在 HTTP 5xx 时重试 | 代理断一次连接，发图/文件/视频就整体失败 | 和官方 cdn-upload.ts 一样，网络错误也重试 3 次 | v2 第一次断连即失败；v3 断两次仍成功 |
| 9 | 上传时明文和密文整份放进内存 | 50 MB 视频的内存峰值超过 100 MB | md5 计算、openssl 文件到文件加密、64 KB 分块上传，全程流式 | 24 MB 视频：Python 堆峰值 72 MB → 2.0 MB；端到端核对密文逐字节一致 |
| 10 | 媒体下载断一次就失败 | 用户只能重发 | 下载是幂等的 GET，重试 3 次（间隔 2/5 秒）；解密失败这类确定性错误不重试 | ✓ |
| 11 | 主动消息（send-direct、send-image --to-user）不带 context_token | 官方要求每次外发都原样回带 | 按用户保存最近一次的 context_token（同官方 contextTokenStore），主动消息自动带上，并输出 token 的年龄 | ✓ |
| 12 | 语音一律回复“暂不支持” | 用户只能改成打字 | 有微信语音转文字（voice_item.text）时当作文字任务处理，与官方一致；没有转写的语音仍回固定提示 | v2 → unsupported；v3 → text |
| 13 | 引用的消息内容丢失；没有 message_id 的消息被丢弃 | 执行者看不到用户引用的是哪条 | claim 输出增加 `quote` 字段；message_id 缺失时用 item 的 msg_id（同官方） | ✓ |
| 14 | 执行期间用户收不到任何反馈 | 可能干等几分钟 | 显示“对方正在输入”：typing_ticket 按用户缓存（24 小时内随机刷新，失败指数退避），处理期间每 5 秒保活，完成后取消；尽力而为 | 真实 serve：保活 2 次，回复后取消 |
| 15 | complete 在 CLI 进程里新建连接发送 | 回复的关键路径上多一次握手 | 回复交给正在运行的 serve：SIGUSR1 唤醒其发送线程，复用热连接；8 秒内没人接手则由 CLI 自己发；只对在心跳里声明支持交接的 serve 发信号 | 发信号后 52 ms 发出；端到端含 CLI 启动共 206 ms |
| 16 | 重新扫码后仍用旧游标，并继续 -14 暂停 | 扫码成功了，桥可能还要再等最多 1 小时 | token 变化时清空游标、-14 暂停、auth 报警和 context token（同 epiral 与 wechat-ilink）；运行中的 serve 检测到凭据变化后直接切换，无需重启 | v2 扫码后仍暂停 60 分钟；v3 立即恢复 |
| 17 | 每次停止或重启都记一条 `getUpdates unexpected error … GracefulStop` | report 和 doctor 的轮询错误数被虚高，干扰定位 | GracefulStop 改为继承 BaseException | 端到端日志干净 |
| 18 | 失败日志只有 kind=network | 判断不了断在哪一层 | 日志加分层 detail（dns/refused/timeout/tls/tunnel/closed/reset），URL 和 IP 已脱敏；report.py 按层统计，并修正从未匹配上的“游标重置”计数 | ✓ |

另外，上一轮的代理故障转移补丁是基于 v1 加游标补丁写的，不能直接打在 v2 上。这次已移植到 v2，并改为走新的连接池。原测试 test_proxy_failover.py 一行未改即通过。

## 兼容性与行为变化

- 新增状态文件 `context_tokens.json`、`ratelimit.json`、`send_tuning.json`、`proxy_pool.json`，以及可选的 `proxy-candidates.txt`。均为 0600；除 context_tokens 外都不进备份。
- 任务 JSON 新增 `quote`、`voice_transcribed`。send 记录新增 `done_upto`、`pending`、`max_attempts`、`not_delivered`、`exhausted`。
- `bridge.py send` 现在等同于人工确认的补发，见第 4 项。
- `complete` 的输出多了 `send.via`。
- send-direct 的输出多了 `via`、`context_age_h`、`context_stale`。
- 心跳新增 `handoff_pid` 和连接复用、输入状态的计数。
- hook 脚本没有改动。hook 定义的提示词加了一句 quote 字段说明，可选更新。
- 开关：`WECHAT_BRIDGE_TYPING=0` 关闭输入状态，`WECHAT_BRIDGE_HANDOFF=0` 让 complete 改回自己发送。

## 部署（按顺序）

1. **不要和正在运行的那套同时用同一个微信号测试**。getupdates 同一时间只允许一个进程轮询，ilink-hub 文档有说明。要么先停掉现在这套，要么换个号测。
2. `cd ~/workspace/wechat-bridge && ops/backup.sh`，先做快照。
3. 解包覆盖代码文件：bridge.py、ops/、tests/、hooks/、文档和 bridge.py.v2。state/ 和 inbox/ 不受影响。
4. 跑离线测试，全部应通过：
   ```
   python3 tests/test_offline.py --bench
   python3 tests/test_watchdog.py
   python3 tests/test_proxy_failover.py bridge.py
   python3 tests/test_v3.py
   python3 tests/test_serve_e2e.py
   python3 tests/test_hook.py
   ```
5. `systemctl restart wechat-bridge`，约 40 秒后跑 `python3 bridge.py status` 和 `python3 ops/doctor.py`。
6. 跑一天后看三处：
   - `status` 的 `net_reused` 应持续增长，说明连接复用在生效。
   - `ops/report.py` 的 `poll error layers`，看失败断在哪一层。
   - `typing_sent`。
7. 可选：把备用代理写进 `state/proxy-candidates.txt`，代理故障转移才有切换对象。

升级过程中，旧版 serve 没有在心跳里声明 `handoff_pid`，新的 complete 不会给它发信号，会自己直接发送，所以不会误杀旧进程。

## 回滚

`cp bridge.py.v2 bridge.py && systemctl restart wechat-bridge`。

v2 会忽略 v3 新增的字段。用 `--max-attempts 6` 排队的重试，在 v2 下按 3 次处理。

## 验证范围

通过的测试：
- test_offline：8 组，含 --bench 基准
- test_watchdog：10 项
- test_hook：14 项，沙箱没有 jq，用 jq shim；旧版 hook 不在包里，所以新旧对比项跳过
- test_proxy_failover：5 项，原样
- test_v3：17 项，其中 v2 对照使用 bridge.py.v2
- test_serve_e2e：8 项，真实的 serve 进程加 CLI 进程，跑在本地 TLS 和代理上

沙箱里验证不了、需要上线实测的：
- 真实服务端是否接受 4000 字分片（已有自动降级兜底）。
- 真实服务端和代理对空闲连接的关闭时长（看 `net_reused` 是否增长）。
- “正在输入”在微信客户端的实际显示。
- voice_item.text 是否总有内容。
- -2 的确切含义：wechat-ilink 认为是限流，ilink-hub 见过它表示“prepare failed”。
- 服务端是否按 client_id 去重。代码不依赖去重，只是续发时沿用 client_id，以便在服务端去重时受益。
- 真实 systemd 下的信号交接。

## 没做的（以及原因）

- SOCKS 代理：当前环境是 HTTP 代理。wechat-clawbot 有 SOCKS 扩展，需要时可以加在 KeepAliveOpener 里。
- 应用层 DNS 候选：桥走 HTTP 代理，域名在代理端解析，本地配 DNS 候选没有作用。v3 的分层日志能确认是否真有 dns 类失败。
- 用机器人消息回显来确认发送成功：ilink-hub 的 README 和它自己的代码注释说法矛盾，所以不依赖。
- getupdates 的 `last_ack_id` / `timeout` 字段：只出现在 ilink-hub 自己模拟的 Hub 接口里，没有证据表明真实服务端支持。
- hook 的 5 秒轮询间隔：由平台的 hook 运行时决定，不在本项目范围内。

---

# wechat-bridge v2 优化（2026-11-01）

基于 2026-10-28 的完整快照审计。所有改动都有离线测试覆盖（tests/）；真实微信链路与真实 jq 未在沙箱验证，见文末。

## 修了什么

| # | 问题（旧版） | 影响 | 修复 |
|---|---|---|---|
| 1 | `update_heartbeat` 被接收/发送两个线程同时写，临时文件同名、读改写无锁 | 实测并发写时 8 个线程 7 个抛异常、7/8 字段丢失；异常会让 serve 进程退出重启 | 临时文件名按线程唯一；flock 串行化；心跳写失败不再上抛；心跳不 fsync |
| 2 | 发送线程每秒两次全量解析 inbox 里所有任务 JSON | 2000 个任务时每秒约 92 ms CPU+IO，且随历史线性增长 | `scan_task_index`：按 (inode, mtime, size) 缓存，目录静止 2 秒后零 stat；实测 0.019 ms/tick |
| 3 | 媒体下载在长轮询线程里同步执行 | 大视频下载超过 180 秒 → hook 判服务死亡并重启 → 下载被打断 | 先落盘 `downloading` 任务，2 个后台线程下载；重启后自动续传 |
| 4 | 游标在消息入库**之前**就落盘 | 入库中途进程被杀 → 该消息永久丢失 | 整批入库后才持久化游标（至少一次 + message_id 去重） |
| 5 | 重复消息先下载再判重 | 重投递会重新下载整段视频 | 先判重再下载 |
| 6 | hook 每 5 秒对每个任务文件 fork 一次 jq，并两次全量 slurp | jq 调用数 ≈ 任务数 + 6/次，历史越多越慢 | 只校验变化文件（批量一次 jq）；grep 预筛后只读相关文件；每 10 分钟全量校验兜底 |
| 7 | hook 对同一个未被领取的任务每 5 秒唤醒一次 | 重复启动执行者（每个都是一次完整 LLM 运行） | 同一任务集 60 秒内不重复唤醒；出现新任务立即唤醒 |
| 8 | 唤醒提示词 3179 字符，含全部视频/文件/出站/故障说明 | 每次唤醒都付全文 token | 精简为 960 字符；详细部分原文移入 EXECUTOR.md，仅非文本任务/故障场景才读 |
| 9 | `claim` 输出含 raw_items（CDN 参数、AES 密钥）、context_token 等 | 白烧 token，且把密钥类字段放进模型上下文 | 默认精简视图（`--full` 保留旧输出） |
| 10 | 每小时 cron 由模型执行 backup + doctor + status | 即使一切正常每小时也是一次完整 LLM 运行 | 备份移入 hook（零 token）；cron 只跑 `ops/watchdog.py`，健康时无输出 |

另：`attempt_send` 在任务被清理时不再抛 TypeError；doctor 增加 `downloading` 卡住检查。

## 兼容性与行为变化
- 新任务状态 `downloading`（hook 只响应 `pending_execution`，不受影响）。
- `claim` 默认输出字段变少：message_id、kind、text；非文本任务另含 media_path/file_name/play_length/media_error（有值时含 media_bytes/thumb_path）。
- 备份由 hook 每小时触发；watchdog 发现备份超过 2 小时才补做。
- cron 的兜底窗口仍是 1 小时。想进一步省 token，可把 cron 间隔改为 3–6 小时（文件名里的 `@1h` 是平台约定，改时需同步）。

## 部署（按顺序）
1. `cd ~/workspace/wechat-bridge && ops/backup.sh`（先快照）
2. `tar -xzf wechat-bridge-v2.tar.gz -C ~/workspace/`（覆盖 bridge.py、ops/doctor.py、README/OPS，新增 ops/watchdog.py、EXECUTOR.md、tests/、hooks/）
3. `python3 tests/test_offline.py && python3 tests/test_watchdog.py`
4. 安装 hook：`cp hooks/wechat-bridge-inbox.sh ~/hooks/scripts/`，`cp hooks/wechat-bridge-inbox.json ~/hooks/definitions/`（路径取自 ops/backup.sh；若平台用界面/API 管理 hook，把 JSON 里的 prompt 字段粘过去）
5. 替换 cron 定义：`cp hooks/health-cron.md ~/workspace/goals/clawbot/crons/hourly/wechat-bridge-health__interval@1h.md`
6. `systemctl restart wechat-bridge`，约 40 秒后 `python3 bridge.py status` 与 `python3 ops/doctor.py`
7. 有真 jq 的机器上再跑一次 `python3 tests/test_hook.py`

## 回滚
`cp bridge.py.v1 bridge.py`；hook 与 cron 从 `backups/<最近时间戳>/` 还原；`systemctl restart wechat-bridge`。回滚前确认 `bridge.py status` 里没有 `downloading` 任务。

## 没做的（以及原因）
- HTTP 连接复用/代理 CONNECT 复用：长轮询本身每 35 秒才建一次连接，收益小，且无法在无网沙箱里验证。
- 把 hook 的 5 秒轮询换成事件驱动：依赖平台 hook 运行时，不在本项目可控范围内。

## 验证范围
通过：tests/test_offline.py（含新旧对比基准）、tests/test_watchdog.py、tests/test_hook.py（沙箱无 jq，使用 tests/jqshim.py 覆盖两版 hook 实际用到的表达式；新增的两条 @tsv 表达式建议在真 jq 上再跑一遍）。
未验证：真实微信服务端交互（沙箱无网络、无凭据）、真实 systemd。
