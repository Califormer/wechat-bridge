# 微信桥运维手册（OPS）

目标：在当前运行环境里让 ClawBot 桥接尽可能接近 7×24 稳定运行——故障自动恢复、
消息不丢失、恢复不了时及时有人（主聊天）知道。本文记录已实测的故障模式、防线与
残余风险；结论以 2026-10-27/28 的多次故障注入实测为准。

## 环境事实（实测）

- **VM 会被频繁替换**（当晚约每 20–40 分钟一次）：`~` 下文件保留；`/etc` 下的
  systemd 单元会被删掉。凭据、游标、任务都在 `~/workspace/wechat-bridge` 下，不丢。
- **出口必须走代理**：systemd 服务以干净环境启动，直连不通。桥接从
  `state/proxy.env`（0600）读取代理变量，由 `bridge.py write-proxy-env` 从调用方
  环境刷新。
- **代理端点是一个池子且会轮换**：不同进程/时段拿到的端点哈希不同，旧端点通常
  仍可用一段时间。所以“proxy.env 与当前环境不一致”只是 WARN，不是故障；
  真正的故障判定看轮询是否成功。端点真失效时轮询失败 → 自愈链会重写文件并重启。
- **桥内代理故障转移**（v3）：`serve` 连续 3 次传输层失败后，后台线程按序探测候选：
  `state/proxy.env` 当前值、可选的 `state/proxy-candidates.txt`（每行一个代理 URL）、
  `state/proxy_pool.json`（最近用过的端点）。**只有当前出口失败而另一个候选
  能通到腾讯节点时才切换**（30 秒冷却，避免上游抖动时来回切）。日志与 status 只显示
  端点哈希（`proxy_id` / `proxy_rotations`），不记录 URL；这两个文件 0600 且不进备份。
  无候选或全部不通时行为与以前相同（仍由 hook 重写 proxy.env 并重启）。
- **到腾讯系节点的出口链路会阵发性抖动**：连接在约 1–1.5 秒被远端切断、成簇出现；
  同出口访问 GitHub/百度稳定。断点在代理上游，环境内不可根治，只能靠重试和
  恢复速度缓解（轮询重试间隔为 1/2/4/8/15 秒渐进）。
- **掉线期间不丢消息**：微信服务端暂存消息，游标恢复后续收。
- **磁盘不是问题**：项目约 25MB，所在卷约 150GB 可用；终态任务与媒体 30 天自动清理。

## 防线（由内到外）

1. **进程级**：systemd `Restart=always`、`RestartSec=5`、`StartLimitIntervalSec=0`
   （永不因重启次数放弃）。强杀（kill -9）实测 10–25 秒新进程接管且轮询恢复
   （大头是服务端首轮询的保持时间，不可控）。
2. **桥接自身**：长轮询单轮异常不退出，并把连续失败计数实时写入心跳
   （供 hook 区分“正在失败”与“首轮询慢”）；发送仅对“服务端明确拒绝”自动重试，
   结果不明标记 `send_uncertain` 绝不盲目重发；**发送中途进程死亡**留下的
   `sending` 任务会在 120 秒后被转为 `send_uncertain` 上报，杜绝回复静默丢失；
   游标连续业务错误自愈重置；
   在线状态（notifystart）持续维护（失败 20 秒重试、10 分钟刷新、恢复即补发）；
   退出不发 notifystop（防重启误报离线，仅显式 `bridge.py stop` 才发）；
   日志 5MB 轮转；终态任务/媒体 30 天清理（`send_uncertain` 不清理，等人工决定）；
   所有状态文件原子写 + 0600；serve 有 flock 单实例锁；
   视频下载/解密全程流式（磁盘→openssl 文件直读→流式 md5），大文件不占内存。
3. **hook（5 秒）**：`wechat-bridge-inbox` 巡检——任务唤醒、卡死任务 10 分钟回收、
   `send_uncertain` 一次性上报、凭据消失/登录失效报警（6 小时节流，健康即清零）、
   服务健康判定（进程存活 + 轮询 180 秒内成功；-14 官方暂停期视为健康；
   新进程未轮询成功时：心跳失败计数 ≥4 且跨度 ≥15 秒立即判故障，
   否则 30 秒启动宽限后判故障）。
   判定不健康时**脚本内联自愈**：刷新 proxy.env、缺失则重装单元、重启服务
   （同一次故障内退避：30 秒、1/2/4/8 分钟，之后每 10 分钟；恢复健康即清零）；持续 90 秒未恢复才唤醒执行者深处理（每小时最多报一次）。
   实测：手动停服+删单元 24 秒恢复轮询。
4. **每小时 cron 兜底**：`wechat-bridge-health`（归属“微信 ClawBot 通道保活”目标），
   防 hook 本身失效；健康时静默，不健康时走同一恢复梯队并向主聊天报告。
5. **备份**：`ops/backup.sh` 快照 state/（含凭据）、bridge.py、单元、hook 与 cron
   定义，保留最近 5 份带日期快照 + 每次优化前一份 + 里程碑（pre-v4、pre-v5、
   opt6 等命名快照），文件 0600；由每小时 cron 顺带执行。凭据文件损坏是唯一
   无法自动修复的故障，靠备份或重新扫码恢复。注意：`proxy.env` 不进备份——
   它可由环境秒级重建，且实测发生过“故障期间做的备份把死代理也备了进去、
   恢复后立刻再中毒”；恢复流程会强制用当前环境刷新它。
   2026-11-02 六-4 留存规矩（先立规矩再删）：backups 只留最近 5 份带日期快照
   + 每次优化前一份 + 里程碑（pre-v4、pre-v5、opt6 等命名快照）；超出部分不自动
   删，先列清单给机主逐项确认，清理前再留一份总快照，绝不断回滚路。注意
   2026-11-02 opt batch 已对齐：backup.sh 不再自动删超出的带日期快照，
   只把超出 5 份的清单写进 backups/.prune-candidates.txt；清理用
   `backup.sh --prune`，先留一份总快照、只删清单里的带日期快照，
   命名快照任何情况下都不碰。
   2026-11-02 晚机主定下长期规矩：以后带日期快照超出 5 份时，先按
   2026-11-02 上次的做法，把超出的部分做成脱敏包（不含 state/、
   credentials.json）上传到 你的云盘 的“备份文件夹”文件夹并在云盘
   列表里核对，再按字节核对本地文件与包内条目、只删已进包且字节一致
   的部分，state/ 副本一律保留本地；这条规矩取代“每次逐项确认清单”，
   但上传核对和字节核对两步不能省。
   注意（2026-11-02 晚收尾已对齐）：`backup.sh --prune` 现在只删
   非 state/ 文件，state/ 一律保留；--prune-list 也只列还剩非 state/
   文件的超出快照，state/ 空壳不再被算进去。但 --prune 仍只是本地删除
   这一步，不能代替“上传脱敏包 + 字节核对”——先上云、核对完再跑它。

## 故障模式矩阵（节选）

| 故障 | 检测 | 自动恢复 | 实测恢复时间 | 残余风险 |
|---|---|---|---|---|
| 进程被杀/崩溃 | systemd | Restart=always | 10–25 秒（服务端首轮询节奏主导） | 无 |
| VM 替换（单元丢失） | hook 巡检（5 秒） | 内联重装单元+重启 | 实测 24 秒 | hook 同时失效时靠 hourly cron，≤1 小时 |
| proxy.env 失效/端点死亡 | 心跳失败计数快速通道（≥4 次/≥15 秒）或启动 30 秒无成功轮询 | 内联重写+重启 | 实测演进 3 分 06 秒 → 1 分 52 秒 → 57 秒 → 45 秒 | 端点池整体不可用时无解（未发生过） |
| 发送中途进程死亡 | 发送层清扫（120 秒） | 转 `send_uncertain` 上报人工决定 | ≤约 2 分钟被发现 | 需用户决定是否补发（防重复的设计取舍） |
| 腾讯链路抖动 | 轮询错误计数 | 渐进重试 1/2/4/8/15 秒 | 抖动结束即恢复 | 抖动时长不可控；期间微信标题可能短暂显示无法连接 |
| 游标/心跳文件损坏 | 读取容错 | 自动重置/重建 | 一轮轮询内 | 无（已注入实测） |
| 单个任务 JSON 损坏 | hook 预检 | 隔离为 `.corrupt` | 10 秒内 | 该条消息需用户重发 |
| 凭据文件丢失 | hook（登录确认过） | 无（报警） | 需用户扫码 | 无法自动登录 |
| 凭据失效（-14 两次） | 桥接 + auth_alert | 无（报警） | 需用户扫码 | 同上 |
| 发送结果不明 | 发送层 | 不重试，上报主聊天 | 需用户决定补发 | 可能重复或遗漏，二选一由用户定 |
| 之前上传过的文件解密失败 | 媒体层 | 无（腾讯 issue #193） | 用户 zip/改名重发 | 腾讯侧 bug，客户端无解 |
| 磁盘写满 | doctor | 30 天清理 + 日志轮转 | — | 当前用量下多年无忧 |

## 运维脚本（ops/）

- `python3 ops/doctor.py [--json]`：只读体检 18 项（凭据、代理漂移、单元、服务、
  心跳新鲜度、状态文件、登录报警、任务积压、磁盘、hook、cron、日志扫描、
  规模一行：inbox/history/归档/引用/backups/claim 上下文字符数）。
  退出码 1 表示有 FAIL。日常排障先跑它。
- `ops/repair.sh [--force-restart]`：手动执行完整修复梯队（刷新代理→修单元→
  隔离损坏状态→按需重启→验证）。绝不触碰凭据与任务文件；发现凭据损坏只报告。
- `ops/backup.sh [--list | --restore <时间戳>]`：快照与恢复。恢复会停服、
  还原 state/ 与单元、重启并验证状态。
- `python3 ops/report.py [--days N]`：从日志与任务文件汇总运行统计
  （重启次数、估算掉线时长、轮询错误、-14 事件、任务分类计数）。

## 恢复不了时（人工流程）

1. 重新登录：主聊天停服 → 生成新二维码 → 用户扫码 → `wait-login` 确认 → 启服。
   不要试图通过已失效的微信通道通知用户。
2. 凭据损坏：先试 `ops/backup.sh --restore <最近时间戳>`；无可用备份再走重新登录。
3. 反复抖动超过 1 小时且非腾讯侧大面积故障：跑 `ops/report.py` 看错误构成，
   再决定是否继续等待（历史经验是链路自行恢复）。

## 诚实的边界

- “7×24 零间断”在此环境不可承诺：VM 替换与腾讯链路抖动都真实存在。
  可承诺的是：**故障自动恢复（秒级到数分钟）、消息不丢失、恢复不了会报警**。
- 微信标题“暂无法连接”在真实掉线窗口内仍会短暂出现，那是服务端如实反映；
  “明明通着却一直显示离线”的假象已通过在线状态维护消除。
- 腾讯若将来强制升级协议版本或调整 ClawBot 政策，桥接可能需要改代码，
  这类变化会先以登录/轮询失败的形式出现并触发报警。

## v2 补充（2026-11，详见 CHANGELOG.md）

- 新增任务状态 `downloading`：`doctor` 对超过 15 分钟的 `downloading` 报 WARN；重启后自动续传。
- 每小时备份改由 hook 脚本直接执行（零 LLM token）；cron 只运行 `ops/watchdog.py`，健康时无输出。
- hook 对同一批待执行任务 60 秒内不重复唤醒；对任务文件只做增量校验（每 10 分钟一次全量校验）。
- 回滚前先确认 `bridge.py status` 的 `tasks_by_status` 里没有 `downloading`（旧版 bridge.py 不认识这个状态）。

## v3 补充（2026-11-02，详见 CHANGELOG.md）

- **连接与超时**：请求复用 keep-alive 连接（复用前检测对端是否已关闭）。连接阶段（TCP+CONNECT+TLS）超时 12 秒，长轮询读取超时不变。
  - 代理黑洞（接受连接却不回应 CONNECT）以前会被记成“空轮询成功”，心跳照常刷新，hook 看不到；现在记为 `kind=connect` 轮询失败，hook 的快速通道（≥4 次 / ≥15 秒）能及时触发。
  - 观测：`status` 的 `net_connects` / `net_reused`（每分钟写入心跳），`doctor` 的 heartbeat 行会显示复用比例。
- **失败分层日志**：`getUpdates error (n) kind=... detail=<层>(...)`，层为 dns / refused / timeout / tls / tunnel / closed / reset 等。
  - `python3 ops/report.py` 输出 `poll error layers` 分布，用来判断问题在哪一层。
  - 停止或重启不再产生假的 `unexpected error` 记录。
- **发送分类**：连接阶段失败（确定未送达）自动重试 6 次；服务端拒绝重试 3 次；超时或中途断连为 send_uncertain，不自动重试；重试耗尽转为 send_uncertain(not_delivered)，hook 一次性报警。
  - `bridge.py send` 为人工确认的续发：从第一段未确认的分片开始，在途分片沿用原 client_id。
- **限流 -2**：发送推迟 90 秒（`state/ratelimit.json`），用户发来新消息即解除；轮询遇 -2 等 30 秒，且不计入游标卡死重置。`doctor` 遇到生效中的限流会给出 WARN。
- **输入状态**：任务在 downloading / pending_execution / executing 期间，每 5 秒发一次“正在输入”，完成后取消，最长 10 分钟。
  - 失败只记日志（每分钟最多一条），不影响任务。
  - 关闭方法：在 `state/proxy.env` 或服务环境里设 `WECHAT_BRIDGE_TYPING=0`。
- **回复交接**：`complete` / `send-direct` 用 SIGUSR1 唤醒 serve 的发送线程，借用它的热连接；8 秒未接手则自己发。
  - 只对在心跳里写了 `handoff_pid` 的 serve 发信号，升级过程中旧版 serve 不会被误杀。
  - 关闭方法：`WECHAT_BRIDGE_HANDOFF=0`。
- **重新扫码**：wait-login 写入新 token 时，会重置游标、-14 暂停、auth_alert、context token 和限流状态；运行中的 serve 发现凭据文件变化后直接切换，不必再等暂停结束或手动重启。
- **新状态文件**：`context_tokens.json`（各用户最近的 context_token）、`ratelimit.json`、`send_tuning.json`（若曾自动降回 1800 字分片）、`proxy_pool.json`、可选的 `proxy-candidates.txt`。均为 0600；除 context_tokens 外不进备份。

| 故障（v3 新增/变化） | 检测 | 自动恢复 | 测试中的表现 |
|---|---|---|---|
| 代理黑洞 / CONNECT 无响应 | 连接阶段超时计为轮询失败 | 重试；满 3 次后探测切换候选代理；hook 快速通道重启 | v2 记为“健康空轮询”；v3 判定为失败 |
| 回复连不上（请求未发出） | `kind=connect` | 10 秒起自动重试 6 次 | v2 会转 send_uncertain，等人工处理 |
| 部分分片送达后中断 | 逐片进度 | 人工确认后从断点续发 | v2 的 send 命令拒绝执行 |
| iLink -2 限流 | ret/errcode -2 | 推迟 90 秒；用户来消息即解除 | — |
| 重新扫码后仍在 -14 暂停期 | 凭据 token 变化 | 立即清除暂停，serve 热切换 | v2 扫码后仍会暂停最多 1 小时 |

## v4 补充（2026-11-02，详见 CHANGELOG.md）

- **出站媒体队列**：`outbox/queue/<id>.json` + 文件副本 `outbox/files/`（0600/0700）。一轮 = getuploadurl（新的上传参数）+ 加密 + CDN POST（3 次快速重试）+ sendmessage；失败的轮次在 8 秒、30 秒、90 秒、5 分钟、10 分钟后重来，共 6 轮约 18 分钟，由 serve 的 media-queue 线程执行。sendmessage 之前的失败都可安全重试；媒体 sendmessage 本身中途断连记为 uncertain，不自动重发。-2/-14 只等待、不消耗轮次。成功后副本立即删除；失败/不确定的保留 7 天供「重发」。`bridge.py queue` 查看，`queue --retry/--cancel` 手动处理。
- **桥接直写提醒**：kind=notice 的任务，走普通回复的发送路径（同样的重试和 context_token），每小时最多 12 条，提醒本身失败不会再生成提醒。触发：回复送达不确定或重试耗尽、文件最终失败或不确定、消息 15 分钟无人接手（合并为一条）、执行者 3 次中途退出、入站媒体的已知失败。关闭：`WECHAT_BRIDGE_NOTICES=0`（回到 v3：由 hook 唤醒执行者）。
- **hook 对应调整**：已被桥接提醒过的 send_uncertain（`send.notified`）不再唤醒执行者；有 progress 心跳的长任务不被当成卡死；已报警列表只保留仍然 uncertain 的任务。**新 hook 需要重新安装**，否则同一次不确定送达会被提醒两次。
- **微信快捷指令**：状态 / 重发 / 帮助（及 /status、/resend、/help）由 serve 直接回复，执行者链路坏掉时同样可用。关闭：`WECHAT_BRIDGE_COMMANDS=0`。
- **对话历史与备忘**：`history/chat.jsonl`（0600，不在 state/ 下、不进备份，超过 1 MB 时裁到最多 600 条且不超过 0.5 MB，裁掉的进 history/archive-YYYY-MM.jsonl）；`state/conversation_memo.json`（进备份）。claim 默认带最近 8 轮、最多 2400 字（`WECHAT_BRIDGE_CONTEXT_TURNS`、`WECHAT_BRIDGE_CONTEXT_CHARS`，设 0 关闭）。用户时区默认 Asia/Shanghai（`WECHAT_BRIDGE_TZ`）。
- **一键恢复**：`bash ops/recover.sh [--notify]`：依赖检查（python3、openssl 必需；ffmpeg、SILK 库可选）→ repair 梯队 → tbtest 缺失时重建（`~/workspace/terminal-browser-headless/rebuild-tbtest.sh`，VM 替换会丢 `/home/tbtest`，重建失败只告警、不影响桥轮询）→ 最多等 60 秒直到轮询成功 → doctor 摘要；`--notify` 恢复后发一条微信。hook 正常时 VM 替换仍由 hook 自动处理（实测 24 秒），这个脚本用于 hook 本身也不工作时。
- **依赖**：SILK 编解码库装到 `vendor/py`（`pip install --target ~/workspace/wechat-bridge/vendor/py silk-python`）：在 ~/workspace 下，VM 替换不丢，root 运行的服务和执行者 CLI 都能找到，装完不用重启服务。apt 装的东西（如 ffmpeg）会随 VM 替换丢失，doctor 的 deps 行会提示。
- **其他新状态文件**：`voice_tuning.json`（服务端拒收语音条的记忆，24 小时）、`serve_starts.json`（「状态」里的 24 小时启动次数），均不进备份。
- **观测**：`ops/report.py` 新增 media queue（按所需轮数分布）、bridge notices、chat commands、executor reclaims 几行；`ops/doctor.py` 新增 deps、media_queue 两项。CLI 命令的日志改走 stderr，stdout 只有 JSON。

| 故障（v4 新增/变化） | 检测 | 自动恢复 | 测试中的表现 |
|---|---|---|---|
| 出站文件 CDN 上传被断连（截图里的情况） | 每轮 3 次快速重试 | 6 轮约 18 分钟，每轮新的上传参数；最终失败桥接直接告诉用户 | 本地 e2e：8 次断连后第 3 轮送达，文字在前 |
| 媒体 sendmessage 中途断连 | phase=message | 不自动重发，标 uncertain 并提醒用户 | ✓ |
| 回复送达不确定 / 重试耗尽 | 发送层 | 桥接直接提醒，「重发」从断点续发 | hook 不再为此唤醒执行者 |
| sendmessage 回 errcode≠0 | v4 同时检查 ret 与 errcode | 按失败处理（-14/-2 等待） | v3 记为已发送 |
| 执行者反复中途退出 | claim 回收计数 | 第 3 次后停下并如实告知 | v3 每 10 分钟重跑一次 |
| 15 分钟无人接手 | sender 每分钟检查 | 合并成一条排队提醒 | ✓ |
| 入站媒体超限 / 解密 / 网络耗尽 | 媒体层 | 桥接直接说原因和办法 | ✓ |

## v5 补充（2026-11-02，详见 CHANGELOG.md）

- **两条硬限制**：用户发消息后 24 小时窗口；每条用户消息后约 10 条回复。桥接按 context_token 记账（`state/send_quota.json`），放不下的部分挂起（任务 `send.deferred=quota|window`、队列项 `deferred`），用户下一条消息时自动续发。`python3 bridge.py stats` 的 `observed_limits` 是服务端提前拒绝时实际允许的条数；和 10 不一致时设 `WECHAT_BRIDGE_CTX_QUOTA`（写进 state/proxy.env 或服务环境）。
- **窗口关闭时的报警**：send-direct 直接返回 `context_expired`（退出码 3），执行者改在 App 里报告；doctor 的 reply_window 在用户 20 小时没说话时开始 WARN。
- **SIGHUP**：`systemctl reload wechat-bridge` 或 `kill -HUP <pid>` 让 serve 重读 proxy.env、进程内切换出口，不丢线程、队列和会话。hook 遇到轮询失败时先这样做，20 秒没恢复才重启；只对心跳里声明了 `hup_pid` 的 v5 serve 发。
- **systemd**：Type=notify + WatchdogSec=180。回滚到 v4 时单元必须一起换回 `wechat-bridge.service.v4`，否则 v4 不报 READY，会启动超时、反复重启。
- **热执行者**：`claim --wait` 期间写 `state/claim_waiter.txt`（pid 和截止时间），hook 看到活着的等待者就不唤醒新执行者。等待者如果在领取后意外退出，任务仍按原规则 10 分钟后回收。
- **hook 间隔**：`poll_interval_secs` 为 5（平台最低 5 秒，设 2 秒会让 hook 失效，见 CHANGELOG 2026-11-02 22:46 条；仓库模板 2026-11-04 起同步为 5）。
- **新文件**：`history/quotes.db`（引用库，0600，30 天、1 万条，不进备份）；`state/token_history.json`（最近 10 个 bot token，用于扫码时的 local_token_list，0600，随 state/ 进备份）；`state/dedupe.json`（canary-dedupe 的结论）。
- **下载续传**：`.part` 残片保留到下载成功或 24 小时；服务器不支持 Range 时自动整段重下。

| 故障（v5 新增/变化） | 检测 | 自动处理 | 测试中的表现 |
|---|---|---|---|
| 用户超过 24 小时没说话，主动消息被拒 | 发送前查窗口；-2 分类 | 不发、挂起；主动消息报 context_expired；用户来消息即补发 | v4 每 90 秒重试一小时 |
| 回复超过约 10 条 | 本地记账；-2 分类 | 末尾一句说明，其余挂起，下一条消息后续发 | 12 段回复：10 段送达 + 提示，「继续」后补齐，无重复 |
| 服务端额度比本地设定小 | 提前的 -2 | 按额度处理并记录实际条数 | 额度 6：第 7 段起挂起，后续补齐 |
| 下载在中途被切断 | Content-Length 校验 | 重试并 Range 续传 | v4 当成完整文件，再误报“解密问题” |
| 代码块被分片切开 | 分片器 | 自动闭合并重开 | v4 后一条的 ``` 吞掉正文 |
| 轮询失败但进程活着 | hook | 先 SIGHUP 重读代理，20 秒后才重启 | e2e：同一进程切到代理继续收消息 |
| 轮询循环卡死、hook 也失效 | systemd 看门狗 | 180 秒内由 systemd 重启 | sd_notify 收到 WATCHDOG |

## 瘦身（2026-11-02 晚，机主让 Muse 安排顺序后做的第一批）

- **总快照只留最新一份**：`.total-before-prune-*.tar.gz` 之前攒了两份内容相同的
  （20261102-222214 与 222224，条目表同为 262 条），222214 已删，只留 222224。
  以后清理前仍先留总快照，但旧总快照不再长期保留。
- **旧版本归档**：根目录的 bridge.py.v1、v3 已移到 archive-old-versions/，
  仅作历史参考，不再是回滚点。当前回滚点是根目录的 bridge.py.v4 和
  backups/ 下的命名快照（pre-opt7、pre-resume、pre-opt8、pre-slim）。
  test_v4 拿 v3 做新旧对照，v3 移走后对照自动跳过，测试本身仍跑新行为断言。
- **快照里的 bridge.py 用硬链接去重**：backup.sh 打包时，若新快照的 bridge.py
  与最新一个带日期快照的 bridge.py 哈希相同，就用硬链接代替拷贝，磁盘上只占
  一份，删掉其中一个快照不影响另一个。注意：硬链接的快照文件不要就地编辑，
  要改先拷出来，否则会连带改掉另一个快照。
- **快照不再存运行时临时文件**：backup.sh 打包 state/ 时，除了原有的
  proxy.env 等排除项，又排除了 `*.lock`、serve.pid、.hook_full_scan、
  .hook_scan_mark——这些是进程活着时才有意义的临时文件，回滚用不上。
- **发送主入口**：对外发媒体、发文件的主入口是 `send_media` 和
  `complete --attach`（走附件队列）；cmd_send_image、cmd_send_file、
  cmd_send_video 只是 legacy 包装，最终也走同一条队列。以后改发送逻辑先改
  主入口，不要三处各改各的。
- **日常回归集（冻结集不跑）**：日常只跑 v5、resume、round2、offline、hook、
  watchdog、serve_e2e、proxy_failover。test_v3 是所有套件共用的夹具文件，
  不能移走（v4、v5、resume、round2、opt 系列都 import 它）；test_v4、
  test_v4_e2e、test_opt、test_opt6、test_opt6b 是已冻结的代际/批次测试，
  不再列入日常回归，文件留在原地不动，避免移走后夹具 import 断掉。
- **入站媒体过期**：任务关联的媒体本来就随任务 30 天清理一起删
  （prune_old_tasks 按 media_path、thumb_path、voice_raw_path 删）；
  2026-11-02 瘦身又加了孤儿媒体清理：每小时清理时，inbox_media 里超过
  30 天、且没有任何任务引用的文件会被删掉，近期的媒体和「最近文件」不受影响。
- **两个瘦身时的实测修正**：(a) 快照名混了两种时区——cron 跑的 backup.sh
  用 UTC 时间戳（如 20261103-022244），交互式跑的用本地时间戳
  （如 20261102-223348），按名字排序不等于按创建时间排序；硬链接去重只认
  内容哈希，所以不受影响，但“最新 5 份”按名字算这一点要知道。(b) 瘦身时
  test_offline 曾因 v1 移走而报错，已把它的旧版引用改到
  archive-old-versions/bridge.py.v1；test_v4 的 3 个语音失败是瘦身前就有的
  （用瘦身前 bridge.py 复测也是同样 3 个失败，原因是语音默认改文件卡片后，
  旧测试仍期望原生语音尝试），不是本次回归。

## 收尾三件小事（2026-11-02 晚，机主说“收个尾”后做的）

- **--prune 对齐新规矩**：--prune 现在只删超出快照里的非 state/ 文件，
  state/ 一律保留，删前仍先留总快照；--prune-list 只列还剩非 state/
  文件的超出快照，state/ 空壳不再被算进去。实测：改完后 --prune-list
  正好列出 222226、221356（6 个 state/ 空壳被排除在外），没有真的跑
  --prune——221356 还没上云，不能删。
- **test_v4 语音测试对齐默认**：g_voice 里的原生语音场景改用
  WECHAT_BRIDGE_NATIVE_VOICE=1 显式开启，末尾新增默认场景断言（不设
  变量时语音走文件卡片、不发 type-3）；旧版对照点改到
  archive-old-versions/bridge.py.v3。改完 test_v4 全部通过。
- **快照时区统一**：backup.sh 的时间戳一律用 America/Caracas，
  不管是 cron 还是手动跑都一样；原来 3 份 UTC 名的快照已改名迁移
  （20261103-022244→20261102-222244、012241→212241、002236→202236），
  改名后按名字排就等于按创建时间排，改名后 --prune-list 列出的是
  212241、202236。总快照里的旧名字是历史归档，不用改。
- **常用环境变量（其余 20 个左右见代码，默认不改）**：
  WECHAT_BRIDGE_CTX_QUOTA（每条消息回复上限，默认 10）、
  WECHAT_BRIDGE_CONTEXT_CHARS（claim 上下文上限，默认 4000 字）、
  WECHAT_BRIDGE_CONTEXT_TURNS（claim 上下文轮数，默认 10）、
  WECHAT_BRIDGE_AUTO_PROGRESS_SECS（自动进度阶段，默认 180,600）、
  WECHAT_BRIDGE_AUTO_PROGRESS_SEC（单阶段兼容别名，设了它就以它为准）、
  WECHAT_BRIDGE_TZ（桥接显示时区，默认 Asia/Shanghai）、
  WECHAT_BRIDGE_NATIVE_VOICE（=1 才尝试原生语音，默认走文件卡片）、
  WECHAT_BRIDGE_MARKDOWN（Markdown 处理模式，默认 filter）。

## 2026-11-04 审查修正（待发布，版本号由机主定，详见 CHANGELOG.md）

- **消息不再因存储失败丢失**：入库遇到存储错误（磁盘满等）时不推进游标，下一轮重拉同一批（message_id 去重）；心跳 `ingest_fail_streak`，doctor 的 pipeline 行 ≥3 次判 FAIL。磁盘满类错误一直重试；其他存储错误重试 30 轮后跳过那一条。
- **"连得上但不回包"可见了**：长轮询客户端超时（含 TCP keepalive 判死）不再记成成功轮询；连续 3 次（`WECHAT_BRIDGE_POLL_SILENT_FAIL_AFTER`，0 关闭）起计为轮询失败，心跳 `last_poll_ok_at` 停止前进，hook/watchdog 照常处置；服务端 `longpolling_timeout_ms` 夹在 1–60 秒。
- **文件权限**：所有原子写（含 credentials.json）临时文件创建即 0600；进程 umask 默认 077（`WECHAT_BRIDGE_UMASK`，执行者与服务不是同一用户时可设 007），启动时收紧 state/、inbox/、inbox_media/、inbox_files/、outbox/、logs/、history/ 的多余权限位。
- **令牌只发往允许的地址**：baseurl 和扫码 redirect_host 只认 https `*.weixin.qq.com` 与本机回环；自建中转（如 ilink-hub）用 `WECHAT_BRIDGE_API_ORIGINS="https://hub.example.com"` 放行。
- **领取令牌**：claim 输出 `claim_id`；complete/progress/heartbeat 带 `--claim-id` 时，任务已被别的执行者接手就拒绝（stale_claim）；被回收但还没人接手时，原执行者的心跳会把任务要回来，不重跑。不带 `--claim-id` 的旧调用照常接受。
- **停服务**：SIGTERM 后最多等 12 秒（`WECHAT_BRIDGE_SHUTDOWN_GRACE_SEC`）让工作线程做完手上那一步，发到一半的回复记为 send_failed、重启后从断点续发，不再误报"不确定送达"。
- **其他开关**：`WECHAT_BRIDGE_DECISIONS_CHARS`（claim 带的决策清单预算，默认 3000，超出时先省略已完成/回灌节）、`WECHAT_BRIDGE_HISTORY_ARCHIVE_MONTHS`（历史归档保留月数，默认 0 = 全留）、`WECHAT_BRIDGE_AUTO_ATTACH_DIRS`（自动附件只认这些目录，冒号分隔）、`WECHAT_BRIDGE_VIDEO_COMPRESS_SOURCE_MAX_MB`（超过 50 MB 时可压缩发送的源视频上限，默认 300）、`WECHAT_BRIDGE_SNAP_TZ`（备份快照时区，默认仍是 America/Caracas）。
- **备份**：未变化的 state/ 文件与上一份快照硬链接（每份快照仍完整，但凭据不再每小时多一份物理副本）。
- **单元文件加固建议（按 CLAUDE.md 未改单元，需要时由你决定）**：`UMask=0077`、`PrivateTmp=yes`、`ProtectHome=read-only` 配 `ReadWritePaths=/home/USERNAME/workspace/wechat-bridge`、`RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6`、`LockPersonality=yes`。长期看应改成专用用户运行，hook 只需 sudoers/polkit 授权 `systemctl restart wechat-bridge` 这一条；在此之前，bridge.py、vendor/、state/proxy.env 可被谁写入，就等于谁能以 root 执行代码。

## 2026-11-05 调优：先看数据（待发布）

**调节位置**：`state/tuning.env`（KEY=VALUE，只认 `WECHAT_BRIDGE_*`；serve 和每条 CLI 启动时读取；改完 `systemctl restart wechat-bridge` 让 serve 生效，CLI 立即生效）。常用项：

| 键 | 默认 | 作用 |
|---|---|---|
| `WECHAT_BRIDGE_WARM_WAIT_S` | 45（代码默认）；线上 90，已验证，2026-11-05 测试1 | `--wait auto` / `--then-wait auto` 的热等待秒数（5–300） |
| `WECHAT_BRIDGE_HOOK_LINGER_MS` | 0（代码默认，关）；线上 3000，已定案保留，2026-11-05 Step 3 | hook 每次运行最多停留多久等新消息（≤4000） |
| `WECHAT_BRIDGE_ECHO_MODE` | observe | observe 只记回显台账；resolve 用回显确认"不确定"分片；off 忽略 |
| `WECHAT_BRIDGE_BATCH_QUIET_MS` | 3000 | 批处理：对方停顿这么久（没有新消息）就整批派发（2026-11-06；原 `WECHAT_BRIDGE_SETTLE_SHORT_MS` 的 claim 合并窗口已删除，见末节） |
| `WECHAT_BRIDGE_BATCH_MAX_MS` | 60000 | 批处理：从这一批第一条起最多等这么久，到点强制派发 |
| `WECHAT_BRIDGE_BATCH` | 1 | 0 = 关掉批处理和打断（应急开关：逐条立即派发，没有任何合并） |
| `WECHAT_BRIDGE_CLAIM_MEDIA_HOLD_MS` | 15000 | 紧跟在下载中媒体后的提问最多等它多久 |
| `WECHAT_BRIDGE_PREDIGEST` | 1 | 媒体预处理（0 关） |

**怎么判断（`python3 wb stats`，看 evidence）**：
- 热等待开多长：`followup_gaps.within` 给出 45/90/180/300 秒窗口各能接住多少追问；改 `WARM_WAIT_S` 后看 `exec.warm_claims / cold_claims` 和 `wake_warm / wake_cold`。90 秒已定案（2026-11-05 测试1：70 秒的追问被接住，100 秒的超出、走冷领取；warm 唤醒 p50 1203 ms，cold 约 4443 ms），不要改回 45。
- hook 停留：3000 已定案保留（2026-11-05 Step 3，线上实测 n=5 条冷消息：唤醒中位数 2632 ms，基线 4731 ms；4 条在收到后 50–103 ms 触发 rerun；start-to-start 仍是 5000 ms，平台从运行开始计 5 秒，周期没变 8 秒）。再调（例如 4000）按单变量做，只看新样本：`latency_ms.wake_wait` 的 p50 / p95、`hook_ticks.reruns` 与 `linger_wakes`（应相等）、`skipped_ticks`、`duplicate_wakes`；`hook_ticks.schedule` 给出平台的 5 秒从哪儿算（只有停留时分得出来）。
- 回显：`echo` 给出已发送的回显比例、延迟、按类型、"不确定"分片后来有没有回显。只有回显对已送达接近 100% 可靠、并和真实送达对上后，才考虑 resolve；自动重发不在本版范围内。
- 送达 0.68 秒去哪了：`exec.pickup / pre_send / network / post_send`。
- 执行时间：`exec.calls`（每条任务的 CLI 调用次数）、`to_first_call`、`exec_no_calls` 对比 `exec_with_calls`。每列都有 `max`，长尾不要只看 p50/p95（n 小时 p95 会把最大值排除在外）；`to_first_call_top` / `exec_no_calls_top` 给出最慢几条的任务 ID。
- 第一次进度（2026-11-05 第二轮）：`python3 wb stats --recent 3` 或 `--task-id <id>` 看单条任务的 `to_first_call_ms`（第一次 progress/heartbeat 距领取的毫秒数）、`calls_ms`、`notices`（reason=progress 是执行者 --notify，auto_progress 是桥自动发的）和 `first_notice_sent_ms`。验收只看新增样本，不看累计百分比。`ops/doctor.py` 的 `latency_tail` 一行给出同样的长尾摘要（to_first_call max 和任务、总计/处理 p95、n），只是信息：延迟由模型决定，不是桥的健康指标，这一行永远 PASS。

## 2026-11-05 审计修正（§8 第 1–5 条，待发布，版本号仍 5.0.5，详见 CHANGELOG.md）

- **woke 计数**：hook 运行时的 `wake()` 会直接结束脚本，所以每处 wake 之前先写 `woke=1` 和 `wake_sig`（任务：排序后 id 集合的 cksum；送达不确定：`u`+cksum；登录 `auth`；服务 `svc`）。以前写在 wake 之后，永远执行不到，`stats` 的 `wakes` / `linger_wakes` / `duplicate_wakes` 一直是 0。测试桩的 `wake()` 也改成会退出，和线上一致。上线后核对：冷消息触发停留后，`hook_ticks.reruns` 与 `linger_wakes` 应相等且大于 0。
- **tick 日志保留**：`state/hook_ticks.log` 保留最后 18000 行（约 25 小时，1 MiB 时轮转），`hook_ticks` 窗口最长 24 小时（`window_h`），`span_h` 是窗口里的数据实际回溯多久。tick 日志不再进每小时备份（诊断日志，不用于恢复）。
- **提示词同步（每次提示词有变都要做）**：线上定义只能用 hooks 工具整段更新；同时把同一段文字写进工作区 `hooks/wechat-bridge-inbox.prompt.txt`、`hooks/wechat-bridge-inbox.json` 的 `prompt` 字段（只改这一个字段）和 `hooks/wechat-bridge-inbox.prompt.sha256`。json 文件里 `delivery` 是 `main`，线上定义是 `side_chat`，两边不要互相覆盖。`tests/test_firstcall_20261105.py` 和 `tests/test_audit_20261105.py` 会核对三者一致。
- **测试临时目录**：`tests/run_all.py` 给每个测试文件单独的 TMPDIR（`<系统临时目录>/wb-runall-*/NN-*`），该文件跑完立刻删，整套跑完不留东西；开跑前清掉以前留下的测试目录（只认测试前缀 + 8 位随机后缀、超过 10 分钟没动的；`vframes-*`、`hook-*`、`tmp*`、`wb-repro-*` 永远不动）。`--keep-tmp` 保留、`--no-sweep` 不清。单独跑某个测试文件仍会在 /tmp 留目录，下一次 run_all 会清。

## 2026-11-06 批处理、可打断回复、失败文本补发（待发布，版本号仍 5.0.5，详见 CHANGELOG.md）

**1 批处理（`state/pending.json`）**
- 发给 Muse 的消息（文字、图片/文件/视频/语音；不含快捷指令、设置指令、桥接自己回的）先进对方的“当前批”，不立即派发。对方停顿 3 秒（3 秒内没有新消息），或从这一批第一条起满 60 秒，二者先到为准，整批一次派发：这一批同时变成可领取，hook 只唤醒一次，一次 claim 全部领走（带一个 merge_hint），执行者只跑一轮。单条消息 3 秒后派发，不会拖到 60 秒。
- 合并只有这一种口径：v5 的 claim 合并窗口（整句 0.3 秒、碎片 1.2 秒、上限 4 秒，`WECHAT_BRIDGE_SETTLE_SHORT_MS`、`claim --settle-ms`）已删除，`--settle-ms` 仍接受但不起作用。merge_hint 的意思就是“这次 claim 是同一批”；一次 claim 只返回一批，`--limit` 不会把一批拆开。
- 消息照旧在游标前先写 `inbox/<id>.json`（状态仍是 pending_execution / downloading），另加 `batch_open: true`，hook 和 claim 都跳过这类任务。入批、派发、打断都在同一把 task_lock 下：派发那一刻到达的消息要么在这一批里，要么开下一批，不会丢、不会两批都有。进程在两次写之间崩溃、或 pending.json 丢了，`batch_recover()`（serve 每 5 秒一次）把还标着 batch_open 的任务直接派发。
- 每批记下开批时的停顿/上限（`quiet_ms` / `max_ms`）：改 tuning 只影响之后新开的批。
- 谁来派发：serve 的 batcher 线程按时派发；发送线程每秒、每次 claim 也会顺手派发到期的批（不依赖单一线程）。派发时 bump `.inbox_signal`（停留中的 hook 立即醒），到达时不再 bump：所以停留触发的 rerun 仍然都会唤醒，`reruns = linger_wakes` 的核对照旧成立。
- claim 碰到还没派发的批会等它派发（最多到这一批的上限），等待期间写 `claim_waiter.txt`，hook 不会再叫第二个执行者。
- 一批就是一个执行单元：progress / heartbeat 给同一 claim 的所有任务一起续心跳、一起带长步骤租约（否则一批里没被心跳的几条会在 10 分钟时被收回重跑，重复回复）；自动进度每批只发一条。
- 看现状：`python3 wb status` 的 `batch`（开着的批、几条、多久后派发）；`cat state/pending.json`；日志 `grep "dispatched:" logs/bridge.log`（`reason=quiet` 是停顿到了，`reason=max` 是 60 秒上限）。`stats` 的 `latency_ms.wake_queue` 现在含 3 秒合批（pending_at 记的是派发时刻）。

**2 可打断回复（退出码 4 = SUPERSEDED）**
- 执行者处理中（任务已派发、还没把回复交给桥：pending_execution / downloading / executing）对方又发来一条发给 Muse 的消息：旧回复直接作废，这几条被拉回新消息所在的批，和新消息一起重新派发，下一轮一次回答。旧执行者之后的 complete / progress / heartbeat / send-media / send-image|file|video（`--task-id`）一律退出码 4、输出 `{"ok": false, "superseded": true, "exit_code": 4}`，什么都不发（文字、进度通知、附件都不发）。提示词和 EXECUTOR.md 让执行者看到它立即停止。
- 不打断：已经交给桥的回复（waiting_send 及之后，可能已在路上）、快捷/设置指令（「进度」「状态」等）、主动消息、带长步骤租约（`progress --hold-minutes`）的任务。
- 被打断过的任务只接受带当前 claim_id 的调用：不带 `--claim-id` 一律退出码 4（桥分不清新旧执行者，宁可不发也不能把作废的回复发出去）。
- 已被领走的任务被打断时换一个 wake_key（`<id>~<n>`），hook 的唤醒 id 随之变化，重派的批会立即叫新执行者，不会被“留给还在启动的执行者”的 45 秒规则卡住；还没人领的任务 key 不变，已经被叫醒的那个执行者直接领新批。
- 退出码一览：0 成功；1 发送没成功（send / send-media / send-*）；2 参数或状态不对；3 停止（「取消」、stale claim，以及 send-direct 窗口关闭等命令自己的拒绝）；4 SUPERSEDED。

**3 失败文本补发（`state/outbox.json`）**
- 发不出去、而且确定没送达的回复文字进 `state/outbox.json`：parked（条数用完 / 24 小时窗口 / 限流 / -14 暂停）、retrying（失败了，等下次重试）、exhausted（重试用完）。送达、取消、被替换，或变成“可能已送达”（发送中超时：照旧绝不自动重发），或超过 72 小时（与「重发」同一范围）就移出。
- 对方下一条消息（任何消息）一到，他名下的条目立即到期，发送线程马上补发（要求 3 秒内；隔离测试里 0.05 秒，线上未测），排在新消息的回复之前。条数限制挂起的回复照旧每条消息续发；失败 / 重试用完的每条文本最多被消息触发补发 3 次，每次只试一次、沿用原来的计数和退避，用完再次变成 exhausted；“没送达”的提示不重复发，hook 也不会因它再叫执行者。「重发」照旧从头开始一轮。
- 和媒体出站队列严格分开：文件/图片走 `outbox/queue/`（v4，不变）；`state/outbox.json` 只放文字，两边代码互不读写（`tests/test_batch_20261106.py` 检查）。注意旧函数 `flush_outbox()` 是“发送所有到期的回复”，名字是历史遗留，与这两者都无关，没有改名（测试和 `flush` 命令在用）。
- 看现状：`python3 wb status` 的 `text_outbox`；`cat state/outbox.json`（含待补发的原文）；日志 `grep "text outbox" logs/bridge.log`。

**备份**：`state/pending.json` 和 `state/outbox.json` 不进每小时快照（`ops/backup.sh`）：都是“此刻在途”的状态，桥能从 inbox/ 任务文件恢复（inbox/ 本来也不备份）；而且 outbox.json 里有回复原文，快照是永久保留的。

**回滚**：换回旧的 bridge.py 和 hook 脚本即可，不用清 state：旧代码不认识 `batch_open`，标着它的任务照常派发；`pending.json`、`outbox.json` 旧代码不读，留着无害（要删也行）。提示词按 CHANGES 用 hooks 工具整段设回。
