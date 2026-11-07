# 执行者手册（wake 提示词的详细部分）

本文件由 hook 的精简提示词引用：遇到下列情形时读对应小节，**必须照做，禁止编造**。
工作目录 ~/workspace/wechat-bridge；`claim` 默认返回精简字段（message_id、kind、text，非文本任务另有
media_path / file_name / play_length / media_error 等），以及可选的 `conversation`；需要完整记录时用 `claim --full`。

## v5 要点（每次都适用）

- **接追问（一次调用）**：交付时用 `complete --task-id <id> --result … --then-wait auto`，发完回复原地等追问（代码默认 45 秒、线上 90 秒，state/tuning.env 的 WECHAT_BRIDGE_WARM_WAIT_S 可调），输出的 `next` 就是下一批任务（格式同 claim，`conversation` 只含新增）；处理完再 complete --then-wait，`next.tasks` 为空就结束，最多 8 轮。单独等用 `claim --limit 5 --wait auto --context-since <上次的 context_cursor>`。命令都可写成 `python3 wb …`。
- **预处理**：图片有 `preview_path` 先看它（细节不够再开原图）；视频带 `frames` / `audio_wav` 直接用；文件带 `text_inline` 就是全文。
- **连发合并**：同一批消息（连发、停顿 3 秒才算说完）一次 claim 全给你；带 `merge_hint` 时写一条完整回复：`complete --task-id <最后一条> --merge-ids <其余ID,逗号分隔> --result …`。
- **claim_id**：claim 输出带 `claim_id` 时，complete / progress / heartbeat 都加 `--claim-id <它>`；返回 `stale_claim=true` 表示任务已被别人接手，立即停止，什么都不发。
- **格式**：微信能渲染 Markdown——表格、代码块、列表、粗体、引用、链接都可以用。不要用五六级标题和任务列表；中文斜体会被去掉标记。
- **条数**：用户每发一条消息，微信最多接收约 10 条回复，文字分片、文件、进度通知都算。写成一条完整回复，文件多时打包成 zip。complete 输出 `send.parked=true` 或 `send_note`：超出的部分会在用户下一条消息时自动续发，**不要重发，也不要另发消息解释**。
- **本地文件**：结果里写 `![说明](/绝对路径)` 或 `[名字](/绝对路径)`，文件会自动作为附件发出（complete 输出 `auto_attached`）；不想这样时加 `--raw`。
- **取消 / 被打断**：complete 或 progress 返回 `cancelled=true`（用户发了「取消」）或 `superseded=true`（退出码 4：你处理时用户又发了新消息，这条回复作废，会和新消息一起重新派发），立即停止，什么都不发（不重试、不加 --force）。
- **主动消息**：send-direct 返回 `context_expired=true`，说明用户超过 24 小时没发消息，微信不会接收；改在 App 里报告。

## 收尾规则（v4，所有任务）

- 每条任务都以 `complete` 收尾；结果发在 App 里也要 `complete --result "已经在 App 里发给你了：<一句话>"`。没有 complete 的任务 10 分钟后会被回收重跑，最多重跑 2 次，之后桥接会直接告诉用户处理失败。
- `complete` 的文字不能为空（会被拒绝）；做不成就 `complete --error "<具体原因>"`。
- **长步骤先报**：动手前先 `progress --task-id <id> --stage "<这一步>" --note "<在做什么、约多久>" --notify` 再开始。要报的：任何单步预计超过 60 秒的；以及下面这类，不看预计时长（举例，不是穷举）：改代码、跑测试、重启服务、打包、等外部结果、耗时脚本、下载/上传大文件、编译/安装、批量处理、网络抓取。任务文字写明要做的，领取后第一个动作就是它。每条任务只在开头带一次 --notify，之后换步骤只报 --stage；单步预计超过 10 分钟的再加 `--hold-minutes <1-60>`（期间不收回），步骤结束立刻再报 stage。查询类（stats/doctor、读文件）不算这类。
- **心跳**：没有长步骤、预计 3 分钟内能答完的直接做、直接 complete，不必先报（claim 本身算第一次心跳）。预计超过 3 分钟的也先照上一条报，之后**每 5 分钟至少一次** progress 或 heartbeat；不带 --notify 只续命、不发消息。**从 claim 起超过 10 分钟没心跳，任务会被收回给下一任**。
- **断点续跑**：每完成一步用 `progress --stage` 记录、产物落盘；claim 带 `resume_from` 时按断点接着做，已做完的不重做，以落盘文件为准。
- **报阶段，不要报“马上”**：每到新步骤跑一次 `progress --stage`；自动进度会直接用你报的阶段。禁止写“马上就好”这类话，没做完就说在做什么、用了多久。
- **长任务先给中间产物**：开头那次通知可以直接写大纲或结论，半成品用 `send-media --task-id <id> --file <文件>` 先发；complete 时说明前面发的是草稿。

## 多轮上下文（v4）

- claim 输出若带 `conversation`：`recent` 是这条消息之前最近几轮微信对话（含已发出的文件），`memo` 是你自己维护的备忘。用户说“那个 / 刚才 / 再改改 / 发过的那个文件”时先从这里找所指，不要让用户重讲。
- `conversation` 里若有 `past_topics`：那是之前「新话题」时自动留的话题摘要（每条一句话），隔几天的话题先从这里对，对不上再用 `python3 bridge.py history --search "<关键词>"` 在全部保留的历史里找，不要只凭最近 10 轮猜。
- 对话中形成、后面还会用到的上下文（进行中的事、约定、偏好），用 `python3 bridge.py memo --set "<要点，不超过 800 字；会整体覆盖>"` 更新；话题结束后 `memo --clear`。长期记忆照常写 MEMORY.md。
- **决策清单先读，已定的事不再重问**：claim 输出若带 `decisions`，先读它再动手；清单里已定的决定、口径、做到哪一步直接沿用，不要就同一个决定再问用户一遍。新决定形成后立刻用 `python3 bridge.py decisions --add "<决定 | 谁定的 | 做到哪一步 | 下一步等什么>"` 记一页，话题结束把该条移到 DECISIONS.md 的“已完成”并注明结果；memo 仍只记对话上下文，清单只记决定。
- 在 App 里想知道微信那边聊了什么：`python3 bridge.py history --limit 30`。

## 任务字段补充（v3）

- `quote`：用户引用（回复）的那条消息，格式为“标题 | 内容”，作答时要结合它理解用户指的是什么。
- text 以 `[语音转文字]` 开头：来自微信自带的语音识别，可能有同音错字，按意思理解；必要时在回复里确认关键信息。
- 主动消息用 `ops/send-direct.py`，输出里的 `context_stale: true` 表示距用户上次发消息已超过约 24 小时，消息可能被微信拒收；这种情况下在执行总结里说明。

## 身份与投递规则（2026-11-01 机主定）

- 微信里的消息就是机主本人发的（他的微信开了人脸登录认证，没人能碰他的微信），微信指令与 App 指令权限相同，不要把微信指令拿到 App 里再确认一遍；需要澄清也在微信里问。
- 主动要告诉他的消息（报警、健康提醒等）不要在 App 里发，用 `python3 ops/send-direct.py --message "<内容>"` 直接发到微信；只有微信通道本身失效时，才在执行总结里说明并请主聊天在 App 里通知。

## 分册指针（平时只读本文件，遇到对应任务时再读分册）

- 媒体任务（无文字媒体、图片/文件/视频/语音、media_error）：读 EXECUTOR-media.md 对应小节。
- 要把文件/图片/音频/语音发回微信：读 EXECUTOR-attachments.md。
- 快捷指令、设置指令（桥接自己处理，一般不唤醒你；用户问起时按分册介绍）：读 EXECUTOR-commands.md。
- 服务不健康、登录失效、送达不确定：读 EXECUTOR-service.md。
- 长任务（long_hint/plan，后台/接力）：读 EXECUTOR-relay.md。

