# 执行者手册分册：长任务——交后台或接力（2026-11-07）

claim 里带 `long_hint`（桥按规则判的长任务）或 `plan`（接力中的下一棒）时读本文件。为什么这样做：一次唤醒的回合到 600 秒会被平台掐掉（桥改不了），桥 10 分钟没心跳也会收回；长任务以前就是撞墙、从一句话断点重来、三次后放弃。现在按“计划 + 每步 ≤7 分钟 + 交棒”做：用户一次发完，执行者自动接力，最后只发一条完整结果。

## 0. 交给后台（v1.4，默认）

后台值守在线时（`wechat-long-task-watch` hook 在跑），桥派发长任务时就自己交给后台了：给用户回一条信号、先写队列再把任务移出前台，你根本收不到。你收到的长任务（媒体还在下载、批处理关着），long_hint 会写“交给后台做”：
- **第一个动作**：`python3 wb complete --task-id <id> --claim-id <claim_id> --background`。桥回信号、写队列、把同一批的其他消息并进来；然后**立即结束本回合**：不加 `--then-wait`，不再 claim / progress / complete 这条（旧 claim 会被拒）。不要在前台做，也不要再发别的消息。
- 后台由主代理（App 主聊天）生成子代理去做，做完由主代理用 `long-tasks finish` 发回唯一一条完整结果（BACKGROUND.md）。
- 没带 long_hint、做着发现要超过 7 分钟的：同样可以 `complete --background`；后台不可用时会被拒（退出码 2），那就按下面的接力做。
- long_hint 写的是接力（`complete --continue`）：后台没在值守，或这条不能进队列（例如提到凭据），按 §1–§5 接力做。
- claim 带 `followups`（v1.7）：这条长任务在后台排队时没人接、退回了前台，`followups` 是用户期间引用它补充的要求，一起照做、最终结果里一并回应；这样的任务 `--background` 会被拒，按接力做。

## 1. 第一棒：先出计划（领取后第一个动作）

```bash
python3 wb progress --task-id <id> --claim-id <claim_id> --plan <计划.json> --notify
```

- 计划可以是文件、`-`（stdin）或直接写 JSON。先写粗的（每步一句话 + 预估分钟），十几秒内发出；之后可以再 `--plan` 细化，已完成的步骤保留。
- 桥校验后存成 `state/plans/<id>.json`（0600，不进备份、不进开源包），并按计划给用户发大纲、步数和预估总时长，不用再写 `--note`。这就是“每条任务开头只带一次 --notify”的那一次。
- 格式：`{"goal": "…", "steps": [{"step_id": "s1", "do": "做什么", "inputs": ["…"], "outputs": ["产物路径"], "est_min": 4, "done_when": "完成标志"}, …]}`
- 每步 `est_min` ≤7（超了会被拒：拆小，例如逐个扫文件就 6–8 个一步）；最多 20 步；计划里不写凭据、token、密码（会被拒）。
- 中间产物放 `plan.workdir`（`state/plans/<id>.d/`）。一步里要处理很多项的，每做完一项就追加写进这一步的产物文件：这棒断了，下一棒读它、跳过已做的项，不重扫。

## 2. 每一步

- 一次只做一步。做完：`python3 wb progress --task-id <id> --claim-id <claim_id> --step-done <step_id> --artifact <产物>`（`--artifact` 可重复；不带 `--step-done` 的 `--artifact` 记到正在做的那一步，算阶段产物）。
- 输出里的 `plan.advice`、`plan.next_fits`、`plan.leg_left_min` 告诉你接着做下一步，还是该交棒了。桥把“第 k/N 步做完：…”写成 stage，「进度」和自动进度都用它。

## 3. 交棒（complete --continue）

- 从 claim 起约 7 分钟（`plan.hand_off_by`），或下一步放不进剩余时间：
  `python3 wb complete --task-id <id> --claim-id <claim_id> --continue [--step-done <刚做完的>] [--stage "<断点>"]`
  然后**立即结束本回合**：不加 `--then-wait`，不再 claim / progress / complete 这条（旧棒的 claim 会被拒，退出码 3）。
- 桥把剩下的步骤作为同一条任务重新派发：不需要用户发消息，换 wake_key，hook 下一轮就唤醒新执行者。下一棒的 claim 带 `plan`：`done`（已完成 + 产物）、`next`（从这一步接着做；`next.partial` 是上一棒做了一半留下的产物）、`todo`、`hand_off_by`，以及 `resume_from`。
- 热接到长任务（`complete --then-wait` 的 `next` 里带 long_hint，提示写着“热接”）：只出计划、发通知，然后马上 `--continue`，让新一棒用完整的回合做。同一批的其他消息在交棒时并入这条链，最后一条结果一起答。

## 4. 收尾

- `todo` 都做完：`python3 wb complete --task-id <id> --claim-id <claim_id> --result-file <完整结果>`（可加 `--then-wait auto`）。整条任务只有这一条最终回复；中间不要用 complete 发阶段结果，进度由桥播报（交棒时最多每 10 分钟一条，并保留最终回复要用的条数）。

## 5. 上限、失败、打断

- 默认最多 8 棒、共 60 分钟；连续两棒没做完一步，或同一步连续 3 次做到一半断掉，桥停下并告诉用户做到哪一步、还剩什么、回「继续」接着做。用户回「继续」时 claim 带 `resume_plan`：`progress --task-id <新id> --claim-id <claim_id> --plan-from <旧任务ID> --notify` 接过计划，已做完的不重做。
- 这一棒被平台掐断（没来得及交棒）：桥 10 分钟后照旧收回，下一棒的 claim 带 plan 和 `resume_from`，从 next 接着做；做完过一步的棒不算“试了 3 次”。
- 用户发「取消」或新消息：照原规则停（cancelled / 退出码 4）；计划和已完成的步骤保留，下一棒接着用，新消息在同一批里最后一起答。不想被打断的长步骤照旧可以加 `--hold-minutes`。
- 开关：`state/tuning.env` 里 `WECHAT_BRIDGE_RELAY=0` 关掉整个接力（不再有 long_hint / plan，`--plan` / `--continue` 被拒，回到 v5.0.5 的行为）。
