---
id: wechat-bridge-health
title: 微信桥接每小时健康兜底检查
enabled: true
owner: goal:clawbot
mode: task
schedule:
  kind: interval
  timezone: Asia/Shanghai
  at: 2026-10-27T23:01:00
  every: 1h
timeout_secs: 900
delivery:
  - chat_id: ef30a421-e095-4a6c-9780-57323885f4b7
metadata:
  tags: [cron:automatic-interval-anchor]
  originating_chat_context_json: '{"chat_id":"ef30a421-e095-4a6c-9780-57323885f4b7","origin_provider":"main","chat_kind":"direct","event_kind":"message","require_mention":false,"device_id":"88450a37-d522-44c2-871c-90c38c80bb96"}'
  presentation_locale: zh-CN
---
这是微信 ClawBot 桥接的每小时兜底检查（主防线是 hook 的 5 秒巡检，这里只兜底 hook 本身失效的情况）。
在 ~/workspace/wechat-bridge 下运行 `python3 ops/watchdog.py`（它自己完成健康判断、必要的自动恢复和备份补做，最长约 1 分钟）。
- 没有任何输出：一切正常，直接结束。不要发送任何消息，也不要运行其他命令。
- 有输出（一行 JSON）：按 alert 字段，用 `python3 ops/send-direct.py --message "<内容>"` 直接发到微信，不要在 App 里发。recovered：“微信桥接曾掉线、已自动恢复”；recovery_failed：报告 detail 里的具体原因（可再运行 `ops/repair.sh`）；auth 或 credentials_missing：报告微信登录可能已失效、需要重新扫码，不要自行生成二维码。
- 只有 send-direct 本身失败（微信通道也失效）时，才在 App 里报告；v5 起 send-direct 返回 context_expired=true（用户超过 24 小时没说话，微信不收）也属于这种情况。
（注：本文件为旧版参考，当前实际定时任务以系统里 wechat-bridge-health 的定义为准；2026-11-01 起主动消息改走微信，见 EXECUTOR.md 的身份与投递规则。）
