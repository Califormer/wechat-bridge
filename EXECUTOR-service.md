# 执行者手册分册：服务异常

本文件是 EXECUTOR.md 的分册，2026-11-02 由 EXECUTOR.md 原文拆出，内容未改。只在遇到对应任务时读本文件，平时只读 EXECUTOR.md。

## wake reason：wechat_service_unhealthy / wechat_auth_alert / wechat_send_uncertain

2) reason 为 wechat_service_unhealthy：注意，hook 脚本自身已先做过内联自愈（刷新代理 env、必要时重装 systemd 单元、重启服务，每 30 秒最多一次）；你被唤醒说明内联自愈持续 90 秒以上仍未恢复，需要你深入处理：先运行 `cd ~/workspace/wechat-bridge && bash ops/recover.sh`（一键恢复：依赖检查、刷新代理环境、修单元、按需重启、等待首轮轮询成功、doctor 摘要；本运行时 systemd 服务靠 state/proxy.env 里的代理变量出网，值不得打印）。它失败时再看 `python3 bridge.py status`、`systemctl status wechat-bridge --no-pager`、logs/bridge.log 尾部和 `python3 ops/report.py`。恢复情况用 send-direct 发到微信；若恢复失败，在微信里说明具体原因。

3) reason 为 wechat_auth_alert：桥接连续两次收到会话失效错误（-14），暂停自愈未成功，登录凭据很可能已经失效。不要自行登录或生成二维码。用 send-direct 发到微信明确告诉用户：微信登录可能已失效，需要走重新扫码流程（停服务→生成新二维码→用户扫码→重启服务），请他说 ok 以便生成新二维码。

4) reason 为 wechat_send_uncertain：v4 起，回复或文件送达不确定、或重试耗尽时，桥接会直接在微信里告诉用户并提示回复「重发」，这些情况不会再唤醒你。你被唤醒只说明桥接的提醒本身没能发出（多半整个微信通道不通），或提醒功能被关闭。读取 payload 中 task_ids 对应的 inbox 任务文件，看其 send 字段：(a) 有 `not_delivered: true`：确定未送达；(b) 没有：发送途中连接中断，结果不明。先试 send-direct 把情况告诉用户（ID、原消息简述、回复内容简述），send-direct 也失败时在执行总结里说明并请主聊天在 App 里通知。两种情况都不要自行补发；用户确认后执行 `python3 bridge.py send --task-id <id>`：它从第一段未确认送达的分片续发，并重新计重试次数。

