# 执行者手册分册：附件直发

本文件是 EXECUTOR.md 的分册，2026-11-02 由 EXECUTOR.md 原文拆出，内容未改。只在遇到对应任务时读本文件，平时只读 EXECUTOR.md。

## 生成物直发：附件（v4）

- 要发给用户的文件、图片、视频、音频，随回复一起交：
  `python3 bridge.py complete --task-id <id> --result "<说明>" --attach <文件> [--attach <文件2>]`
  文字先发，附件随后。桥接把文件复制进自己的队列（/tmp 里的文件扛不过 VM 替换），网络断了会在后台自动重试约 18 分钟，每轮都重新申请上传地址；最终失败时桥接直接告诉用户原因，用户回复「重发」就能再试。所以：
  - 文字里别写“已发送”，写“文件随后发来”之类；
  - 附件失败不要另发消息，也不要再 send-file，桥接会处理；
  - 先确认文件真实存在、内容正确，不要发空文件或占位文件。文件不存在或超限时 complete 直接报错且什么都不发，修正后重新 complete。
- 语音条：加 `--attach-as voice`，音频不超过 60 秒。需要 SILK 编码库，装一次即可，VM 替换后仍在：`pip install --target ~/workspace/wechat-bridge/vendor/py silk-python`。没装、超过 60 秒或服务端不收语音条时，会自动改成音频文件发出，complete 输出里 attachments[].fallback / note 会写明，这时文字里说“以音频文件形式发给你”。
- 大小上限：图片 10 MB（超过自动按文件发）、视频 50 MB、文件 25 MB。音频尽量用较低码率，文件越小越不容易被这条线路的断连打断。
- 任务进行中想先发一个文件：`python3 bridge.py send-media --task-id <id> --file <文件> [--as voice]`，默认等 60 秒并报告结果，没送达的转入后台继续重试。旧的 send-image / send-file / send-video 仍可用，失败时同样转入后台队列（输出 `queued: true`）。

