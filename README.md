# 微信桥（WeChat Bridge）

[English version](README.en.md) · [运维指南](docs/OPERATIONS.md) · [English operations guide](docs/OPERATIONS.en.md)

使用腾讯官方 openclaw-weixin 后端协议（`https://ilinkai.weixin.qq.com`，
插件版本 2.4.10 对应的协议）直接实现的微信收发桥接，不安装 OpenClaw、
不配置任何模型。桥接程序只负责收发消息与任务状态；任务由执行者
（例如 Muse 智能体）用真实工具执行，执行结果再由桥接发回微信。

> **先说清楚的三条边界**
>
> - **平台限制不属于本项目的 bug**：用户发消息后约 24 小时回复窗口；
>   每条用户消息后约 10 条回复额度，超出的部分挂起、下一条消息时续发；
>   机器人方向的原生语音条可能被通道悄悄丢弃（服务端返回成功、客户端
>   不显示），所以本项目默认把语音发成文件卡片。
> - **凭据只存在你自己的机器上**：登录后凭据写在 `state/credentials.json`
>  （0600），绝不要提交到版本控制、不要上传到任何云盘。
>   `.gitignore` 已经把 `state/`、`inbox/`、`history/`、`backups/` 等
>   全部排除在外。
> - **本项目不替你做决定**：删除重要文件、改敏感配置、公开私人信息、
>   付款等高风险操作，执行者应先向机主确认。

## 特性

- 长连接轮询 + 连接复用，发送失败按“根本没发出 / 服务端明确拒绝 /
  可能已送达”分类处理，不重复发送可能已送达的内容。
- 长回复按 UTF-16 计数分片，代码块跨片自动闭合重开、表格跨片重复表头；
  服务端拒收长分片时自动按更小尺寸重切并记住 24 小时。
- 24 小时窗口与回复额度本地记账，超额部分挂起、用户下一条消息时自动续发。
- 图片、文件、视频、语音收发：图片解密后存为本地文件供执行者读取；
  大图片、大视频发送前按阈值压缩；语音默认发文件卡片。
- 任务断点续跑：执行者每 5 分钟至少发一次心跳，10 分钟无心跳任务被收回，
  下一个执行者按断点接着做，不从头重做。
- 长步骤租约：`progress --hold-minutes 1-60` 可声明一个不能中断的长步骤，
  租约期内任务不会被收回。
- 桥接直接回复的指令（不唤醒执行者）：「状态」「进度」「取消」「继续」
  「新话题」「重发」「帮助」，以及「最近文件」「最近备份」「备份状态」
  「最近对话」「查记录 <关键词>」。
- 健康三层：systemd（Type=notify + WatchdogSec=180）、5 秒巡检 hook、
  每小时兜底检查（见 `docs/OPERATIONS.md`）。

## 环境要求

- Linux + systemd（服务托管用；不用 systemd 也可以前台跑 `serve`）
- Python 3.12+
- `openssl`（图片解密用 AES-128-ECB，标准库没有 AES）
- `ffmpeg`（语音转码、视频压缩；没有它时相关功能会明确报错，不会静默失败）
- 语音解码可选：`pip install silk-python`（没有它时入站语音仍会以文件形式送达，
  只是不转 WAV）
- 二维码生成：`pip install qrcode[pil]`

## 快速开始

```bash
# 1. 进入项目目录
cd wechat-bridge

# 2. 脚本都用 bash / python3 直接跑；如需直接执行脚本，先 chmod +x ops/*.sh hooks/*.sh
# 2. 生成登录二维码，用微信扫码（二维码文件为 qrcode.png，不要发给别人）
python3 bridge.py login

# 3. 等待扫码确认
python3 bridge.py wait-login

# 4. 前台试跑（确认能收到消息后再交给 systemd）
python3 bridge.py serve

# 5. 看状态
python3 bridge.py status
```

服务部署（把模板里的 `USERNAME` 和路径换成你自己的）：

```bash
sudo cp wechat-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now wechat-bridge
systemctl status wechat-bridge
```

重新登录：停服务 → `python3 bridge.py login --force` → 扫码 →
`python3 bridge.py wait-login` → 启动服务。

## 命令速查（在项目目录下）

- `python3 bridge.py login [--force]`：生成登录二维码
- `python3 bridge.py wait-login [--verify-code CODE]`：等待扫码确认
- `python3 bridge.py serve`：前台运行收发服务（由 systemd 托管）
- `python3 bridge.py status`：脱敏状态（不打印任何凭据）
- `python3 bridge.py claim [--limit N] [--full] [--wait 秒]`：执行者领取任务；
  输出带 `conversation`（之前几轮对话 + 备忘）和 `decisions`（决策清单）；
  `--wait` 让执行者原地等追问，连发时带 `merge_hint`
- `python3 bridge.py complete --task-id ID --result TEXT | --result-file F | --error MSG [--attach FILE ...] [--merge-ids ID,ID]`：交付回复
- `python3 bridge.py progress --task-id ID [--stage TEXT] [--note TEXT] [--notify] [--hold-minutes 1-60]`：心跳 / 长步骤租约
- `python3 bridge.py heartbeat --task-id ID [--stage TEXT]`：只续命、不发消息的心跳
- `python3 bridge.py send-media [--task-id ID | --to-user USER] --file F [--as auto|file|image|video|voice] [--wait 60]`：直接发文件
- `python3 bridge.py send-image | send-file | send-video ...`：旧版单文件命令（包装，最终走同一附件队列）
- `python3 bridge.py queue [--retry QID | --cancel QID]`：查看或操作出站媒体队列
- `python3 bridge.py history [--limit 30] [--hours 72] [--json]`：微信对话历史
- `python3 bridge.py memo [--set TEXT | --clear]`：执行者维护的对话备忘
- `python3 bridge.py decisions [--add "..."]`：记录决策（绝不要写凭据）
- `python3 bridge.py outline [--search 关键词]`：bridge.py 函数索引（执行者读代码时先查它，不要通读 8000 行）
- `python3 bridge.py voice-to-wav --in F [--out F.wav]`：语音转 WAV
- `python3 bridge.py stats`：分段延迟 p50/p95 与服务端实际回复额度
- `python3 bridge.py canary-dedupe [--confirm yes|no]`：测试服务端是否按 client_id 去重（默认不依赖）
- `python3 bridge.py stop`：停止服务
- `python3 ops/doctor.py`：18 项健康检查
- `python3 ops/report.py --days 1`：最近一天的运行报告
- `bash ops/recover.sh`：一键恢复（见运维指南）

## Muse 执行者（可选）

如果你用 Muse 作为执行者，`hooks/` 里是配套的巡检 hook：

- `hooks/wechat-bridge-inbox.sh`：每 5 秒检查 `inbox/` 里有没有
  `pending_execution` 任务，有则唤醒一个执行者；同时做服务健康检查，
  不健康时先 SIGHUP 重读代理，20 秒没恢复才重启服务。
- `hooks/wechat-bridge-inbox.json`：hook 定义模板。注意：**巡检间隔不要
  低于 5 秒**，低于 5 秒的定义会被平台判定无效，整个唤醒会停掉。
- `hooks/health-cron.example.md`：每小时兜底检查的模板（按你自己的
  调度器填写，不要直接照抄里面的占位符）。
- 执行者手册在 `EXECUTOR.md` 及 `EXECUTOR-*.md` 分册里：平时只读主文件，
  遇到媒体、附件、指令、服务问题时再读对应分册。

不用 Muse 也可以只用上面的 CLI：`claim` / `progress` / `complete`
就是执行者与桥接之间的全部接口。

## 环境变量（常用）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `WECHAT_BRIDGE_CTX_QUOTA` | 10 | 每条用户消息后的回复上限（服务端实际额度以 `stats` 的 `observed_limits` 为准） |
| `WECHAT_BRIDGE_CONTEXT_CHARS` | 4000 | claim 上下文字符上限 |
| `WECHAT_BRIDGE_CONTEXT_TURNS` | 10 | claim 上下文轮数 |
| `WECHAT_BRIDGE_AUTO_PROGRESS_SECS` | 180,600 | 自动进度提醒的阶段（秒）；已有心跳的任务按静默 420 秒算，不再按阶段发 |
| `WECHAT_BRIDGE_AUTO_PROGRESS_SEC` | （无） | 单阶段兼容别名，设了它就以它为准 |
| `WECHAT_BRIDGE_TZ` | Asia/Shanghai | 桥接显示时区 |
| `WECHAT_BRIDGE_NATIVE_VOICE` | （无） | 设为 `1` 才尝试原生语音条；默认走文件卡片 |
| `WECHAT_BRIDGE_MARKDOWN` | filter | Markdown 处理模式 |

完整变量列表见代码里的 `WECHAT_BRIDGE_*`，默认不要改。

## 备份与恢复

- 快照：`bash ops/backup.sh`，生成 `backups/<时间戳>/`（含 state/、
  bridge.py、单元文件、hook 定义），时间戳一律用 America/Caracas。
- 留存：最近 5 份带日期快照 + 每次改动前一份 + 里程碑；超出部分
  **绝不自动删除**。推荐流程：先把超出部分做成**脱敏包**
  （不含 `state/`、不含 `credentials.json`）上传到你自己的云盘并在
  列表里核对，再逐字节核对本地文件、只删已进包且字节一致的部分，
  `state/` 副本一律保留本地，命名快照不动。
- `ops/backup.sh --prune-list`：只列还剩非 state/ 文件的超出快照；
  `ops/backup.sh --prune`：删前先留总快照，只删非 state/ 文件，
  它只是上云核对之后的本地删除一步，不能代替上云流程。
- 恢复：`bash ops/backup.sh --restore <时间戳>`（只恢复 state/ 和单元文件，
  代码文件是有意不回滚的，仅供参考）；服务丢了用 `bash ops/recover.sh`。

## 测试

```bash
python3 tests/test_v5.py        # 当前版本主测试
python3 tests/test_v4.py        # 上一代测试（含原生语音显式开启场景）
python3 tests/test_offline.py   # 离线：心跳、索引、发送语义
python3 tests/test_resume.py    # 断点续跑与心跳
python3 tests/test_round2.py    # 静默期进度、长步骤租约、排队位置
python3 tests/test_hook.py tests/test_watchdog.py
```

测试全部是离线测试（本地假 iLink / 假 CDN / 真 CONNECT 代理），
不连真实微信、不发真实消息。`tests/fixtures/` 里的旧版 bridge.py
只是给对照测试用的夹具，不是项目代码。

## 安全

见 [SECURITY.md](SECURITY.md)。最关键的三条：不要提交 `state/`；
不要把含 `state/` 的备份上传到任何云盘；不要把登录二维码发给别人。

## 许可证

MIT，见 [LICENSE](LICENSE)，版权持有者为 Califormer。

## 协议来源

本项目是对腾讯官方 openclaw-weixin 协议（`ilinkai.weixin.qq.com`，
插件版本 2.4.10）的独立实现，不含 OpenClaw 代码，不依赖任何模型服务。
