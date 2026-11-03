# 更新日志

## v5.0.4（开源包修正，2026-11-03；尚未发布、尚未回灌线上桥）

- 重定向守卫补测试：此前没有任何测试碰 `_SafeRedirectHandler`（
  proxy_failover 只确认回退路径走了 build_opener，删掉守卫也不会
  红）。新增 tests/test_redirect_guard.py，用本地 A→B 服务器实测
  池模式与回退模式：跨域白名单目标剥 Authorization/Cookie、跨域
  非白名单拒绝、同域保留凭据、https→http 降级拒绝；已用阉割版
  反向验证新测试会红。
- 跨域重定向被拒的异常统一：回退路径此前把白名单拒绝包成
  URLError，下载循环便当成网络错误重试 6 次再报网络中断；现与
  池路径一致，改为 MediaError（确定性失败，不重试，原始错误交
  执行者）。降级拒绝在两模式本来就是 URLError，保持不变。
- state/DECISIONS.md 改 0600 原子写：此前 `decisions --add` 直接
  write_text（本机实测 0660，与 state 下其他 0600 文件不一致），
  现新增 atomic_write_text（临时文件 + chmod + rename），中断不
  会留半截文件。
- 坏密文改 4097 字节：原固定全零 4096 块的注释把密文字节和填充
  字节混为一谈；固定密钥下才确定报错，随机密钥实测约 0.5% 不
  报错。4097 不是 16 的倍数，长度本身即错，与密钥无关。
- CI 矩阵防塌缩：两任务各加后端断言（job1 要求 cryptography
  不可导入且后端为 ctypes；job2 要求后端为 cryptography），
  test_open_fixes 循环已装的全部后端（cryptography/ctypes/openssl
  CLI）跑向量与坏长度文件。
- CHANGELOG 整理：v5.0.3 的 CI 过程记录（运行号、提交号、上传
  插曲）压成一行。

## v5.0.3（开源包修正，2026-11-03；同日已发布并回灌线上桥）

- 更正 v5.0.2 的说法：那版的旧决定回退只接进了 claim，`decisions` 显示
  仍读 state 一处，第一次 `--add` 会只用新条目新建 state 文件，此后
  claim 见到 state 就不再读旧文件，旧决定等于从执行者上下文里消失
  （文件还在磁盘上）。本版抽出 `_decisions_read()`，claim、显示、
  `--add` 三处共用，`--add` 首次建文件时用旧文本做底再追加；
  新增 tests/test_decisions_migrate_20261103.py。线上桥因回灌时已把
  旧文件原样复制到 state/，不受此问题影响。
- urllib 回退路径的重定向防护：池被停用时（https/socks 代理）请求走
  `urlopen`，默认处理器会把 Authorization 原样带到跨域目标（已用
  本地 A→B 服务器实测复现）。新增 `_SafeRedirectHandler`，复用池里
  三条规则（拒 https→http、跨域剥凭据、跨域重过白名单），池的回退
  分支改走带该处理器的 opener。
- CI：补上漏跑的 test_opt_20261102、test_opt6b_20261102（CHANGELOG
  原先"全部离线测试"不准确）；测试分两遍跑，一遍装 cryptography、
  一遍不装（上轮 bug 只在装了时出现）；补 `permissions: contents: read`
  与 `timeout-minutes`。
- CI：工作流补装 ffmpeg、修正 proxy 检查后运行全绿（工作流文件须走网页端维护）。
- 测试稳定性：test_open_fixes 的坏密文改成固定全零块（原来 urandom
  4096 字节有 1/256 概率最后一字节恰为 0x01、填充合法而误报）；
  test_v5_e2e 的 SIGHUP 检查改为送达后再等一个代理隧道出现（切换
  本身在下一个轮询周期才生效，切换中途的消息由在途直连轮询接走
  是正常现象，不应判失败）。
- SIGHUP 语义说明：切换在下一个轮询周期生效，切换中途到达的消息
  仍由在途轮询正常投递，不会丢。


## v5.0.2（开源包修正，2026-11-03；同日已发布并回灌线上桥）

- `.gitignore` 锚定：`bridge.py.v*` 和 `wechat-bridge.service.v*` 前加 `/`，
  否则 fresh clone 里 `git add -A` 会漏掉 `tests/fixtures/bridge.py.v1/.v3`
  （test_offline、test_v4 依赖它们）。GitHub 当前树不受影响（快照推送不走 gitignore），
  但新贡献者会踩这个坑；CI 里加了 `git ls-files tests/fixtures` 守门。
- 流式解密：cryptography 后端坏数据抛裸 `ValueError`，文件解密路径没有转成
  `MediaError("decrypt")`（小数据路径一直会转），导致 FILE_DECRYPT_HINT 不触发、
  续传后解不开重下分支也走不到。本机复现条件是装了 cryptography（本机默认
  ctypes 后端所以 v5.0.1 时没测出来）；修法是在文件解密包装器里转换，
  并在 `_aes_stream` 里统一把填充错误转成 MediaError。
- 决策文件：运行时决定改写 `state/DECISIONS.md`（已被忽略），仓库里只留
  `DECISIONS.example.md` 示例；claim 优先读 `state/`，读不到再回退旧位置，
  已有部署的决定不会丢。
- 重定向：跨域重定向现在也要过媒体 URL 白名单（同域重定向不变），
  堵住可信域名上的开放重定向把请求带到任意 https 主机。
- SECURITY.md：私人联系渠道写明为仓库 Security 标签页的私人漏洞报告。
- 新增 CI：GitHub Actions 跑全部离线测试（含 e2e）与夹具守门检查。
  注：CI 文件因连接器令牌无 workflow 权限，改由网页端单独创建（提交 c7e8301），
  后续连接器推送须继续排除该文件；线上回灌见线上 CHANGELOG 的 v5.0.2 条目。


## v5.0.1（开源包修正，2026-11-03；同日已回灌线上桥）

- 文档：EXECUTOR.md 精简到 8000 字节以下，test_opt6 通过；bridge.py、
  backup.sh、recover.sh、服务模板里残留的 OPS.md 引用改到 docs/OPERATIONS.md。
- 可移植性：backup.sh 的项目目录、hook 目录、cron 定义全部改成环境变量可覆盖，
  默认取脚本所在目录，不再硬编码私有路径；doctor.py 的 cron 检查同理（未配置则跳过）。
  「最近文件」的归档目录改成 WECHAT_BRIDGE_ARCHIVE_DIR（默认 icloud-upload/），
  不再以 iCloud 专有名词写进通用逻辑。
- 代理：代理池只接受 http:// CONNECT 代理；https/socks 代理会记一条明确日志后停用池子，
  不再假装接管。重定向：拒绝 https→http 降级，跨域重定向剥离 Authorization/Cookie。
- 媒体 URL：服务端给的下载/上传地址只放行 https 的腾讯 CDN 域名（*.qq.com、*.qpic.cn、*.qlogo.cn），测试用
  WECHAT_BRIDGE_MEDIA_ALLOW_LOCAL=1 放行回环地址；相关测试 mock 已改成 qq.com 域名。
- AES：优先 cryptography，其次系统 libcrypto（ctypes），最后回退 openssl 命令行并记警告；
  密钥不再默认出现在 ps 里。test_v4_e2e 的原生语音场景改为显式开启（与 test_v4 一致），
  此前它按旧默认期待原生语音，与语音发文件卡片的默认行为矛盾。
- 服务模板：User=USERNAME（不再默认 root）、MemoryMax=2G、RestartSec=5、
  WantedBy=multi-user.target。
- 回灌说明：线上桥服务单元只放宽 MemoryMax/RestartSec，仍跑在 root 下、
  WantedBy 不变（hook 重启需要）；线上 OPS/DECISIONS 历史文档未动。


## v5.0（当前版本）

- 24 小时回复窗口与每条消息约 10 条回复额度：本地记账，超额挂起、
  用户下一条消息时自动续发；`stats` 记录服务端实际观测到的额度。
- Markdown 保留渲染：轻量过滤 + 代码块/表格感知分片（UTF-16 计数），
  代码块跨片自动闭合重开、表格跨片重复表头。
- SQLite 引用库（30 天、1 万条）与对话历史；下载 Content-Length 校验 +
  Range 续传。
- `claim --wait` 热执行者与连发合并；打字预取；systemd Type=notify +
  WatchdogSec=180；SIGHUP 重读代理无需重启。
- 直接指令扩展：「取消」「进度」「新话题」「继续」；自动进度改为
  静默期规则；长步骤租约；排队位置显示；`outline` 与历史搜索指令。
- 任务断点续跑与心跳强化：每 5 分钟心跳、10 分钟无心跳收回、
  收回时留检查点、下一任按断点接着做。

## v4.0

- 出站媒体持久队列与后台重试；发送失败按“没发出 / 明确拒绝 /
  可能已送达”分类，`send_uncertain` 不自动重发。
- 对话历史与备忘；引用媒体保留；语音、图片、视频的流式下载与解密。
- 桥接直接回复的状态、帮助、重发指令；通知与自动进度机制。

## v3.0

- 长连接保活、拆分超时、CDN 上传、入站处理、打字指示、回复交接、
  重新登录等传输层基础。

## v2.0

- 首个可用的直接实现：登录扫码、收发消息、任务状态持久化、
  出站队列雏形。

## 开源版说明

- 本开源包基于 v5.0 代码整理：删除了全部运行时私人数据
  （`state/`、收件箱、历史、备份、登录二维码），删除了第三方
  vendored 依赖（改为文档里的 pip 依赖），删除了旧版代码副本，
  只保留当前 `bridge.py` 与对照测试用的旧版夹具。
- 文档按多语言开源项目的做法组织：README 与运维指南各有中文版
  与英文版；EXECUTOR 手册保留中文（它是给中文执行者用的操作手册，
  需要英文版时按同一结构翻译即可）。
