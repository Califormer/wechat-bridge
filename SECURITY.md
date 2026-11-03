# 安全说明 / Security

## 中文

- **绝不要提交 `state/`**：`state/credentials.json` 是微信登录凭据，
  0600 权限、只存在你自己的机器上。`.gitignore` 已经排除了它，
  但提交前仍请用 `git status` 核对一遍。
- **绝不要把含 `state/` 的备份上传到云盘**：备份上云前先做脱敏包
  （不含 `state/`、不含 `credentials.json`），并在云盘列表里核对。
- **登录二维码不要发给别人**：`qrcode.png` 扫码即登录你的微信账号，
  用完即删，`.gitignore` 也排除了它。
- **不要在 issue、日志、截图里贴凭据**：`bridge.py status`、
  `ops/doctor.py` 输出都是脱敏的；如果你在别处看到完整 token，
  先作废、重新扫码，再报告问题。
- 发现安全问题：请不要公开开 issue，请通过仓库 Security 标签页的
  Report a vulnerability（私人漏洞报告）提交，说明影响范围、复现步骤
  （不要附凭据）。如果该入口不可见，说明维护者尚未开启它，
  请改走仓库主页上留的其他联系方式并提醒维护者开启。

## English

- **Never commit `state/`**: `state/credentials.json` is your WeChat
  login credential, mode 0600, meant to live only on your own machine.
  `.gitignore` already excludes it, but always check `git status`
  before committing.
- **Never upload a backup containing `state/` to any cloud drive**:
  build a sanitized archive first (no `state/`, no `credentials.json`)
  and verify it in the drive listing.
- **Never send your login QR code to anyone**: scanning `qrcode.png`
  logs into your WeChat account. Delete it after use; `.gitignore`
  excludes it too.
- **Never paste credentials into issues, logs, or screenshots**:
  `bridge.py status` and `ops/doctor.py` outputs are redacted. If you
  see a full token somewhere else, revoke it, re-scan, and then report.
- If you find a security issue, do not open a public issue. Use the
  repository's Security tab, Report a vulnerability (private
  vulnerability reporting), with the impact and reproduction steps
  (without credentials). If that entry is not visible, the maintainer
  has not enabled it yet; use another contact listed on the
  repository home page and ask them to enable it.
