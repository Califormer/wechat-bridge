# WeChat Bridge

[中文版](README.md) · [Operations guide (Chinese)](docs/OPERATIONS.md) · [Operations guide (English)](docs/OPERATIONS.en.md)

A WeChat send/receive bridge implemented directly against Tencent's
official openclaw-weixin backend protocol (`https://ilinkai.weixin.qq.com`,
protocol version matching plugin 2.4.10). It does not install OpenClaw
and does not configure any model. The bridge only moves messages and
task state; tasks are executed by an executor (for example a Muse agent)
with real tools, and results are sent back to WeChat through the bridge.

> **Three boundaries, stated up front**
>
> - **Platform limits are not bugs in this project**: after a user message
>   there is a roughly 24-hour reply window; each user message allows
>   roughly 10 replies, and the overflow is parked and resumed on the
>   user's next message; bot-direction native voice notes may be silently
>   dropped by the channel (the server returns success, the client shows
>   nothing), so this project sends voice as a file card by default.
> - **Credentials stay on your own machine**: after login, credentials are
>   written to `state/credentials.json` (0600). Never commit it to version
>   control and never upload it to any cloud drive. `.gitignore` already
>   excludes `state/`, `inbox/`, `history/`, `backups/`, and the rest.
> - **This project does not decide for you**: deleting important files,
>   changing sensitive configuration, disclosing private information, or
>   paying for anything should first be confirmed with the owner.

## Features

- Long-polling with connection reuse, and send failures classified as
  "never sent / explicitly refused / possibly delivered", so a possibly
  delivered message is never auto-repeated.
- Long replies split by UTF-16 count, with code fences auto-closed and
  reopened across chunks and table headers repeated; if the server
  refuses a long chunk, it is re-split smaller and remembered for 24 hours.
- Local accounting for the 24-hour window and the reply budget, with
  overflow parked and auto-resumed on the next user message.
- Image, file, video, and voice messaging: images are decrypted and saved
  locally for the executor to read; large images and videos are compressed
  above a threshold before sending; voice goes out as a file card by default.
- Checkpoint/resume for tasks: the executor must heartbeat at least every
  5 minutes; a task with no heartbeat for 10 minutes is reclaimed, and the
  next executor resumes from the checkpoint instead of starting over.
- Long-step leases: `progress --hold-minutes 1-60` declares an
  uninterrupted long step during which the task is not reclaimed.
- Commands answered directly by the bridge (no executor wake-up):
  status, progress, cancel, continue, new topic, resend, help, plus
  recent files, backup status, recent dialogue, and history search.
- Three health layers: systemd (Type=notify + WatchdogSec=180), a
  5-second polling hook, and an hourly safety-net check
  (see `docs/OPERATIONS.en.md`).

## Requirements

- Linux + systemd (for service hosting; you can also run `serve` in the
  foreground without systemd)
- Python 3.12+
- `openssl` (image decryption uses AES-128-ECB; the standard library has
  no AES)
- `ffmpeg` (voice transcoding and video compression; without it, the
  related features fail loudly instead of silently)
- Voice decoding is optional: `pip install silk-python` (without it,
  inbound voice still arrives as a file, it just is not converted to WAV)
- QR code generation: `pip install qrcode[pil]`

## Quick start

```bash
# 1. Enter the project directory
cd wechat-bridge

# 2. Scripts are run with bash / python3; if you want to execute them directly, run chmod +x ops/*.sh hooks/*.sh first
# 2. Generate a login QR code and scan it with WeChat
#    (the QR file is qrcode.png; do not send it to anyone)
python3 bridge.py login

# 3. Wait for the scan confirmation
python3 bridge.py wait-login

# 4. Run in the foreground first (hand it to systemd once it works)
python3 bridge.py serve

# 5. Check the status
python3 bridge.py status
```

Service deployment (replace `USERNAME` and the paths with your own):

```bash
sudo cp wechat-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now wechat-bridge
systemctl status wechat-bridge
```

To re-login: stop the service → `python3 bridge.py login --force` →
scan → `python3 bridge.py wait-login` → start the service.

## Command reference (inside the project directory)

- `python3 bridge.py login [--force]`: generate a login QR code
- `python3 bridge.py wait-login [--verify-code CODE]`: wait for scan confirmation
- `python3 bridge.py serve`: run the send/receive service in the foreground (systemd-hosted)
- `python3 bridge.py status`: redacted status (never prints credentials)
- `python3 bridge.py claim [--limit N] [--full] [--wait SECONDS]`: the executor
  claims tasks; output carries `conversation` (recent dialogue + memo) and
  `decisions` (the decision list); `--wait` lets the executor wait in place
  for follow-ups, with `merge_hint` for bursts
- `python3 bridge.py complete --task-id ID --result TEXT | --result-file F | --error MSG [--attach FILE ...] [--merge-ids ID,ID]`: deliver a reply
- `python3 bridge.py progress --task-id ID [--stage TEXT] [--note TEXT] [--notify] [--hold-minutes 1-60]`: heartbeat / long-step lease
- `python3 bridge.py heartbeat --task-id ID [--stage TEXT]`: heartbeat only, sends no message
- `python3 bridge.py send-media [--task-id ID | --to-user USER] --file F [--as auto|file|image|video|voice] [--wait 60]`: send a file directly
- `python3 bridge.py send-image | send-file | send-video ...`: legacy single-file
  commands (wrappers over the same attachment queue)
- `python3 bridge.py queue [--retry QID | --cancel QID]`: inspect or operate on the outbound media queue
- `python3 bridge.py history [--limit 30] [--hours 72] [--json]`: WeChat dialogue history
- `python3 bridge.py memo [--set TEXT | --clear]`: executor-maintained dialogue memo
- `python3 bridge.py decisions [--add "..."]`: record a decision (never write credentials here)
- `python3 bridge.py outline [--search KEYWORD]`: function index of bridge.py
  (check this before reading the 8000-line file)
- `python3 bridge.py voice-to-wav --in F [--out F.wav]`: voice to WAV
- `python3 bridge.py stats`: latency p50/p95 and the server's observed reply limit
- `python3 bridge.py canary-dedupe [--confirm yes|no]`: test whether the server
  deduplicates by client_id (not relied on by default)
- `python3 bridge.py stop`: stop the service
- `python3 ops/doctor.py`: 18 health checks
- `python3 ops/report.py --days 1`: report for the last day
- `bash ops/recover.sh`: one-click recovery (see the operations guide)

## Muse executor (optional)

If you use Muse as the executor, `hooks/` contains the companion polling hook:

- `hooks/wechat-bridge-inbox.sh`: every 5 seconds, checks `inbox/` for
  `pending_execution` tasks and wakes an executor; it also checks service
  health, first SIGHUP-reloading the proxy, and only restarts the service
  if it has not recovered in 20 seconds.
- `hooks/wechat-bridge-inbox.json`: hook definition template. Note:
  **do not set the poll interval below 5 seconds**; a definition below
  5 seconds is rejected by the platform and stops all wakes.
- `hooks/health-cron.example.md`: template for the hourly safety-net check
  (fill it in for your own scheduler; do not copy the placeholders verbatim).
- The executor manual is `EXECUTOR.md` plus the `EXECUTOR-*.md` volumes:
  read the main file normally, and the matching volume only for media,
  attachments, commands, or service problems.

Without Muse, the CLI alone is enough: `claim` / `progress` / `complete`
are the entire interface between an executor and the bridge.

## Security notes (summary; full version in SECURITY.md)

- **Proxies are http:// only**: the pool speaks plaintext CONNECT, never TLS
  to the proxy itself, and does not speak SOCKS. With an https/socks proxy
  configured, the pool disables itself with a clear log line; do not assume
  it took over.
- **Server URL allowlist**: download/upload URLs handed by the server must be
  https on Tencent CDN suffixes (`*.qq.com`, `*.qpic.cn`, `*.qlogo.cn`). Tests may set `WECHAT_BRIDGE_MEDIA_ALLOW_LOCAL=1` to
  allow loopback addresses.
- **AES backend**: cryptography first, system libcrypto second, openssl CLI
  last (the key is briefly visible in ps on fallback, with a warning logged;
  installing cryptography avoids it).
- **Do not run the service as root**: set `User=USERNAME` in the template to
  a dedicated user or your own login user. `MemoryMax=2G`, `RestartSec=5`,
  `WantedBy=multi-user.target`.

## Environment variables (common)

| Variable | Default | Meaning |
| --- | --- | --- |
| `WECHAT_BRIDGE_CTX_QUOTA` | 10 | Reply limit per user message (`stats` `observed_limits` shows the server's real limit) |
| `WECHAT_BRIDGE_CONTEXT_CHARS` | 4000 | Claim context character limit |
| `WECHAT_BRIDGE_CONTEXT_TURNS` | 10 | Claim context turns |
| `WECHAT_BRIDGE_AUTO_PROGRESS_SECS` | 180,600 | Auto-progress stages in seconds; tasks with a live heartbeat use a 420-second silence rule instead |
| `WECHAT_BRIDGE_AUTO_PROGRESS_SEC` | (none) | Single-stage compatibility alias; if set, it wins |
| `WECHAT_BRIDGE_TZ` | Asia/Shanghai | Display timezone used by the bridge |
| `WECHAT_BRIDGE_NATIVE_VOICE` | (none) | Set to `1` to attempt native voice notes; default is a file card |
| `WECHAT_BRIDGE_MARKDOWN` | filter | Markdown handling mode |

See the `WECHAT_BRIDGE_*` names in the code for the full list; do not
change the defaults unless you need to.

## Backup and recovery

- Snapshot: `bash ops/backup.sh`, producing `backups/<timestamp>/`
  (state, bridge.py, unit file, hook definition). Timestamps always use
  America/Caracas.
- Retention: newest 5 dated snapshots + one before each change + milestones;
  excess snapshots are **never auto-deleted**. Recommended flow: first build
  a **sanitized archive** (no `state/`, no `credentials.json`), upload it
  to your own cloud drive and verify it in the listing, then byte-verify
  local files and delete only the ones that are in the archive and
  byte-identical. Keep `state/` copies local and never touch named snapshots.
- `ops/backup.sh --prune-list`: lists only excess snapshots that still hold
  non-state files; `ops/backup.sh --prune`: takes a total snapshot first,
  then deletes only non-state files. Prune is only the local deletion step
  after the cloud upload flow, never a substitute for it.
- Restore: `bash ops/backup.sh --restore <timestamp>` (restores state and
  the unit file only; code files are deliberately not rolled back, they are
  reference only); if the service is gone, use `bash ops/recover.sh`.

## Tests

```bash
python3 tests/test_v5.py        # main test suite for the current version
python3 tests/test_v4.py        # previous generation (with the native-voice opt-in scenario)
python3 tests/test_offline.py   # offline: heartbeat, index, send semantics
python3 tests/test_resume.py    # checkpoint/resume and heartbeat
python3 tests/test_round2.py    # silence-based progress, long-step leases, queue position
python3 tests/test_hook.py tests/test_watchdog.py
```

All tests are offline (local fake iLink / fake CDN / real CONNECT proxy);
they never connect to real WeChat and never send real messages.
`tests/fixtures/` holds old bridge.py versions used only by comparison
tests; they are not project code.

## Security

See [SECURITY.md](SECURITY.md). The three most important rules: never
commit `state/`; never upload a backup containing `state/` to any cloud
drive; never send your login QR code to anyone.

## License

MIT, see [LICENSE](LICENSE). Copyright holder: Califormer.

## Protocol source

This project is an independent implementation of Tencent's official
openclaw-weixin protocol (`ilinkai.weixin.qq.com`, plugin version 2.4.10).
It contains no OpenClaw code and depends on no model service.
