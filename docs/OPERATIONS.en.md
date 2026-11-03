# Operations guide

This guide is for operators: how to tell whether the bridge is healthy,
how to fix it when it is not, and how to handle backups. For the command
reference, see README.en.md.

## Three health layers

1. **systemd**: the service is always restarted on exit
   (`Restart=always`). Since v5, `Type=notify` + `WatchdogSec=180`:
   if the polling loop wedges, systemd restarts it itself;
   `systemctl reload` is SIGHUP, which re-reads proxy.env without losing
   threads, the queue, or the session.
2. **5-second polling hook**: besides task detection, it checks service
   health — the process must be alive and match by PID and command line;
   a live process with no successful poll for 3 minutes also counts as
   unhealthy (except during an official -14 pause). On unhealthy status it
   SIGHUPs first and only restarts the service if it has not recovered in
   20 seconds; the same persistent fault alarms at most once per hour,
   and the alarm budget resets on recovery.
   Note: do not set the hook poll interval below 5 seconds; a definition
   below 5 seconds is rejected by the platform.
3. **Hourly safety-net check**: the hook poll is the main line of defence;
   this only covers the case where the hook itself has failed. Silent when
   healthy, it runs the same recovery flow when unhealthy.
   See `hooks/health-cron.example.md` and fill it in for your own scheduler.

Useful checks:

```bash
python3 bridge.py status     # redacted status
python3 ops/doctor.py        # 18 health checks
python3 ops/report.py --days 1
```

## Recovery

- Service gone or unit file lost (for example, `/etc` wiped after a VM
  replacement): `bash ops/recover.sh`. It reinstalls the unit from the
  project directory, starts it, and waits for one successful poll.
- Service present but unhealthy: `bash ops/repair.sh`.
- Login dead (`auth` / `credentials_missing`, or doctor reports a login
  problem): do not generate a QR code yourself. Stop the service →
  `python3 bridge.py login --force` → scan → `python3 bridge.py wait-login`
  → start the service.
- Executor died mid-task: a task with no heartbeat for 10 minutes is
  reclaimed, and the next executor resumes from the checkpoint; work that
  was never written to disk cannot be resumed, so split long tasks.

## Backups

- `bash ops/backup.sh` creates a dated snapshot; timestamps always use
  America/Caracas.
- Retention: newest 5 dated snapshots + one before each change + milestones;
  excess snapshots are never auto-deleted.
- Recommended excess-cleanup flow (every step matters):
  1. Take a total snapshot first.
  2. Build a sanitized archive of the excess: no `state/`, no `credentials.json`.
  3. Upload it to your own cloud drive and verify it in the listing
     (file name and size).
  4. Byte-verify local files against the archive entries, and delete only
     the ones that are in the archive and byte-identical.
  5. Always keep `state/` copies local; never touch named snapshots.
- `ops/backup.sh --prune-list` lists only excess snapshots that still hold
  non-state files; `--prune` takes a total snapshot first and deletes only
  non-state files. Prune is only the local deletion step after the cloud
  upload flow, never a substitute for it.
- Snapshot bridge.py files are hard-linked when their hash matches the
  previous snapshot: one copy on disk, deleting one snapshot does not affect
  the other; do not edit a hard-linked snapshot file in place, copy it out
  first.

## Common problems

| Symptom | Check first | Action |
| --- | --- | --- |
| WeChat shows the bot as unreachable | `systemctl status`, `ops/doctor.py` | Unit lost: `recover.sh`; unhealthy service: `repair.sh` |
| Task stays pending, nobody claims it | Whether the hook definition is valid and the poll interval is >= 5 seconds | Fix the definition and wait for one poll; do not keep restarting the service |
| "Typing..." disappeared but the task is still executing | Task state, not the typing indicator | Typing is a transient UI state cleared by a new message or a client timeout; task state is the truth |
| Reply stopped halfway | Reply budget / 24-hour window | The overflow is parked and auto-resumes on the next user message; do not resend |
| Voice note sent but the other side sees nothing | Whether native voice was used | The default should be a file card; native voice is only attempted with `WECHAT_BRIDGE_NATIVE_VOICE=1` |
| Image/video fails to send | Size and compression thresholds, CDN upload | Read the specific reason in `media_error`; do not just retry |

## How to read the monitoring numbers

- `status` / `stats` latency segments: wake, exec, deliver. Deliver
  typically has a median under 1 second; exec time is the executor's work,
  not the bridge's speed.
- `observed_limits`: the server's actually observed reply limit. Empty
  means not observed yet, and 10 remains the default; do not probe it with
  dummy messages, that would spam the owner's own WeChat.

## Proxy and media security (since v5.0.1)

- The proxy pool supports http:// CONNECT proxies only; https/socks proxies
  disable the pool with a log line. Do not mistake "pool disabled, urllib
  fallback" for a working proxy.
- Server-provided download/upload URLs must be https on Tencent CDN suffixes (`*.qq.com`, `*.qpic.cn`, `*.qlogo.cn`);
  everything else is refused. Local tests may set
  `WECHAT_BRIDGE_MEDIA_ALLOW_LOCAL=1` to allow loopback addresses.
- AES prefers cryptography, then system libcrypto, then the openssl CLI
  fallback (which logs a warning; install cryptography on production hosts).
