#!/usr/bin/env python3
"""Zero-LLM hourly watchdog (replaces the agent-run cron routine).

Does what the cron prompt used to ask a model to do - judge health, run the
recovery ladder, keep backups fresh - in plain code, so a healthy hour costs
no model tokens beyond a one-line "nothing to report" run.

stdout contract:
  (empty)                      everything fine, say nothing
  {"alert": "recovered"}       was down, auto-recovered
  {"alert": "recovery_failed", "detail": "..."}
  {"alert": "auth"}            login looks dead -> user must re-scan the QR
  {"alert": "credentials_missing"}
Exit code is always 0. Never prints proxy values or credentials.
"""
import json, os, shutil, subprocess, sys, time
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
STATE = BASE / "state"
UNIT_SRC = BASE / "wechat-bridge.service"
UNIT_DST = Path(os.environ.get("WATCHDOG_UNIT_DST", "/etc/systemd/system/wechat-bridge.service"))
RECOVERY_WAIT = int(os.environ.get("WATCHDOG_RECOVERY_WAIT", "40"))
BACKUP_MAX_AGE_SEC = 2 * 3600


def rj(p, default=None):
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def now_ms():
    return int(time.time() * 1000)


def serve_alive() -> bool:
    try:
        pid = int((STATE / "serve.pid").read_text().strip())
        os.kill(pid, 0)
        cmd = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
        return "bridge.py" in cmd and "serve" in cmd
    except (OSError, ValueError):
        return False


def healthy() -> bool:
    if not serve_alive():
        return False
    if (rj(STATE / "session.json", {}) or {}).get("paused_until_ms", 0) > now_ms():
        return True                       # official 1h pause after -14, not a fault
    hb = rj(STATE / "heartbeat.json", {}) or {}
    last, started = hb.get("last_poll_ok_at") or 0, hb.get("serve_started_at") or 0
    if started and last <= started:       # this incarnation has not polled yet
        return now_ms() - started < 30_000
    return now_ms() - last < 180_000


def run(cmd, timeout=60):
    try:
        subprocess.run(cmd, cwd=BASE, capture_output=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError):
        pass


def recover() -> bool:
    run([sys.executable, "bridge.py", "write-proxy-env"])
    if not UNIT_DST.exists() and UNIT_SRC.exists():
        try:
            shutil.copy(UNIT_SRC, UNIT_DST)
        except OSError:
            pass
        run(["systemctl", "daemon-reload"])
        run(["systemctl", "enable", "wechat-bridge.service"])
    run(["systemctl", "daemon-reload"])
    try:
        (STATE / "serve_restart_reason.json").write_text(
            json.dumps({"reason": "watchdog", "at": now_ms()}))
    except OSError:
        pass
    run(["systemctl", "restart", "wechat-bridge"])
    time.sleep(RECOVERY_WAIT)
    return healthy()


def newest_backup_age() -> float:
    d = BASE / "backups"
    try:
        stamps = [p.stat().st_mtime for p in d.iterdir() if p.is_dir()]
    except OSError:
        return float("inf")
    return time.time() - max(stamps) if stamps else float("inf")


def main() -> int:
    def out(**kw):
        print(json.dumps(kw, ensure_ascii=False))

    creds_ok = (STATE / "credentials.json").exists()
    confirmed = "confirmed" in ((STATE / "login_status.json").read_text()
                                if (STATE / "login_status.json").exists() else "")
    if not creds_ok and confirmed:
        out(alert="credentials_missing")
        return 0
    if not creds_ok:
        return 0                          # fresh install, nothing to guard yet
    if (STATE / "auth_alert.json").exists():
        out(alert="auth")
        return 0
    if healthy():
        if newest_backup_age() > BACKUP_MAX_AGE_SEC and (BASE / "ops/backup.sh").exists():
            run(["bash", str(BASE / "ops/backup.sh")], timeout=120)
        return 0
    if recover():
        out(alert="recovered")
    else:
        hb = rj(STATE / "heartbeat.json", {}) or {}
        out(alert="recovery_failed",
            detail=f"serve_alive={serve_alive()} last_poll_ok_at={hb.get('last_poll_ok_at')} "
                   f"poll_fail_streak={hb.get('poll_fail_streak')}; run ops/repair.sh / see logs/bridge.log")
    return 0


if __name__ == "__main__":
    sys.exit(main())
