#!/usr/bin/env python3
"""Offline tests for ops/watchdog.py (systemctl stubbed, temp dirs)."""
import json, os, shutil, subprocess, sys, tempfile, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
fails = []

def check(c, m):
    print(("PASS  " if c else "FAIL  ") + m)
    if not c: fails.append(m)

def sandbox():
    d = Path(tempfile.mkdtemp(prefix="wd-"))
    (d / "ops").mkdir(); (d / "state").mkdir(); (d / "backups").mkdir()
    shutil.copy(ROOT / "ops/watchdog.py", d / "ops/watchdog.py")
    (d / "bridge.py").write_text("import sys\n")            # write-proxy-env no-op
    (d / "wechat-bridge.service").write_text("[Unit]\n")
    (d / "ops/backup.sh").write_text("#!/usr/bin/env bash\nmkdir -p \"$(dirname \"$0\")/../backups/$(date +%s)\"\ntouch \"$(dirname \"$0\")/../BACKUP_RAN\"\n")
    (d / "ops/backup.sh").chmod(0o755)
    (d / "state/credentials.json").write_text("{}")
    stub = d / "stub"; stub.mkdir()
    (stub / "systemctl").write_text(f"#!/bin/sh\necho \"$@\" >> '{d}/sysctl.log'\n"); (stub / "systemctl").chmod(0o755)
    return d

def serve(d, last_poll_ago=1000, started_ago=600_000):
    p = subprocess.Popen(["bash", "-c", "exec -a 'python3 bridge.py serve' sleep 120"])
    (d / "state/serve.pid").write_text(str(p.pid))
    now = int(time.time() * 1000)
    (d / "state/heartbeat.json").write_text(json.dumps({"serve_started_at": now - started_ago, "last_poll_ok_at": now - last_poll_ago}))
    return p

def run(d):
    env = {**os.environ, "PATH": f"{d/'stub'}:{os.environ['PATH']}", "WATCHDOG_RECOVERY_WAIT": "1",
           "WATCHDOG_UNIT_DST": str(d / "unit-installed.service")}
    r = subprocess.run([sys.executable, str(d / "ops/watchdog.py")], env=env, capture_output=True, text=True, timeout=60)
    return r.returncode, r.stdout.strip()

# healthy: silent, and stale backups get refreshed
d = sandbox(); p = serve(d)
rc, out = run(d); time.sleep(0.2)
check(rc == 0 and out == "", "healthy: no output at all (zero tokens to relay)")
check((d / "BACKUP_RAN").exists(), "healthy + no recent backup: backup refreshed by watchdog")
p.kill()

# healthy with a fresh backup: no backup rerun
d = sandbox(); p = serve(d); (d / "backups/20261028-000000").mkdir()
run(d); check(not (d / "BACKUP_RAN").exists(), "fresh backup present: not re-run")
p.kill()

# session paused after -14 counts as healthy even with stale polls
d = sandbox(); p = serve(d, last_poll_ago=900_000)
(d / "state/session.json").write_text(json.dumps({"paused_until_ms": int(time.time() * 1000) + 3_600_000}))
rc, out = run(d); check(out == "", "official -14 pause: healthy, silent"); p.kill()

# dead serve: recovery attempted (restart called, unit installed), fails -> JSON alert
d = sandbox()
rc, out = run(d); a = json.loads(out)
check(a["alert"] == "recovery_failed" and "restart wechat-bridge" in (d / "sysctl.log").read_text(), "dead serve: recovery ladder runs, failure reported as one JSON line")
check((d / "unit-installed.service").exists(), "missing unit gets reinstalled")

# stale polls with live pid -> unhealthy
d = sandbox(); p = serve(d, last_poll_ago=400_000)
rc, out = run(d); check(json.loads(out)["alert"] == "recovery_failed", "alive but polls stale >180s: treated as down"); p.kill()

# auth alert / credentials gone after confirmed login
d = sandbox(); (d / "state/auth_alert.json").write_text("{}")
check(json.loads(run(d)[1])["alert"] == "auth", "auth_alert.json present: reports auth, does not try to 'fix' it")
d = sandbox(); (d / "state/credentials.json").unlink(); (d / "state/login_status.json").write_text('{"status": "confirmed"}')
check(json.loads(run(d)[1])["alert"] == "credentials_missing", "credentials vanished after confirmed login: alert")
d = sandbox(); (d / "state/credentials.json").unlink()
check(run(d)[1] == "", "fresh install (never logged in): silent")

print("\nALL WATCHDOG CHECKS PASSED" if not fails else f"\n{len(fails)} FAILED")
sys.exit(1 if fails else 0)
