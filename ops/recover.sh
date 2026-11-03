#!/usr/bin/env bash
# One-click recovery ("一键恢复") - after a VM replacement, or any time the
# WeChat channel is down and the hook is not fixing it by itself.
# The hook normally re-installs the unit and restarts the service within
# seconds (OPS.md: measured 24 s); this script is the manual path for when
# the hook itself is down, and it also re-checks what a fresh VM image may
# lack. Never touches credentials or tasks.
#   1. dependencies: python3 + openssl (required), ffmpeg + SILK codec (optional)
#   2. repair ladder (ops/repair.sh): proxy.env, systemd unit, corrupt state,
#      restart if down or stale
#   3. wait up to 60 s for a successful poll (instead of a fixed sleep)
#   4. doctor summary; --notify sends a short "recovered" message to WeChat
# Usage: ops/recover.sh [--notify]          exit 0 = polling again
set -uo pipefail
BRIDGE_DIR="$HOME/workspace/wechat-bridge"
cd "$BRIDGE_DIR" || { echo "[recover] FAIL: $BRIDGE_DIR missing"; exit 1; }
t0=$(date +%s)

echo "[recover] 1/4 dependencies"
for bin in python3 openssl; do
  command -v "$bin" >/dev/null || { echo "[recover] FAIL: $bin missing"; exit 1; }
done
command -v ffmpeg >/dev/null || echo "[recover] WARN: ffmpeg missing (voice notes / video frames degrade)"
python3 - <<'PY'
import importlib.util, sys
sys.path.insert(0, "vendor/py")
codec = next((n for n in ("pysilk", "pilk") if importlib.util.find_spec(n)), None)
print("[recover] SILK codec: " + (codec or "absent - voice notes go out as audio files "
      "(pip install --target vendor/py silk-python)"))
PY

echo "[recover] 2/4 repair ladder"
printf '{"reason":"vm_recovery","at":%s}' "$(date +%s%3N)" > "$BRIDGE_DIR/state/serve_restart_reason.json"
chmod 600 "$BRIDGE_DIR/state/serve_restart_reason.json" 2>/dev/null || true
REPAIR_WAIT=0 bash ops/repair.sh || echo "[recover] WARN: repair reported a problem"

echo "[recover] 3/4 waiting for a successful poll (max 60s)"
ok=0
for _ in $(seq 1 60); do
  if python3 - <<'PY'
import json, sys, time
try:
    hb = json.load(open("state/heartbeat.json"))
except Exception:
    sys.exit(1)
last, started = hb.get("last_poll_ok_at") or 0, hb.get("serve_started_at") or 0
sys.exit(0 if last > started and time.time() * 1000 - last < 60_000 else 1)
PY
  then ok=1; break; fi
  sleep 1
done

echo "[recover] 4/4 doctor"
python3 ops/doctor.py | tail -n 1
dt=$(( $(date +%s) - t0 ))
if [[ "$ok" == 1 ]]; then
  echo "[recover] OK: polling again (${dt}s)"
  if [[ "${1:-}" == "--notify" ]]; then
    python3 ops/send-direct.py --message "微信桥接已恢复（一键恢复，用时 ${dt} 秒）。" >/dev/null 2>&1 \
      || echo "[recover] WARN: could not send the WeChat notice"
  fi
  exit 0
fi
echo "[recover] FAIL: no successful poll within 60s - see logs/bridge.log, python3 ops/report.py"
exit 1
