#!/usr/bin/env bash
# Idempotent repair ladder for the WeChat ClawBot bridge.
# Mirrors (and extends) the hook's inline recovery so a human or an
# agent can run the whole ladder by hand:
#   1. refresh state/proxy.env from this process's environment
#   2. reinstall the systemd unit if missing or drifted + enable
#   3. reset corrupt cursor/heartbeat (self-healing state only -
#      credentials and tasks are NEVER touched)
#   4. restart the service if it is down or its polls are stale
#   5. verify with bridge.py status
#   6. rebuild terminal-browser tbtest if missing (VM replacement drops
#      /home/tbtest, which breaks tb-fetch.py URL grabs; the bridge itself
#      keeps polling without it, so a rebuild failure only warns)
# Usage: ops/repair.sh [--force-restart]
set -uo pipefail

# Where this script lives (ops/..); BRIDGE_DIR overrides. Same default
# as before on the live host, and no longer a hard-coded home path.
BRIDGE_DIR="${BRIDGE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
STATE_DIR="$BRIDGE_DIR/state"
UNIT_DST="/etc/systemd/system/wechat-bridge.service"
FORCE="${1:-}"

cd "$BRIDGE_DIR"

echo "[repair] 1/5 refresh proxy.env"
python3 bridge.py write-proxy-env || echo "[repair] WARN: no proxy vars in env"

echo "[repair] 2/5 systemd unit"
if [[ ! -f "$UNIT_DST" ]] || ! cmp -s "$BRIDGE_DIR/wechat-bridge.service" "$UNIT_DST"; then
  cp "$BRIDGE_DIR/wechat-bridge.service" "$UNIT_DST"
  systemctl daemon-reload
  systemctl enable wechat-bridge.service >/dev/null 2>&1 || true
  echo "[repair] unit (re)installed"
else
  echo "[repair] unit present and current"
fi

echo "[repair] 3/5 state files"
for f in cursor.json heartbeat.json; do
  if [[ -f "$STATE_DIR/$f" ]] && ! python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$STATE_DIR/$f" 2>/dev/null; then
    mv "$STATE_DIR/$f" "$STATE_DIR/$f.corrupt.$(date +%s)"
    echo "[repair] quarantined corrupt $f (bridge rebuilds it)"
  fi
done
if [[ -f "$STATE_DIR/credentials.json" ]] && ! python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$STATE_DIR/credentials.json" 2>/dev/null; then
  echo "[repair] FAIL: credentials.json is corrupt - NOT touching it; re-login or restore from ops backup required"
  exit 1
fi

echo "[repair] 4/5 service"
NEED_RESTART=0
if [[ "$FORCE" == "--force-restart" ]]; then
  NEED_RESTART=1
elif ! systemctl is-active --quiet wechat-bridge; then
  NEED_RESTART=1
else
  STALE="$(python3 - <<'PY'
import json, time
try:
    hb = json.load(open("state/heartbeat.json"))
    last = hb.get("last_poll_ok_at") or 0
    started = hb.get("serve_started_at") or 0
    ok = last > started and (time.time() * 1000 - last) < 180000
    print("0" if ok else "1")
except Exception:
    print("1")
PY
)"
  [[ "$STALE" == "1" ]] && NEED_RESTART=1
fi
if [[ "$NEED_RESTART" == "1" ]]; then
  # Tag this restart for serve_starts attribution (bridge.py consumes the
  # file at serve start; a bare `systemctl restart` leaves "unknown").
  # A fresh pre-set reason (e.g. vm_recovery from recover.sh) is kept.
  if ! python3 -c 'import json,time,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if d.get("reason") and time.time()*1000-int(d.get("at") or 0)<600000 else 1)' "$STATE_DIR/serve_restart_reason.json" 2>/dev/null; then
    printf '{"reason":"repair","at":%s}' "$(date +%s%3N)" > "$STATE_DIR/serve_restart_reason.json"
  fi
  chmod 600 "$STATE_DIR/serve_restart_reason.json" 2>/dev/null || true
  systemctl restart wechat-bridge
  echo "[repair] service restarted, waiting ${REPAIR_WAIT:-40}s for first polls..."
  sleep "${REPAIR_WAIT:-40}"
else
  echo "[repair] service healthy, no restart needed"
fi

echo "[repair] 5/5 verify"
python3 bridge.py status

echo "[repair] 6/6 terminal-browser tbtest (tb-fetch.py URL grab)"
TB_REBUILD="$(dirname "$BRIDGE_DIR")/terminal-browser-headless/rebuild-tbtest.sh"
if id tbtest >/dev/null 2>&1 && [ -x /home/tbtest/.local/bin/terminal-browser ]; then
  echo "[repair] tbtest OK (user + binary present, skipping rebuild)"
elif [ -x "$TB_REBUILD" ]; then
  echo "[repair] tbtest missing - rebuilding (VM replacement drops /home/tbtest)"
  bash "$TB_REBUILD" || echo "[repair] WARN: tbtest rebuild failed - URL fetch via tb-fetch.py stays broken until rebuilt manually (bridge polling unaffected)"
  if id tbtest >/dev/null 2>&1 && [ -x /home/tbtest/.local/bin/terminal-browser ]; then
    echo "[repair] tbtest rebuilt"
  else
    echo "[repair] WARN: tbtest rebuild did not produce a working binary - URL fetch via tb-fetch.py stays broken until rebuilt manually (bridge polling unaffected)"
  fi
else
  echo "[repair] WARN: tbtest missing and $TB_REBUILD not found - URL fetch via tb-fetch.py stays broken until rebuilt manually (bridge polling unaffected)"
fi
