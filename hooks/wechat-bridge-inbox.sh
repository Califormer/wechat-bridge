#!/usr/bin/env bash
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

BRIDGE_DIR="$HOME/workspace/wechat-bridge"
INBOX_DIR="$BRIDGE_DIR/inbox"
STATE_DIR="$BRIDGE_DIR/state"
CREDS="$STATE_DIR/credentials.json"
PID_FILE="$STATE_DIR/serve.pid"
HEARTBEAT="$STATE_DIR/heartbeat.json"
SESSION="$STATE_DIR/session.json"
AUTH_ALERT="$STATE_DIR/auth_alert.json"
WAITER="$STATE_DIR/claim_waiter.txt"   # v5: an executor blocked in `claim --wait`

now_ms="$(( $(date +%s) * 1000 ))"
state="$(hook_state_get)"
# A corrupted hook state must never kill the script.
printf '%s' "$state" | jq -e 'type == "object"' >/dev/null 2>&1 || state='{}'

# 1) Pending tasks, including executors that died mid-run (stale executing).
#    Cost now scales with what CHANGED, not with the 30-day task history:
#    the old version forked one `jq` per task file every tick (hundreds of
#    forks per 5 s once history accumulated) and slurped every file twice.
shopt -s nullglob
MARK="$STATE_DIR/.hook_scan_mark"
NEW_MARK="$STATE_DIR/.hook_scan_mark.new"
FULL_MARK="$STATE_DIR/.hook_full_scan"
mkdir -p "$STATE_DIR"
# Back-dated 2s so a file written in the same instant is re-checked next tick.
touch -d '2 seconds ago' "$NEW_MARK" 2>/dev/null || : >"$NEW_MARK"
task_files=("$INBOX_DIR"/*.json)
changed=()
if [[ -f "$MARK" && -f "$FULL_MARK" && -z "$(find "$FULL_MARK" -mmin +10 2>/dev/null)" ]]; then
  # Incremental: only files modified since the previous tick.
  while IFS= read -r -d '' f; do changed+=("$f"); done \
    < <(find "$INBOX_DIR" -maxdepth 1 -name '*.json' -newer "$MARK" -print0 2>/dev/null)
else
  # First run, and a full re-validation every 10 minutes as a safety net
  # (catches files restored with old mtimes).
  changed=("${task_files[@]}")
  : >"$FULL_MARK"
fi
# Quarantine corrupt task files first: one malformed JSON would
# otherwise kill every jq -s below (verified: jq parse error aborts
# the whole hook, stopping both task wakes and health recovery).
# One jq for the whole batch; only if it complains do we go file by file.
if (( ${#changed[@]} > 0 )) && ! jq -e . "${changed[@]}" >/dev/null 2>&1; then
  for f in "${changed[@]}"; do
    jq -e . "$f" >/dev/null 2>&1 || mv "$f" "$f.corrupt" 2>/dev/null || true
  done
fi
mv -f "$NEW_MARK" "$MARK" 2>/dev/null || true
if (( ${#changed[@]} > 0 )); then
  task_files=("$INBOX_DIR"/*.json)
fi
# One grep pre-selects the few files that can matter; jq then only reads those.
cand=()
if (( ${#task_files[@]} > 0 )); then
  mapfile -t cand < <(grep -lE '"status": *"(pending_execution|executing|send_uncertain)"' -- "${task_files[@]}" 2>/dev/null || true)
fi
if (( ${#cand[@]} > 0 )); then
  # v4: an executor that reports `bridge.py progress` (heartbeat_at) on a
  # long task is alive - do not wake a second one after 10 minutes.
  ids="$(jq -s --argjson now "$now_ms" '
    [ .[] | select(
        .status == "pending_execution"
        or (.status == "executing"
            and ((.execution.claimed_at // 0) > 0)
            and ($now - ([(.execution.claimed_at // 0), (.execution.heartbeat_at // 0)] | max) > 600000))
      ) | .message_id ]' "${cand[@]}")"
  count="$(printf '%s' "$ids" | jq 'length')"
  if [[ "$count" -gt 0 ]]; then
    # Wake throttle: the same unclaimed set is not re-woken for 60s. Before,
    # every 5 s tick spawned another executor for a task that was already
    # being claimed (the prompt even had a "claim returned empty, just
    # stop" branch for those) - each one a full LLM run for nothing.
    sig="$(printf '%s' "$ids" | jq -c 'sort')"
    read -r last_sig last_t < <(printf '%s' "$state" | jq -r '[((.last_task_wake_ids // []) | tostring), ((.last_task_wake_ms // 0) | tostring)] | @tsv') || true
    # v5 (1): an executor already running and blocked in `claim --wait`
    # takes new messages itself within ~1 s, conversation still in its
    # context - no cold start, no second agent.
    waiter=0
    if [[ -f "$WAITER" ]]; then
      wpid=""; wuntil=""
      read -r wpid wuntil _ <"$WAITER" 2>/dev/null || true
      if [[ "$wpid" =~ ^[0-9]+$ && "$wuntil" =~ ^[0-9]+$ ]] \
         && (( wuntil > now_ms )) && kill -0 "$wpid" 2>/dev/null; then
        waiter=1
      fi
    fi
    # v5 (2): messages arriving while the executor woken for earlier ones is
    # still starting (those are not claimed yet) are left to it - it claims
    # up to 5 - instead of starting a second agent for each newcomer.
    still=0
    if [[ "$waiter" -eq 0 && -n "${last_sig:-}" && "${last_sig:-}" != "[]" ]] \
       && (( now_ms - ${last_t:-0} < 45000 )); then
      still="$(jq -nc --argjson a "$last_sig" --argjson u "$ids" '$a - ($a - $u)' | jq 'length')"
    fi
    if [[ "$waiter" -eq 1 ]] || (( still > 0 )); then
      :
    elif [[ "$sig" != "${last_sig:-}" ]] || (( now_ms - ${last_t:-0} > 60000 )); then
      state="$(printf '%s' "$state" | jq -c --argjson s "$sig" --argjson t "$now_ms" '.last_task_wake_ids = $s | .last_task_wake_ms = $t')"
      hook_state_set "$state"
      log "pending wechat tasks" "{\"count\": $count}"
      wake "wechat_task" "{\"task_ids\": $ids, \"count\": $count}"
    fi
  fi
fi

# 1b) Sends with unknown outcome (connection dropped mid-send): never
#     auto-retried (duplicate risk). v4: the bridge tells the user itself
#     and marks send.notified, so only an uncertain send it could NOT
#     announce (notices off or capped, or the notice itself failed) still
#     needs an executor. The alerted list keeps only ids that are still
#     uncertain - it used to grow forever.
if (( ${#cand[@]} > 0 )); then
  uids="$(jq -s '[ .[] | select(.status == "send_uncertain" and ((.send.notified // false) | not)) | .message_id ]' "${cand[@]}")"
  alerted="$(printf '%s' "$state" | jq -c '.alerted_uncertain // []')"
  kept="$(jq -nc --argjson a "$alerted" --argjson u "$uids" '$a - ($a - $u)')"
  new_ids="$(jq -n --argjson a "$uids" --argjson b "$kept" '$a - $b')"
  ncount="$(printf '%s' "$new_ids" | jq 'length')"
  if [[ "$ncount" -gt 0 || "$kept" != "$alerted" ]]; then
    state="$(printf '%s' "$state" | jq -c --argjson ids "$(jq -n --argjson a "$kept" --argjson n "$new_ids" '$a + $n')" '.alerted_uncertain = $ids')"
    hook_state_set "$state"
  fi
  if [[ "$ncount" -gt 0 ]]; then
    log "wechat send uncertain" "{\"count\": $ncount}"
    wake "wechat_send_uncertain" "{\"task_ids\": $new_ids, \"count\": $ncount}"
  fi
fi

# No credentials: a fresh install is fine, but credentials vanishing
# AFTER a confirmed login is an auth emergency - alert (6h throttle),
# never stay silent forever.
if [[ ! -f "$CREDS" ]]; then
  if [[ -f "$STATE_DIR/login_status.json" ]] \
     && grep -q '"confirmed"' "$STATE_DIR/login_status.json" 2>/dev/null; then
    last_auth="$(printf '%s' "$state" | jq -r '.last_auth_alert_ms // 0')"
    if (( now_ms - last_auth > 21600000 )); then
      hook_state_set "$(printf '%s' "$state" | jq -c --argjson t "$now_ms" '.last_auth_alert_ms = $t')"
      log "wechat credentials missing after confirmed login" '{}'
      wake "wechat_auth_alert" '{"reason": "credentials file missing after confirmed login; re-login likely needed"}'
    fi
  fi
  silent "no credentials yet" '{}'
fi

# 2) Auth alert: bridge saw repeated stale-token errors; re-login likely
#    needed. Alert at most once per 6 hours while the alert file exists.
if [[ -f "$AUTH_ALERT" ]]; then
  last_auth="$(printf '%s' "$state" | jq -r '.last_auth_alert_ms // 0')"
  if (( now_ms - last_auth > 21600000 )); then
    hook_state_set "$(printf '%s' "$state" | jq -c --argjson t "$now_ms" '.last_auth_alert_ms = $t')"
    log "wechat auth alert" '{}'
    wake "wechat_auth_alert" '{"reason": "repeated stale token; re-login likely needed"}'
  fi
fi

# 3) Service health: process alive AND polls actually succeeding.
alive=0
if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null \
     && tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null | grep -q "bridge.py serve"; then
    alive=1
  fi
fi

healthy=0
if [[ "$alive" -eq 1 ]]; then
  paused_until=0
  [[ -f "$SESSION" ]] && paused_until="$(jq -r '.paused_until_ms // 0' "$SESSION" 2>/dev/null || echo 0)"
  if (( paused_until > now_ms )); then
    healthy=1   # official 1h session pause after stale token; not a fault
  else
    last_poll=0; started=0; streak=0; fail_at=0
    if [[ -f "$HEARTBEAT" ]]; then
      read -r last_poll started streak fail_at < <(jq -r '[(.last_poll_ok_at // 0), (.serve_started_at // 0), (.poll_fail_streak // 0), (.last_poll_fail_at // 0)] | @tsv' "$HEARTBEAT" 2>/dev/null || echo "0 0 0 0") || true
    fi
    if (( started > 0 && last_poll <= started )); then
      # This serve incarnation has not completed a poll yet. A recent
      # last_poll left behind by the PREVIOUS incarnation must not
      # mask a dead-on-arrival serve (e.g. started with a dead
      # proxy.env). Two ways to judge it unhealthy:
      #  - fast path: the serve itself reports an actively-failing
      #    poll streak (>=4 transport failures spanning >=15s in the
      #    heartbeat) - that is a dead egress, not a slow first poll;
      #  - otherwise a 30s startup grace, then unhealthy at once
      #    instead of waiting for the stale 180s rule.
      if (( streak >= 4 && fail_at - started > 15000 )); then
        healthy=0
      elif (( now_ms - started > 30000 )); then
        healthy=0
      else
        silent "wechat bridge starting" '{}'
      fi
    elif (( now_ms - last_poll < 180000 )); then
      healthy=1
    fi
  fi
fi

if [[ "$healthy" -eq 1 ]]; then
  # Healthy again: clear the alert/recovery timestamps so the NEXT
  # outage recovers and alerts immediately instead of being throttled
  # by the previous one.
  if [[ "$(printf '%s' "$state" | jq -r '.last_service_alert_ms // 0')" != "0" \
     || "$(printf '%s' "$state" | jq -r '.inline_recovery_since_ms // 0')" != "0" \
     || "$(printf '%s' "$state" | jq -r '.hup_at_ms // 0')" != "0" ]]; then
    state="$(printf '%s' "$state" | jq -c '.last_service_alert_ms = 0 | .inline_recovery_since_ms = 0 | .last_inline_recovery_ms = 0 | .hup_at_ms = 0')"
    hook_state_set "$state"
  fi
  # Hourly state snapshot, done by this script (zero LLM tokens). The
  # hourly cron agent used to run backup.sh + doctor.py + status through
  # the model every hour even when everything was fine.
  last_maint="$(printf '%s' "$state" | jq -r '.last_maint_ms // 0')"
  if (( now_ms - last_maint > 3600000 )); then
    state="$(printf '%s' "$state" | jq -c --argjson t "$now_ms" '.last_maint_ms = $t')"
    hook_state_set "$state"
    ( cd "$BRIDGE_DIR" && setsid nohup ./ops/backup.sh >/dev/null 2>&1 & ) >/dev/null 2>&1 || true
  fi
  silent "wechat bridge healthy" '{}'
fi

# v5: a live v5 serve (it advertises hup_pid) whose polls fail is first told
# to re-read proxy.env with SIGHUP: its pool switches egress in-process -
# threads, queue and the session survive, with none of the 10-25 s first-poll
# gap of a restart. Restart only if that has not helped within 20 s.
if [[ "$alive" -eq 1 && -f "$HEARTBEAT" ]]; then
  hup_pid="$(jq -r '.hup_pid // 0' "$HEARTBEAT" 2>/dev/null || echo 0)"
  if [[ "$hup_pid" == "$pid" ]]; then
    hup_at="$(printf '%s' "$state" | jq -r '.hup_at_ms // 0')"
    if (( hup_at == 0 )); then
      (cd "$BRIDGE_DIR" && python3 bridge.py write-proxy-env) >/dev/null 2>&1 || true
      kill -HUP "$pid" 2>/dev/null || true
      state="$(printf '%s' "$state" | jq -c --argjson t "$now_ms" '.hup_at_ms = $t')"
      hook_state_set "$state"
      log "wechat serve asked to reload proxy (SIGHUP)" '{}'
      silent "wechat bridge reloading proxy" '{}'
    elif (( now_ms - hup_at < 20000 )); then
      silent "wechat bridge waiting for proxy reload" '{}'
    fi
  fi
fi

# Unhealthy (down or alive-but-not-polling). First line of defense is
# INLINE recovery by this script itself - no need to wait for a worker
# agent to start: refresh the proxy env, reinstall the systemd unit if
# a VM replacement deleted it, and restart the service. Throttled to
# one attempt per 30s (each attempt is cheap and a restart during a
# network flap is harmless); the outage start is remembered so a
# persistent outage can escalate to a worker below.
inline_since="$(printf '%s' "$state" | jq -r '.inline_recovery_since_ms // 0')"
last_inline="$(printf '%s' "$state" | jq -r '.last_inline_recovery_ms // 0')"
if (( now_ms - last_inline > 30000 )); then
  [[ "$inline_since" -eq 0 ]] && inline_since="$now_ms"
  state="$(printf '%s' "$state" | jq -c --argjson s "$inline_since" --argjson t "$now_ms" '.inline_recovery_since_ms = $s | .last_inline_recovery_ms = $t')"
  hook_state_set "$state"
  (cd "$BRIDGE_DIR" && python3 bridge.py write-proxy-env) >/dev/null 2>&1 || true
  if [[ ! -f /etc/systemd/system/wechat-bridge.service ]]; then
    cp "$BRIDGE_DIR/wechat-bridge.service" /etc/systemd/system/wechat-bridge.service 2>/dev/null || true
    systemctl daemon-reload >/dev/null 2>&1 || true
    systemctl enable wechat-bridge.service >/dev/null 2>&1 || true
  fi
  printf '{"reason":"hook_recovery","at":%s}' "$(date +%s%3N)" > "$BRIDGE_DIR/state/serve_restart_reason.json" 2>/dev/null || true
  systemctl restart wechat-bridge.service >/dev/null 2>&1 || true
  log "wechat serve inline recovery attempted" "{\"alive\": $alive}"
  silent "wechat bridge inline recovery attempted" '{}'
fi

# Inline recovery has had its chance (>90s) and the service is still
# unhealthy: escalate to a worker agent, at most once per hour during
# one continuous outage.
if (( inline_since > 0 && now_ms - inline_since > 90000 )); then
  last_alert="$(printf '%s' "$state" | jq -r '.last_service_alert_ms // 0')"
  if (( now_ms - last_alert > 3600000 )); then
    hook_state_set "$(printf '%s' "$state" | jq -c --argjson t "$now_ms" '.last_service_alert_ms = $t')"
    log "wechat serve unhealthy" "{\"alive\": $alive}"
    wake "wechat_service_unhealthy" "{\"alive\": $alive, \"reason\": \"serve process down or polls stale; inline recovery already attempted\"}"
  fi
fi

silent "wechat bridge unhealthy, recovery in progress" '{}'
