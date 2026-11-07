#!/usr/bin/env bash
# wechat-long-task-watch (2026-11-07, v1.4; v1.7 live fix: idle paths call
# silent(), a bare `exit 0` makes the hook report "exited without calling
# silent() or wake()") - wake the main agent (Muse) when the WeChat bridge
# has handed a long task to the background queue.
#
# Registered by Muse in the main environment (hooks.add -> dry_run -> enable)
# with hooks/wechat-long-task-watch.json: poll 10 s, delivery main. The woken
# worker only relays the task to the main chat; the main agent does it with a
# sub-agent (BACKGROUND.md) - never this hook's worker (single turn, 600 s).
#
# Per run: touch the liveness file the bridge checks before it routes anything
# to the background (no fresh tick = long tasks stay in the foreground relay),
# then one glob + grep over the queue. The bridge CLI runs only when a queued
# task exists; it decides whom to (re)wake - once, again after
# WECHAT_BRIDGE_BG_REWAKE_MIN while nobody took it, at most 3 times - and
# prints the payload. No jq, no state of its own; each wake gets a log line
# in state/long-tasks/watch.log.
set -uo pipefail
source "$HATCH_HOOK_RUNTIME"

BRIDGE_DIR="${WECHAT_BRIDGE_DIR:-$HOME/workspace/wechat-bridge}"
LT="$BRIDGE_DIR/state/long-tasks"
Q="$LT/queue"
mkdir -p "$Q" 2>/dev/null || { silent "cannot create long-task queue dir" '{}'; exit 0; }
chmod 700 "$LT" "$Q" 2>/dev/null || true
touch "$LT/.watch_tick" 2>/dev/null || true

shopt -s nullglob
files=("$Q"/*.json)
(( ${#files[@]} )) || { silent "no long-task queue files" '{}'; exit 0; }
grep -qs '"state": "queued"' "${files[@]}" || { silent "no queued long tasks" '{}'; exit 0; }

out="$(cd "$BRIDGE_DIR" && python3 wb long-tasks wake-check 2>/dev/null)" || { silent "wake-check failed" '{}'; exit 0; }
[[ "$(printf '%s\n' "$out" | sed -n '1p')" == "WAKE" ]] || { silent "no tasks to wake" '{}'; exit 0; }
payload="$(printf '%s\n' "$out" | sed -n '2p')"
ids="$(printf '%s\n' "$out" | sed -n '3p')"
[[ -n "$payload" ]] || { silent "empty wake payload" '{}'; exit 0; }

printf '%s wake %s\n' "$(date +%s%3N 2>/dev/null || date +%s)" "$ids" >> "$LT/watch.log" 2>/dev/null || true
if (( $(stat -c %s "$LT/watch.log" 2>/dev/null || echo 0) > 262144 )); then
  tail -n 2000 "$LT/watch.log" > "$LT/watch.log.tmp" 2>/dev/null \
    && mv -f "$LT/watch.log.tmp" "$LT/watch.log" 2>/dev/null || true
fi
wake "wechat_long_task" "$payload"
