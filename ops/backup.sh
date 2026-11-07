#!/usr/bin/env bash
# Snapshot / restore the bridge's irreplaceable state.
#
# A backup contains: state/ (credentials, cursor, session, login
# status - the login is the one thing that cannot be regenerated
# without the user scanning a QR code), the canonical systemd unit,
# bridge.py, the hook script + definition, and the health cron
# definition. Backups live in backups/<timestamp>/ with 0600 files.
#
# Usage:
#   ops/backup.sh              create a snapshot (retention: newest 5 dated
#                              snapshots + named snapshots, see OPS.md;
#                              excess dated snapshots are NEVER auto-deleted)
#   ops/backup.sh --prune-list   list dated snapshots beyond the newest 5
#                              that still hold non-state files (state-only
#                              shells are NOT listed: there is nothing
#                              left in them to prune)
#   ops/backup.sh --prune        after a total snapshot, delete ONLY the
#                              NON-STATE files of the snapshots listed by
#                              --prune-list; state/ copies are ALWAYS kept.
#                              This is only the local deletion step AFTER
#                              the iCloud upload + byte-verify flow in
#                              OPS.md, never a substitute for it.
#                              Named snapshots (pre-*, opt*, *.v4-*) are
#                              never touched by any prune
#   ops/backup.sh --list       list snapshots
#   ops/backup.sh --restore <timestamp>
#                              stop service, restore state/ + unit,
#                              restart and verify (code files such as
#                              bridge.py are snapshotted for reference
#                              but deliberately NOT rolled back)
set -euo pipefail

# Paths are env-overridable; defaults are this deployment's real paths.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRIDGE_DIR="${BRIDGE_DIR:-$(dirname "$SCRIPT_DIR")}"
BACKUPS="$BRIDGE_DIR/backups"
HOOKS_DIR="${HOOKS_DIR:-$HOME/hooks}"
CRON_SRC="${WECHAT_BRIDGE_CRON_SRC:-$HOME/workspace/goals/clawbot/crons/hourly/wechat-bridge-health__interval@1h.md}"

mkdir -p "$BACKUPS"
chmod 700 "$BACKUPS"

# Snapshot timestamps are always America/Caracas (2026-11-02 tail batch):
# the hourly cron used to stamp in UTC while interactive runs stamped in
# local time, so name order no longer matched creation order. Forcing the
# timezone here unifies both callers; old UTC-stamped snapshots were
# renamed to their Caracas equivalents in the same batch.
SNAP_TZ="America/Caracas"
SNAP_TZ="${WECHAT_BRIDGE_SNAP_TZ:-$SNAP_TZ}"   # other deployments: set their own
snap_date() { TZ="$SNAP_TZ" date +%Y%m%d-%H%M%S; }

# Dated snapshots beyond the newest 5 that still hold files outside
# state/. A state-only shell (left by an earlier upload + byte-verify
# cleanup) has nothing left to prune and is deliberately NOT listed.
prune_candidates() {
  for d in $(ls -1 "$BACKUPS" | grep -E '^[0-9]{8}-[0-9]{6}$' | sort -r | tail -n +6); do
    if find "$BACKUPS/$d" -type f ! -path "$BACKUPS/$d/state/*" -print -quit | grep -q .; then
      echo "$d"
    fi
  done
}

case "${1:-}" in
  --list)
    ls -1 "$BACKUPS" | grep -E '^[0-9]{8}-[0-9]{6}$' || echo "(no backups)"
    ;;
  --prune-list)
    CANDIDATES="$(prune_candidates || true)"
    if [[ -z "$CANDIDATES" ]]; then echo "(none beyond newest 5 with non-state files)"; else echo "$CANDIDATES"; fi
    ;;
  --prune)
    echo "[backup] prune candidates (dated beyond newest 5, non-state files only):"
    CANDIDATES="$(prune_candidates || true)"
    if [[ -z "$CANDIDATES" ]]; then echo "[backup] none; nothing to prune"; exit 0; fi
    echo "$CANDIDATES"
    echo "[backup] NOTE: prune only deletes non-state files, AFTER the iCloud"
    echo "[backup] upload + byte-verify flow in OPS.md. state/ is always kept."
    echo "[backup] total snapshot first (backups/ itself, before any deletion):"
    TOTAL="$BACKUPS/.total-before-prune-$(snap_date).tar.gz"
    tar -czf "$TOTAL" -C "$BACKUPS" --exclude='.total-before-prune-*.tar.gz' . 2>/dev/null || true
    echo "[backup] total snapshot: $TOTAL ($(du -sh "$TOTAL" | cut -f1))"
    echo "$CANDIDATES" | while read -r old; do
      find "${BACKUPS:?}/$old" -type f ! -path "${BACKUPS:?}/$old/state/*" -delete
      rmdir "${BACKUPS:?}/$old/hooks" 2>/dev/null || true
      echo "[backup] pruned non-state files of dated snapshot: $old (state/ kept)"
    done
    echo "[backup] prune done; named snapshots untouched. Remaining dated:"
    ls -1 "$BACKUPS" | grep -E '^[0-9]{8}-[0-9]{6}$' | sort -r || echo "(none)"
    ;;
  --restore)
    STAMP="${2:?usage: backup.sh --restore <timestamp>}"
    SRC="$BACKUPS/$STAMP"
    [[ -d "$SRC" ]] || { echo "no such backup: $STAMP" >&2; exit 1; }
    echo "[backup] stopping service"
    systemctl stop wechat-bridge || true
    cp -a "$SRC/state/." "$BRIDGE_DIR/state/"
    # proxy.env is regenerable and a snapshot taken mid-outage can
    # contain a dead endpoint (learned the hard way) - always refresh
    # it from the current environment instead of trusting the backup.
    (cd "$BRIDGE_DIR" && python3 bridge.py write-proxy-env) \
      || echo "[backup] WARN: could not refresh proxy.env"
    [[ -f "$SRC/wechat-bridge.service" ]] && cp "$SRC/wechat-bridge.service" /etc/systemd/system/wechat-bridge.service
    systemctl daemon-reload
    systemctl enable wechat-bridge.service >/dev/null 2>&1 || true
    systemctl start wechat-bridge
    echo "[backup] restored $STAMP; verifying in 10s..."
    sleep 10
    (cd "$BRIDGE_DIR" && python3 bridge.py status)
    ;;
  "")
    STAMP="$(snap_date)"
    DST="$BACKUPS/$STAMP"
    mkdir -p "$DST"
    chmod 700 "$DST"
    cp -a "$BRIDGE_DIR/state" "$DST/state"
    # proxy.env is deliberately excluded: it is regenerated from the
    # live environment on restore (see --restore), and a snapshot
    # taken during an outage could otherwise restore a dead endpoint.
    # Same reasoning for the failover / transient files: proxy URLs (possibly
    # with credentials), stale endpoints, rate-limit and send-tuning state.
    rm -f "$DST/state/proxy.env" "$DST/state/proxy_pool.json" \
          "$DST/state/proxy-candidates.txt" "$DST/state/ratelimit.json" \
          "$DST/state/send_tuning.json" "$DST/state/voice_tuning.json" \
          "$DST/state/serve_starts.json"
    # Slim (2026-11-02): runtime transient files are useless for rollback.
    rm -f "$DST/state/"*.lock "$DST/state/serve.pid" \
          "$DST/state/.hook_full_scan" "$DST/state/.hook_scan_mark" \
          "$DST/state/.inbox_signal"
    # 2026-11-05 audit (§8-2): the hook's tick log is a diagnostic, never
    # restored, and changes every 5 s (so the hard-link dedup below never
    # applies). Now that it keeps 24 h (up to 1 MiB), copying it into every
    # hourly snapshot - kept forever - would add ~15-20 MB a day.
    rm -f "$DST/state/hook_ticks.log" "$DST/state/hook_ticks.log.tmp"
    # 2026-11-06: the batch index and the failed-text outbox describe what is
    # in flight right now - the bridge recovers both from the inbox/ task
    # files (batch_recover / text_outbox_sync; inbox/ is not backed up
    # either) - and outbox.json holds reply TEXT, while snapshots are kept
    # forever.
    # v1.7: task_index.json is a rebuildable cache of inbox/ - not worth a snapshot
    rm -f "$DST/state/pending.json" "$DST/state/outbox.json" "$DST/state/task_index.json"
    # 2026-11-07 (长任务接力): state/plans holds in-flight long-task plans and
    # their work files (copies of what a task is processing) - not snapshotted.
    # (find -delete: tests/test_tail_20261102.py forbids recursive rm in this script)
    if [ -d "$DST/state/plans" ]; then find "$DST/state/plans" -depth -delete; fi
    # 2026-11-07 v1.4: the background queue and its work dirs - in flight, not snapshotted
    if [ -d "$DST/state/long-tasks" ]; then find "$DST/state/long-tasks" -depth -delete; fi
    cp "$BRIDGE_DIR/wechat-bridge.service" "$DST/" 2>/dev/null || true
    cp "$BRIDGE_DIR/bridge.py" "$DST/" 2>/dev/null || true
    # Slim (2026-11-02): if bridge.py is unchanged vs the newest dated
    # snapshot, hard-link it instead of copying, so disk holds one copy.
    # Never edit a hard-linked snapshot file in place; copy it out first.
    PREV="$(ls -1 "$BACKUPS" | grep -E '^[0-9]{8}-[0-9]{6}$' | sort -r | grep -v "^$STAMP$" | head -1 || true)"
    if [[ -n "$PREV" && -f "$BACKUPS/$PREV/bridge.py" && -f "$DST/bridge.py" ]]; then
      if cmp -s "$BACKUPS/$PREV/bridge.py" "$DST/bridge.py"; then
        rm "$DST/bridge.py"
        ln "$BACKUPS/$PREV/bridge.py" "$DST/bridge.py"
      fi
    fi
    # 2026-11-04 review: the same for unchanged state/ files. Every hourly
    # snapshot keeps a complete state/ (the standing rule) but no longer a
    # new physical copy of credentials.json / token_history.json each time:
    # unchanged files are hard links to the previous snapshot's copy.
    if [[ -n "$PREV" && -d "$BACKUPS/$PREV/state" ]]; then
      while IFS= read -r -d '' f; do
        rel="${f#"$DST/state/"}"
        old="$BACKUPS/$PREV/state/$rel"
        if [[ -f "$old" && ! -L "$old" ]] && cmp -s "$old" "$f"; then
          ln -f "$old" "$f" 2>/dev/null || true
        fi
      done < <(find "$DST/state" -type f -print0)
    fi
    mkdir -p "$DST/hooks"
    cp "$HOOKS_DIR/scripts/wechat-bridge-inbox.sh" "$DST/hooks/" 2>/dev/null || true
    cp "$HOOKS_DIR/definitions/wechat-bridge-inbox.json" "$DST/hooks/" 2>/dev/null || true
    [[ -f "$CRON_SRC" ]] && cp "$CRON_SRC" "$DST/health-cron.md" || true
    find "$DST" -type f -exec chmod 600 {} +
    # Retention (OPS.md, 2026-11-02 rule): newest 5 dated snapshots +
    # named snapshots; excess dated snapshots are NOT auto-deleted here.
    # Run --prune-list to see them; --prune deletes only their non-state
    # files, after a total snapshot and after the iCloud upload flow.
    EXCESS="$(prune_candidates || true)"
    if [[ -n "$EXCESS" ]]; then
      echo "[backup] dated snapshots beyond newest 5 (kept, not auto-deleted):"
      echo "$EXCESS"
      echo "$EXCESS" > "$BACKUPS/.prune-candidates.txt"
    fi
    echo "[backup] created $STAMP ($(du -sh "$DST" | cut -f1))"
    ;;
  *)
    echo "usage: backup.sh [--list | --prune-list | --prune | --restore <timestamp>]" >&2
    exit 2
    ;;
esac
