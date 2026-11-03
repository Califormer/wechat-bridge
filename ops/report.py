#!/usr/bin/env python3
"""Operational statistics for the WeChat ClawBot bridge.

Parses logs/bridge.log and the inbox task files into a compact
uptime/incident summary, so trends (restart frequency, poll error
bursts, stale-token events, send problems) are visible at a glance
instead of buried in the log.

Usage: python3 ops/report.py [--days N]   (default: all retained log)
"""
import json
import re
import sys
from collections import Counter
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
LOG = BASE / "logs" / "bridge.log"
INBOX = BASE / "inbox"

days = None
if "--days" in sys.argv:
    days = float(sys.argv[sys.argv.index("--days") + 1])

lines = LOG.read_text(encoding="utf-8", errors="replace").splitlines() \
    if LOG.exists() else []

# Log timestamps are UTC ISO (e.g. 2026-10-27T16:15:25). Convert to
# epoch for windowing.
from datetime import datetime, timezone  # noqa: E402


def epoch(line: str):
    m = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", line)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S") \
            .replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


now = datetime.now(timezone.utc).timestamp()
if days is not None:
    cutoff = now - days * 86400
    lines = [ln for ln in lines if (epoch(ln) or now) >= cutoff]

starts = [ln for ln in lines if "serve starting" in ln]
stops = [ln for ln in lines if "serve stopped" in ln]
poll_errs = [ln for ln in lines if re.search(r"getUpdates .*error", ln)]
notify_fails = [ln for ln in lines if "notifyStart failed" in ln
                or "notifyStop failed" in ln]
stale = [ln for ln in lines if "stale token (-14)" in ln]
auth_alerts = [ln for ln in lines if "auth alert raised" in ln]
# (the old pattern never matched: the bridge logs "reset wedged cursor")
cursor_resets = [ln for ln in lines if "reset wedged cursor" in ln
                 or "resetting get_updates_buf" in ln]
media_fails = [ln for ln in lines if "download failed" in ln]
recoveries = [ln for ln in lines if "inline recovery" in ln.lower()]
rate_limited = [ln for ln in lines if "rate limit" in ln and "-2" in ln]
failovers = [ln for ln in lines if "proxy failover:" in ln and "->" in ln]
# v3 logs "getUpdates error (n) kind=<connect|network|...> detail=<layer>(...)":
# the layer (dns/refused/timeout/tls/tunnel/closed/reset) shows where the
# chain breaks; pre-v3 lines have no detail ("-").
layers = Counter()
for ln in poll_errs:
    m = re.search(r"kind=(\w+)(?: detail=(\w+))?", ln)
    if m:
        layers[f"{m.group(1)}/{m.group(2) or '-'}"] += 1

# Downtime estimate: sum gaps between a "serve stopped" and the next
# "serve starting", capped at 1h per gap (log boundaries).
downtime = 0.0
last_stop = None
for ln in lines:
    ts = epoch(ln)
    if ts is None:
        continue
    if "serve stopped" in ln:
        last_stop = ts
    elif "serve starting" in ln and last_stop is not None:
        downtime += min(ts - last_stop, 3600)
        last_stop = None

# v4: bridge-written notices, chat commands, media queue
notices = Counter(re.search(r"reason=(\S+)", ln).group(1) for ln in lines
                  if "notice queued" in ln and re.search(r"reason=(\S+)", ln))
commands = Counter(re.search(r"command=(\w+)", ln).group(1) for ln in lines
                   if "task stored" in ln and "command=" in ln)
q_sent = [ln for ln in lines if re.search(r"queue: \S+ sent ", ln)]
q_failed = [ln for ln in lines if re.search(r"queue: \S+ -> (failed|uncertain)", ln)]
q_rounds = Counter(re.search(r"rounds=(\d+)", ln).group(1) for ln in q_sent
                   if re.search(r"rounds=(\d+)", ln))
cdn_retries = [ln for ln in lines if "CDN upload attempt" in ln]
reclaims = [ln for ln in lines if "reclaimed stale executing" in ln]
given_up = [ln for ln in lines if "given up after" in ln]

kinds = Counter()
statuses = Counter()
if INBOX.exists():
    for p in INBOX.glob("*.json"):
        try:
            t = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        kinds[t.get("kind") or "?"] += 1
        statuses[t.get("status") or "?"] += 1

span_h = 0.0
epochs = [e for e in (epoch(ln) for ln in lines) if e]
if epochs:
    span_h = (max(epochs) - min(epochs)) / 3600

print(f"window: {len(lines)} log lines, ~{span_h:.1f}h of retained log")
print(f"serve starts: {len(starts)}   stops: {len(stops)}   "
      f"est. downtime in gaps: {downtime/60:.1f} min")
print(f"poll errors: {len(poll_errs)}   notify failures: {len(notify_fails)}")
print(f"stale-token (-14) events: {len(stale)}   auth alerts: {len(auth_alerts)}")
print(f"cursor self-heal resets: {len(cursor_resets)}   "
      f"media download failures: {len(media_fails)}")
print(f"poll error layers (kind/detail): {dict(layers.most_common())}")
print(f"rate-limit (-2) events: {len(rate_limited)}   proxy failovers: {len(failovers)}")
print(f"media queue: sent {len(q_sent)} (by rounds needed: {dict(sorted(q_rounds.items()))}), "
      f"failed/uncertain {len(q_failed)}, CDN upload retries {len(cdn_retries)}")
print(f"bridge notices: {dict(notices)}   chat commands: {dict(commands)}")
print(f"executor reclaims: {len(reclaims)}   given up after reclaim cap: {len(given_up)}")
print(f"tasks by kind: {dict(kinds)}")
print(f"tasks by status: {dict(statuses)}")
# v5: latency per stage from task timestamps; -2 classes; reply-budget parking
def _pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(round(p * (len(v) - 1))))] if v else None


lat = {"wake": [], "exec": [], "deliver": [], "total": []}
parts = []
if INBOX.exists():
    for p in INBOX.glob("*.json"):
        try:
            t = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        ex, sd = t.get("execution") or {}, t.get("send") or {}
        if t.get("status") != "sent" or t.get("kind") in ("command", "notice") \
                or ex.get("proactive") or ex.get("merged_into"):
            continue
        rec, cl = t.get("received_at") or 0, ex.get("claimed_at") or 0
        fin, at = ex.get("finished_at") or 0, sd.get("at") or 0
        if days is not None and rec < (now - days * 86400) * 1000:
            continue
        if rec and cl and fin and at:
            lat["wake"].append(cl - rec)
            lat["exec"].append(fin - cl)
            lat["deliver"].append(max(0, at - fin))
            lat["total"].append(at - rec)
            parts.append(sd.get("parts") or 0)
fmt = lambda ms: "-" if ms is None else f"{ms / 1000:.1f}s"  # noqa: E731
print(f"latency p50/p95 (n={len(lat['total'])}): "
      + "  ".join(f"{k} {fmt(_pct(v, .5))}/{fmt(_pct(v, .95))}" for k, v in lat.items()))
cls = Counter(m.group(1) for m in (re.search(r"classified as (\w+)", ln) for ln in lines) if m)
parked = [ln for ln in lines if re.search(r"deferred=(quota|window)|parked \((quota|window)\)", ln)]
print(f"-2 classes: {dict(cls)}   sends parked by reply budget/window: {len(parked)}   "
      f"fast connect retries: {sum(1 for ln in lines if 'fast retry' in ln)}   "
      f"parts per reply p50: {_pct(parts, .5)}")
if poll_errs:
    print(f"last poll error: {poll_errs[-1][:120]}")
