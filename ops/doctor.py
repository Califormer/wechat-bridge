#!/usr/bin/env python3
"""Read-only health audit for the WeChat ClawBot bridge.

Checks every layer that can silently break 7x24 operation and prints
PASS / WARN / FAIL lines (plus a summary). Never mutates anything and
never prints secrets: credentials are reported masked, proxy values
only as short hashes.

Usage: python3 ops/doctor.py [--json]
Exit code: 0 = no FAIL, 1 = at least one FAIL.
"""
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
STATE = BASE / "state"
INBOX = BASE / "inbox"
LOGS = BASE / "logs"
UNIT_PATH = Path("/etc/systemd/system/wechat-bridge.service")
UNIT_CANON = BASE / "wechat-bridge.service"
HOOK_DEF = Path.home() / "hooks" / "definitions" / "wechat-bridge-inbox.json"
# Optional override; default is this deployment's real cron definition.
_cron_env = os.environ.get("WECHAT_BRIDGE_CRON_DEF")
CRON_DEF = Path(_cron_env) if _cron_env else (Path.home() / "workspace" / "goals" / "clawbot" / "crons"
            / "hourly" / "wechat-bridge-health__interval@1h.md")

results = []
PROXY_VAR_NAMES = {"http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY",
                   "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY"}


def add(name: str, status: str, detail: str = "") -> None:
    results.append({"check": name, "status": status, "detail": detail})


def load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def sha8(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:8]


def mode_str(path: Path) -> str:
    return oct(path.stat().st_mode & 0o777)


def check_credentials() -> None:
    creds_f = STATE / "credentials.json"
    if not creds_f.exists():
        add("credentials", "FAIL", "credentials.json missing - re-login needed")
        return
    if (creds_f.stat().st_mode & 0o777) != 0o600:
        add("credentials", "WARN", f"permissions {mode_str(creds_f)}, want 0600")
    creds = load_json(creds_f)
    if not creds or not creds.get("bot_token"):
        add("credentials", "FAIL", "credentials.json unparsable or no token")
        return
    bot = creds.get("ilink_bot_id") or ""
    masked = f"{bot[:6]}...{bot[-2:]}" if len(bot) > 8 else "present"
    add("credentials", "PASS", f"bot={masked} saved_at={creds.get('saved_at')}")


def check_proxy_env() -> None:
    env_f = STATE / "proxy.env"
    if not env_f.exists():
        # v5 also runs without a proxy (direct keep-alive pool): a missing
        # file is only a fault when serve is not polling OK without it.
        hb = load_json(STATE / "heartbeat.json") or {}
        last = int(hb.get("last_poll_ok_at") or 0)
        if hb.get("transport_mode") == "direct" and last \
                and int(time.time() * 1000) - last < 180_000:
            add("proxy_env", "PASS", "no proxy.env: direct mode, polling OK")
        else:
            add("proxy_env", "FAIL", "state/proxy.env missing and serve is not "
                "polling OK without a proxy")
        return
    if (env_f.stat().st_mode & 0o777) != 0o600:
        add("proxy_env", "WARN", f"permissions {mode_str(env_f)}, want 0600")
    file_vars = {}
    for line in env_f.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:]
        if "=" in line:
            k, v = line.split("=", 1)
            file_vars[k.strip()] = v.strip().strip('"').strip("'")
    # 2026-11-04 review: proxy.env is the EnvironmentFile of a ROOT service;
    # anything but proxy variables there (LD_PRELOAD=, PYTHONPATH=, ...) runs
    # with root's rights at the next start.
    extra = sorted(k for k in file_vars if k not in PROXY_VAR_NAMES)
    if extra:
        add("proxy_env_vars", "FAIL", f"proxy.env sets non-proxy variables {extra}: "
            "remove them (bridge.py write-proxy-env only writes proxy variables)")
    amb = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or ""
    file_https = file_vars.get("HTTPS_PROXY") or file_vars.get("https_proxy") or ""
    if not file_https:
        add("proxy_env", "FAIL", "proxy.env has no HTTPS_PROXY entry")
    elif amb and file_https != amb:
        # 2026-11-02, 四: drift alone is not a fault. If the file endpoint
        # is the one currently polling successfully, downgrade the permanent
        # WARN to a PASS note - otherwise a healthy bridge carries a WARN
        # forever and a real WARN hides behind it. Still WARN when the
        # heartbeat is stale or the live endpoint is not the file one.
        hb = load_json(STATE / "heartbeat.json") or {}
        last_poll = hb.get("last_poll_ok_at") or 0
        live = str(hb.get("proxy_id") or "")
        age_ms = int(time.time() * 1000) - last_poll
        if last_poll and age_ms < 180_000 and live \
                and sha8(file_https).startswith(live):
            add("proxy_env", "PASS",
                f"endpoint drift noted (file={sha8(file_https)} ambient={sha8(amb)}) "
                f"but the file endpoint is still polling OK ({age_ms // 1000}s ago) - ignored")
        else:
            add("proxy_env", "WARN",
                f"endpoint drift: file={sha8(file_https)} ambient={sha8(amb)} "
                "(old endpoint may still work; hook refreshes the file, "
                "next restart picks the fresh one up)")
    else:
        add("proxy_env", "PASS", f"endpoint hash={sha8(file_https)}")


def check_unit() -> None:
    if not UNIT_PATH.exists():
        add("systemd_unit", "FAIL",
            "unit file missing (VM replacement?) - repair reinstalls it")
        return
    if UNIT_PATH.read_bytes() != UNIT_CANON.read_bytes():
        add("systemd_unit", "WARN", "installed unit differs from canonical copy")
        return
    add("systemd_unit", "PASS", "installed and matches canonical copy")


def run(*args: str) -> str:
    try:
        return subprocess.run(args, capture_output=True, text=True,
                              timeout=10).stdout.strip()
    except Exception as e:  # noqa: BLE001
        return f"<error {e}>"


def check_service() -> None:
    active = run("systemctl", "is-active", "wechat-bridge")
    enabled = run("systemctl", "is-enabled", "wechat-bridge")
    if active != "active":
        add("service", "FAIL", f"systemctl is-active = {active}")
    elif enabled != "enabled":
        add("service", "WARN", f"active but is-enabled = {enabled}")
    else:
        add("service", "PASS", "active + enabled")


def check_heartbeat() -> None:
    hb = load_json(STATE / "heartbeat.json")
    if hb is None:
        add("heartbeat", "FAIL", "heartbeat.json missing or corrupt")
        return
    now = int(time.time() * 1000)
    last_poll = hb.get("last_poll_ok_at") or 0
    started = hb.get("serve_started_at") or 0
    age_s = (now - last_poll) // 1000 if last_poll else None
    if last_poll > started and age_s is not None and age_s < 180:
        extra = ""
        if hb.get("net_connects") is not None:
            c, r = hb.get("net_connects") or 0, hb.get("net_reused") or 0
            extra = f", connections reused {r}/{c + r}"
        add("heartbeat", "PASS",
            f"last poll {age_s}s ago, serve pid={hb.get('serve_pid')}{extra}")
    elif started and last_poll <= started and now - started < 60_000:
        add("heartbeat", "WARN", "serve started <60s ago, no poll yet")
    else:
        add("heartbeat", "FAIL",
            f"poll stale: last_poll age={age_s}s started={started} "
            f"last_poll={last_poll}")


def check_state_files() -> None:
    problems = []
    for name in ("cursor.json", "session.json", "login_status.json"):
        p = STATE / name
        if p.exists() and load_json(p) is None:
            problems.append(f"{name} corrupt")
    if problems:
        add("state_files", "WARN", "; ".join(problems)
            + " (bridge self-heals cursor/heartbeat; repair can reset)")
    else:
        add("state_files", "PASS", "cursor/session/login_status parse cleanly")
    rl = load_json(STATE / "ratelimit.json") or {}
    if (rl.get("until_ms") or 0) > int(time.time() * 1000):
        add("rate_limit", "WARN", "iLink -2 rate limit active: sends deferred "
            "until it expires or the user sends a message")
    if (STATE / "auth_alert.json").exists():
        add("auth_alert", "FAIL",
            "auth_alert.json present - login likely dead, re-login needed")
    else:
        add("auth_alert", "PASS", "no auth alert")


def check_inbox() -> None:
    now = int(time.time() * 1000)
    counts: dict = {}
    stuck_exec = stuck_wait = stuck_dl = uncertain = corrupt = 0
    if INBOX.exists():
        for p in INBOX.iterdir():
            if p.suffix == ".corrupt":
                corrupt += 1
                continue
            if p.suffix != ".json":
                continue
            t = load_json(p)
            if t is None:
                corrupt += 1
                continue
            st = t.get("status") or "?"
            counts[st] = counts.get(st, 0) + 1
            if st == "executing":
                claimed = (t.get("execution") or {}).get("claimed_at") or 0
                if claimed and now - claimed > 600_000:
                    stuck_exec += 1
            elif st == "waiting_send":
                if now - (t.get("received_at") or now) > 300_000:
                    stuck_wait += 1
            elif st == "downloading":
                if now - (t.get("received_at") or now) > 900_000:
                    stuck_dl += 1
            elif st == "send_uncertain":
                uncertain += 1
    detail = f"tasks={counts}"
    worst = "PASS"
    if stuck_exec or stuck_wait or stuck_dl:
        worst = "WARN"
        detail += (f" stuck_executing={stuck_exec} stuck_waiting={stuck_wait}"
                   f" stuck_downloading={stuck_dl}")
    if uncertain:
        worst = "WARN"
        detail += f" send_uncertain={uncertain} (needs a human resend decision)"
    if corrupt:
        detail += f" quarantined_corrupt={corrupt}"
    add("inbox", worst, detail)


def check_disk_and_logs() -> None:
    usage = os.statvfs(BASE)
    free_gb = usage.f_bavail * usage.f_frsize / 1e9
    total = subprocess.run(["du", "-sm", str(BASE)], capture_output=True,
                           text=True).stdout.split("\t")[0]
    log_size = 0
    if LOGS.exists():
        log_size = sum(p.stat().st_size for p in LOGS.iterdir() if p.is_file())
    media = 0
    for d in ("inbox_media", "inbox_files"):
        p = BASE / d
        if p.exists():
            media += sum(f.stat().st_size for f in p.iterdir() if f.is_file())
    status = "PASS" if free_gb > 5 else ("WARN" if free_gb > 1 else "FAIL")
    add("disk", status,
        f"free={free_gb:.1f}GB project={total}MB logs={log_size//1024}KB "
        f"media={media//1024}KB")


def check_watchers() -> None:
    hook = load_json(HOOK_DEF)
    if hook is None:
        add("hook", "FAIL", "hook definition missing/unparsable")
    elif not hook.get("enabled"):
        add("hook", "WARN", "hook definition present but disabled")
    else:
        add("hook", "PASS",
            f"wechat-bridge-inbox enabled, poll={hook.get('poll_interval_secs')}s")
    if CRON_DEF.exists():
        add("hourly_cron", "PASS", "health cron definition present")
    else:
        add("hourly_cron", "WARN",
            "health cron definition file not found under goal workspace")


def check_deps() -> None:
    """v4: what a fresh VM image may lack. An AES backend is required
    (cryptography, libcrypto, or the openssl CLI); ffmpeg and a SILK codec
    are optional (voice notes, video frames) and degrade gracefully -
    voice requests then go out as files."""
    import ctypes.util as _ctu
    import importlib.util
    import shutil as _sh
    if (not _sh.which("openssl")
            and importlib.util.find_spec("cryptography") is None
            and not _ctu.find_library("crypto")):
        add("deps", "FAIL",
            "no AES backend: install cryptography, libcrypto, or openssl")
        return
    vend = BASE / "vendor" / "py"
    if vend.is_dir():
        sys.path.insert(0, str(vend))
    codec = next((n for n in ("pysilk", "pilk") if importlib.util.find_spec(n)), None)
    notes = []
    if not _sh.which("ffmpeg"):
        notes.append("ffmpeg missing (voice notes, video frames)")
    if not codec:
        notes.append("no SILK codec: voice notes go out as audio files "
                     "(pip install --target vendor/py silk-python)")
    add("deps", "WARN" if notes else "PASS",
        "; ".join(notes) if notes else f"openssl, ffmpeg, SILK codec ({codec})")


def check_queue() -> None:
    """v4 outbound media queue: failed/uncertain items wait for 重发."""
    qdir = BASE / "outbox" / "queue"
    counts: dict = {}
    stuck = 0
    now = int(time.time() * 1000)
    for p in (qdir.glob("*.json") if qdir.exists() else []):
        it = load_json(p) or {}
        st = it.get("status") or "?"
        counts[st] = counts.get(st, 0) + 1
        if st == "sending" and now - (it.get("claimed_at") or now) > 600_000:
            stuck += 1
    if not counts:
        add("media_queue", "PASS", "empty")
        return
    warn = counts.get("failed") or counts.get("uncertain") or stuck
    add("media_queue", "WARN" if warn else "PASS",
        f"{counts}" + (f" stuck_sending={stuck}" if stuck else "")
        + (" (user can answer 重发)" if warn else ""))


def check_restarts() -> None:
    """2026-11-02, 四: restarts with attribution, not just a count. WARN
    only on a repeat pattern (>=8 starts in 24 h, or >=3 starts <2 min
    apart = flapping); a few attributed restarts are normal here (VM
    replacement, hook recovery) and stay PASS with the breakdown."""
    raw = load_json(STATE / "serve_starts.json") or []
    now = int(time.time() * 1000)
    starts = []
    for x in raw if isinstance(raw, list) else []:
        if isinstance(x, int):
            starts.append((x, "legacy"))
        elif isinstance(x, dict) and x.get("at"):
            starts.append((int(x["at"]), str(x.get("reason") or "unknown")))
    starts = sorted(starts)
    recent = [(at, r) for at, r in starts if at > now - 86_400_000]
    if not recent:
        add("restarts", "PASS", "no serve starts recorded in 24 h")
        return
    by_reason: dict = {}
    for _, r in recent:
        by_reason[r] = by_reason.get(r, 0) + 1
    breakdown = "、".join(f"{r} {n} 次" for r, n in sorted(by_reason.items()))
    rapid = sum(1 for (a, _), (b, _) in zip(recent, recent[1:])
                if b - a < 120_000)
    # Opt round 2 item 4: planned restarts (batch upgrades, resume work)
    # and pre-attribution legacy entries no longer trigger the warning;
    # only unplanned restarts count toward it, so a WARN means a real
    # problem instead of "we did a lot of planned work today".
    planned = sum(n for r, n in by_reason.items()
                  if r.startswith(("opt", "resume", "upgrade", "planned")))
    legacy = by_reason.get("legacy", 0)
    unplanned = len(recent) - planned - legacy
    detail = f"24 h 内启动 {len(recent)} 次：{breakdown}（计划内 {planned} 次、非计划 {unplanned} 次）"
    if rapid:
        detail += f"；间隔不到 2 分钟的快速重启 {rapid} 次"
    status = "WARN" if (unplanned >= 8 or rapid >= 3) else "PASS"
    add("restarts", status, detail)


def check_log_tail() -> None:
    log_f = LOGS / "bridge.log"
    if not log_f.exists():
        add("log_scan", "WARN", "bridge.log missing")
        return
    lines = log_f.read_text(encoding="utf-8", errors="replace").splitlines()
    recent = lines[-400:]
    pats = {
        "poll_errors": re.compile(r"getUpdates (unexpected )?error"),
        "notify_failures": re.compile(r"notify(Start|Stop) failed"),
        "stale_token_-14": re.compile(r"stale token \(-14\)"),
        "restarts": re.compile(r"serve starting"),
    }
    found = {k: sum(1 for ln in recent if p.search(ln))
             for k, p in pats.items()}
    status = "PASS"
    if found["stale_token_-14"]:
        status = "WARN"
    add("log_scan", status,
        f"last {len(recent)} log lines: {found}")



def check_reply_window() -> None:
    """v5: WeChat only delivers bot messages within 24 h of the user's last
    message; past that, alerts (send-direct) are refused until they write."""
    creds = load_json(STATE / "credentials.json") or {}
    user = creds.get("ilink_user_id")
    toks = load_json(STATE / "context_tokens.json") or {}
    entry = toks.get(user) if isinstance(toks, dict) and user else None
    at = int((entry or {}).get("at") or 0) if isinstance(entry, dict) else 0
    if not at:
        add("reply_window", "WARN", "no message from the user recorded yet: proactive "
            "messages may be refused until the user writes once")
        return
    age_h = (time.time() * 1000 - at) / 3_600_000
    if age_h >= 24:
        add("reply_window", "WARN", f"user last wrote {age_h:.1f} h ago: the 24 h window "
            "is closed - alerts cannot reach WeChat until the user sends any message "
            "(send-direct reports context_expired; report in the App instead)")
    elif age_h >= 20:
        add("reply_window", "WARN", f"user last wrote {age_h:.1f} h ago: the reply "
            f"window closes in {24 - age_h:.1f} h")
    else:
        add("reply_window", "PASS", f"user last wrote {age_h:.1f} h ago "
            f"({24 - age_h:.1f} h of reply window left)")


def check_transport() -> None:
    hb = load_json(STATE / "heartbeat.json") or {}
    mode = hb.get("transport_mode")
    if mode is None:
        add("transport", "WARN", "serve has not reported a transport mode (pre-v5 serve?)")
    elif mode == "urllib":
        add("transport", "WARN", "no connection pool (proxy scheme the pool cannot use)")
    else:
        add("transport", "PASS", f"{mode} keep-alive pool; fast connect retries "
            f"{hb.get('net_fast_retries') or 0}, resumed downloads {hb.get('net_resumed') or 0}, "
            f"DNS fallbacks {hb.get('net_dns_fallbacks') or 0}")


_STATS: dict = {}


def bridge_stats():
    """`bridge.py stats`, run once per doctor run (check_scale and
    check_latency_tail both read it); None when it cannot be read."""
    if "v" not in _STATS:
        try:
            _STATS["v"] = json.loads(run("python3", str(BASE / "bridge.py"), "stats"))
        except Exception:  # noqa: BLE001
            _STATS["v"] = None
    return _STATS["v"]


def check_scale() -> None:
    """2026-11-02 六-5: one scale line (inbox / history / archives /
    quote rows / backups / claim context chars, via bridge.py stats).
    Informational only: always PASS unless stats itself is unreadable,
    so growth never masquerades as a fault."""
    st = bridge_stats()
    if st is None:
        add("scale", "WARN", "bridge.py stats unreadable")
        return
    sc = st.get("scale") or {}
    add("scale", "PASS",
        f"inbox={sc.get('inbox_files')} files {sc.get('inbox_bytes', 0)//1024}KB, "
        f"history={sc.get('history_lines')} lines, "
        f"archives={sc.get('archive_files')} files {sc.get('archive_lines')} lines, "
        f"quotes={sc.get('quote_rows')} rows, "
        f"backups={sc.get('backup_entries')} entries {sc.get('backup_bytes', 0)//1024}KB, "
        f"claim_context~{sc.get('claim_context_chars')} chars")


def check_latency_tail() -> None:
    """2026-11-05 (Step 0): the long tail in one line - to_first_call max
    (and its task), total / exec p95 and how many samples - so an outlier
    like test 2-3's 132 s first progress is visible in doctor, not only in
    `stats`. Information only: latency is decided by the model, not a
    health signal of the bridge, so this line is always PASS and never
    turns WARN or FAIL however slow the numbers are (an unreadable stats
    is already reported by check_scale)."""
    st = bridge_stats()
    if st is None:
        add("latency_tail", "PASS", "info only: no data (bridge.py stats unreadable, see scale)")
        return
    lat = st.get("latency_ms") or {}
    tf = ((st.get("evidence") or {}).get("exec") or {}).get("to_first_call") or {}
    top = ((((st.get("evidence") or {}).get("exec") or {}).get("to_first_call_top")) or [{}])[0]

    def sec(ms):
        return "-" if ms is None else f"{ms / 1000:.1f}s"
    who = f" (task {top['task_id']})" if tf.get("max") is not None and top.get("task_id") else ""
    add("latency_tail", "PASS",
        f"info only, never a FAIL: to_first_call max {sec(tf.get('max'))}{who}, n={tf.get('n') or 0}; "
        f"total p95 {sec((lat.get('total') or {}).get('p95'))}, exec p95 "
        f"{sec((lat.get('exec') or {}).get('p95'))}, n={lat.get('n') or 0} "
        f"(stats window 72 h; one task: wb stats --task-id <id>)")


def check_pipeline() -> None:
    """2026-11-04 review heartbeat fields: messages that cannot be stored
    (the cursor is held back until they can), long polls that get no answer
    at all, and serve worker threads that stopped looping."""
    hb = load_json(STATE / "heartbeat.json") or {}
    fail, warn = [], []
    n = int(hb.get("ingest_fail_streak") or 0)
    if n >= 3:
        fail.append(f"inbound messages cannot be stored ({n} polls in a row - disk full?)")
    elif n:
        warn.append(f"inbound store failed {n}x (retrying)")
    n = int(hb.get("client_timeout_streak") or 0)
    if n >= 3:
        warn.append(f"{n} long polls in a row got no answer at all")
    ages = hb.get("threads_age_s") or {}
    stuck = sorted(k for k, v in ages.items()
                   if isinstance(v, (int, float)) and v > 1800)
    if stuck:
        warn.append("worker threads not looping for >30 min: " + ", ".join(stuck))
    status = "FAIL" if fail else ("WARN" if warn else "PASS")
    add("pipeline", status, "; ".join(fail + warn)
        or "ingest, long polls and worker threads OK")


def check_decisions() -> None:
    f = STATE / "DECISIONS.md"
    if not f.exists():
        f = BASE / "DECISIONS.md"
    try:
        n = len(f.read_text(encoding="utf-8"))
    except OSError:
        add("decisions", "PASS", "no decision list")
        return
    budget = int(os.environ.get("WECHAT_BRIDGE_DECISIONS_CHARS") or 3000)
    if n > budget:
        add("decisions", "WARN", f"decision list is {n} chars (claim budget {budget}): "
            "finished/回灌 sections are left out of claims first - move done items out")
    else:
        add("decisions", "PASS", f"{n}/{budget} chars")


def check_relay() -> None:
    """2026-11-07 长任务接力: plans stay private (state/plans 0700, files
    0600) and no relay leg sits waiting for an executor - the hook should
    wake one within a poll or two of `complete --continue`."""
    pdir = STATE / "plans"
    if not pdir.is_dir():
        add("relay", "PASS", "no long-task plans")
        return
    problems, n, waiting = [], 0, []
    try:
        if pdir.stat().st_mode & 0o077:
            problems.append("state/plans is not 0700")
    except OSError:
        pass
    now = int(time.time() * 1000)
    for f in sorted(pdir.glob("*.json")):
        n += 1
        try:
            if f.stat().st_mode & 0o077:
                problems.append(f"{f.name} is not 0600")
        except OSError:
            continue
        t = load_json(INBOX / f.name)
        if isinstance(t, dict) and t.get("relay_pending") \
                and t.get("status") == "pending_execution" \
                and now - int(t.get("pending_at") or now) > 120_000:
            waiting.append(f.stem)
    if waiting:
        problems.append(f"{len(waiting)} relay leg(s) waiting > 2 min for an executor "
                        f"(hook not waking?): {', '.join(waiting[:3])}")
    if problems:
        add("relay", "WARN", "; ".join(problems))
    else:
        add("relay", "PASS", f"{n} plan file(s), all private; no relay leg waiting")


def check_background() -> None:
    """2026-11-07 v1.4 后台长任务: queue private (state/long-tasks 0700, entries
    0600), the watch hook alive (it touches .watch_tick every run; without it
    the bridge keeps long tasks in the foreground relay), nothing queued past
    the take timeout, no running job silent past the stale limit."""
    lt = STATE / "long-tasks"
    q = lt / "queue"
    problems, counts = [], {}
    tick = lt / ".watch_tick"
    try:
        age = time.time() - tick.stat().st_mtime
        if age > 120:
            problems.append(f"watch hook last ran {int(age)} s ago (long tasks stay in the foreground relay)")
    except OSError:
        problems.append("watch hook has not run: register hooks/wechat-long-task-watch "
                        "(until then long tasks use the foreground relay)")
    for d in (lt, q):
        try:
            if d.exists() and d.stat().st_mode & 0o077:
                problems.append(f"{d.name} is not 0700")
        except OSError:
            pass
    now = int(time.time() * 1000)
    running = sum(1 for f in (q.glob("*.json") if q.is_dir() else [])
                  if (load_json(f) or {}).get("state") == "running")
    for f in sorted(q.glob("*.json")) if q.is_dir() else []:
        try:
            if f.stat().st_mode & 0o077:
                problems.append(f"{f.name} is not 0600")
        except OSError:
            continue
        e = load_json(f) or {}
        st = e.get("state") or "?"
        counts[st] = counts.get(st, 0) + 1
        # v1.7: a requeued task waits since its requeue, not its first queueing
        since = max(int(e.get("queued_at") or now), int(e.get("requeued_at") or 0))
        if st == "queued" and now - since > 11 * 60_000:
            mins = int((now - since) / 60000)      # v1.6: always shown (WARN only)
            problems.append(f"{f.stem} queued {mins} min, "
                            + (f"waiting behind {running} running task(s)" if running else "not taken"))
        if st == "running" and now - int(e.get("heartbeat_at") or now) > 31 * 60_000:
            problems.append(f"{f.stem} running but silent {int((now - int(e['heartbeat_at'])) / 60000)} min")
    if problems:
        add("background", "WARN", "; ".join(problems[:5]) + f" queue={counts}")
    else:
        add("background", "PASS", f"watch hook alive; queue={counts or 'empty'}; all private")


def check_quote_routing() -> None:
    """v1.7, information only (never FAIL): which way a quote of a bridge
    message can be routed on this account. sendmessage returns a server id only
    since protocol 2.4.9 and some accounts never echo - then the bridge's own
    messages are known by client id and text only, and a quote of the
    "已转到后台" signal routes by its quoted text (long-tasks show:
    last_quote_lookup.via = text)."""
    import sqlite3
    db = BASE / "history" / "quotes.db"
    if not db.exists():
        add("quote_routing", "PASS", "quote store empty (no message sent yet)")
        return
    since = int(time.time() * 1000) - 7 * 86_400_000
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=2)
        try:
            out, sid = conn.execute(
                "SELECT COUNT(*), SUM(CASE WHEN id NOT LIKE 'cid:%' THEN 1 ELSE 0 END) "
                "FROM msgs WHERE dir='out' AND at>=?", (since,)).fetchone()
        finally:
            conn.close()
    except sqlite3.Error as e:
        add("quote_routing", "WARN", f"quote store unreadable ({type(e).__name__})")
        return
    out, sid = int(out or 0), int(sid or 0)
    led = load_json(STATE / "echo_ledger.json") or {}
    echoed = sum(1 for r in led.values() if isinstance(r, dict) and r.get("echo_at"))
    looks = load_json(STATE / "long-tasks" / "quote_lookups.json") or []
    last = looks[-1] if isinstance(looks, list) and looks else {}
    mode = ("by server id" if sid else "by echo id" if echoed else "by quoted text only")
    add("quote_routing", "PASS",
        f"7 d: {out} bridge messages, {sid} with a server id, {echoed} echoes -> quotes route {mode}"
        + (f"; last lookup via={last.get('via')} found={last.get('found')}" if last else ""))


def main() -> int:
    check_credentials()
    check_proxy_env()
    check_unit()
    check_service()
    check_heartbeat()
    check_state_files()
    check_inbox()
    check_disk_and_logs()
    check_watchers()
    check_deps()
    check_queue()
    check_reply_window()
    check_transport()
    check_log_tail()
    check_restarts()
    check_scale()
    check_latency_tail()
    check_pipeline()
    check_decisions()
    check_relay()
    check_background()
    check_quote_routing()
    fails = sum(1 for r in results if r["status"] == "FAIL")
    warns = sum(1 for r in results if r["status"] == "WARN")
    if "--json" in sys.argv:
        print(json.dumps({"fail": fails, "warn": warns, "checks": results},
                         ensure_ascii=False, indent=2))
    else:
        for r in results:
            mark = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}[r["status"]]
            print(f"{mark} {r['check']}: {r['detail']}")
        print(f"\n{len(results)} checks: {fails} FAIL, {warns} WARN")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
