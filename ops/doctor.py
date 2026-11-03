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
# Optional platform integration (Muse hourly cron definition). Set
# WECHAT_BRIDGE_CRON_DEF to a definition file to check it; unset = skip.
_cron_env = os.environ.get("WECHAT_BRIDGE_CRON_DEF")
CRON_DEF = Path(_cron_env) if _cron_env else None

results = []


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
        add("proxy_env", "FAIL", "state/proxy.env missing - serve cannot egress")
        return
    if (env_f.stat().st_mode & 0o777) != 0o600:
        add("proxy_env", "WARN", f"permissions {mode_str(env_f)}, want 0600")
    file_vars = {}
    for line in env_f.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("export "):
            line = line[7:]
        if "=" in line:
            k, v = line.split("=", 1)
            file_vars[k.strip()] = v.strip().strip('"').strip("'")
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
    if CRON_DEF is None:
        add("hourly_cron", "WARN",
            "hourly cron not configured (optional platform integration)")
    elif CRON_DEF.exists():
        add("hourly_cron", "PASS", "health cron definition present")
    else:
        add("hourly_cron", "WARN",
            "configured health cron definition file not found")


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


def check_scale() -> None:
    """2026-11-02 六-5: one scale line (inbox / history / archives /
    quote rows / backups / claim context chars, via bridge.py stats).
    Informational only: always PASS unless stats itself is unreadable,
    so growth never masquerades as a fault."""
    out = run("python3", str(BASE / "bridge.py"), "stats")
    try:
        sc = json.loads(out).get("scale") or {}
    except Exception:
        add("scale", "WARN", "bridge.py stats unreadable")
        return
    add("scale", "PASS",
        f"inbox={sc.get('inbox_files')} files {sc.get('inbox_bytes', 0)//1024}KB, "
        f"history={sc.get('history_lines')} lines, "
        f"archives={sc.get('archive_files')} files {sc.get('archive_lines')} lines, "
        f"quotes={sc.get('quote_rows')} rows, "
        f"backups={sc.get('backup_entries')} entries {sc.get('backup_bytes', 0)//1024}KB, "
        f"claim_context~{sc.get('claim_context_chars')} chars")


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
