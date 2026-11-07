#!/usr/bin/env python3
"""2026-11-05 audit fixes (Claude-WeChatBridge.md section 8, items 1-5).
Offline, stdlib only; nothing here reaches iLink or the host's services.

1 woke: the hook runtime's wake() EXITS, so every wake path must log woke=1
  and its wake_sig before calling it (run under a runtime stub that exits,
  like the real one); linger re-run wakes count in stats (reruns, linger_wakes)
2 retention: the hook keeps 25 h of tick lines, stats reads them all and caps
  its window at 24 h (window_h / span_h); hourly backups skip the tick log
3 deploy: the JSON definition keeps delivery "main" (the live one is
  side_chat - never mix) and its prompt equals prompt.txt / the sha256 record
4 temp dirs: run_all.py gives every test file its own TMPDIR and removes it;
  the sweep removes only stale test dirs (never vframes-*, tmp*, fresh ones)
5 wording: EXECUTOR.md and the prompt say "代码默认 45 秒、线上 90 秒", with
  the live value taken from state/tuning.env (else TUNING.md)
Run: python3 tests/test_audit_20261105.py
"""
import contextlib, hashlib, importlib.util, io, json, os, re, shutil, subprocess, sys
import tempfile, time
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
HOOK = HERE / "hooks/wechat-bridge-inbox.sh"
sys.path.insert(0, str(HERE / "tests"))
import run_all  # noqa: E402

fails = []
_n = [0]


def check(cond, name, detail=""):
    print(("PASS  " if cond else "FAIL  ") + name + (f" ({detail})" if detail and not cond else ""))
    if not cond:
        fails.append(name)


# The real runtime's wake() and silent() both end the script.
RUNTIME = r'''
hook_state_get() { [[ -f "$HOOK_STATE" ]] && cat "$HOOK_STATE" || echo '{}'; }
hook_state_set() { printf '%s' "$1" > "$HOOK_STATE"; }
log()    { echo "LOG $1" >> "$HOOK_EVENTS"; }
wake()   { echo "WAKE $1" >> "$HOOK_EVENTS"; exit 0; }
silent() { echo "SILENT $1" >> "$HOOK_EVENTS"; exit 0; }
'''


class Hook:
    def __init__(self, serve=True, creds=True):
        self.home = Path(tempfile.mkdtemp(prefix="wb-audit-hook-"))
        self.br = self.home / "workspace/wechat-bridge"
        for sub in ("inbox", "state", "ops"):
            (self.br / sub).mkdir(parents=True)
        if creds:
            (self.br / "state/credentials.json").write_text("{}")
        (self.br / "ops/backup.sh").write_text("#!/usr/bin/env bash\n")
        (self.br / "ops/backup.sh").chmod(0o755)
        stub = self.home / "stub"
        stub.mkdir()
        if not shutil.which("jq"):
            (stub / "jq").write_text(f"#!/bin/sh\nexec {sys.executable} {HERE}/tests/jqshim.py \"$@\"\n")
            (stub / "jq").chmod(0o755)
        for name in ("systemctl", "cp"):
            (stub / name).write_text("#!/bin/sh\nexit 0\n")
            (stub / name).chmod(0o755)
        (self.home / "runtime.sh").write_text(RUNTIME)
        self.proc = None
        if serve:
            self.proc = subprocess.Popen(["bash", "-c", "exec -a 'python3 bridge.py serve' sleep 300"])
            (self.br / "state/serve.pid").write_text(str(self.proc.pid))
            now = int(time.time() * 1000)
            (self.br / "state/heartbeat.json").write_text(json.dumps(
                {"serve_pid": self.proc.pid, "serve_started_at": now - 600_000,
                 "last_poll_ok_at": now - 1000}))
        self.env = {**os.environ, "HOME": str(self.home), "PATH": f"{stub}:{os.environ['PATH']}",
                    "HATCH_HOOK_RUNTIME": str(self.home / "runtime.sh"),
                    "HOOK_STATE": str(self.home / "state.json"), "HOOK_EVENTS": str(self.home / "events")}

    def task(self, mid, status, **kw):
        t = {"message_id": str(mid), "status": status, "kind": "text", "text": "x",
             "execution": None, "send": None}
        t.update(kw)
        (self.br / "inbox" / f"{mid}.json").write_text(json.dumps(t, indent=2))

    def run(self, script=HOOK):
        subprocess.run(["bash", str(script)], env=self.env, capture_output=True, timeout=120)

    def events(self):
        f = self.home / "events"
        return [e for e in (f.read_text().splitlines() if f.exists() else []) if not e.startswith("LOG")]

    def ticks(self):
        f = self.br / "state/hook_ticks.log"
        return [ln.split() for ln in f.read_text().splitlines()] if f.exists() else []

    def close(self):
        if self.proc:
            self.proc.kill()
        shutil.rmtree(self.home, ignore_errors=True)


def cksum(s: str) -> str:
    return subprocess.run(["cksum"], input=s.encode(), capture_output=True).stdout.split()[0].decode()


_made = []


def load(tag):
    _n[0] += 1
    d = Path(tempfile.mkdtemp(prefix=f"wb-audit-{tag}-"))
    _made.append(d)
    shutil.copy(HERE / "bridge.py", d / "bridge.py")
    spec = importlib.util.spec_from_file_location(f"bp_audit_{tag}_{_n[0]}", d / "bridge.py")
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    b.log = lambda msg: None
    for p in (b.STATE_DIR, b.INBOX_DIR, b.HISTORY_DIR):
        p.mkdir(parents=True, exist_ok=True)
    return b, d


# ------------------------------------------------------------------ 1 woke
def t_every_wake_logs_woke():
    cases = []
    h = Hook()
    try:
        h.task(101, "pending_execution")
        h.run()
        cases.append(("wechat_task", h.events(), h.ticks()[-1], cksum('["101"]')))
    finally:
        h.close()
    h = Hook()
    try:
        h.task(5, "send_uncertain")
        h.run()
        cases.append(("wechat_send_uncertain", h.events(), h.ticks()[-1], re.compile(r"u\d+")))
    finally:
        h.close()
    h = Hook(creds=False)
    try:
        (h.br / "state/login_status.json").write_text('{"status": "confirmed"}')
        h.run()
        cases.append(("wechat_auth_alert", h.events(), h.ticks()[-1], "auth"))
    finally:
        h.close()
    h = Hook()
    try:
        (h.br / "state/auth_alert.json").write_text("{}")
        h.run()
        cases.append(("wechat_auth_alert", h.events(), h.ticks()[-1], "auth"))
    finally:
        h.close()
    h = Hook(serve=False)
    try:
        now = int(time.time() * 1000)
        (h.home / "state.json").write_text(json.dumps({"inline_recovery_since_ms": now - 100_000,
                                                       "last_inline_recovery_ms": now,
                                                       "inline_attempts": 1}))
        h.run()
        cases.append(("wechat_service_unhealthy", h.events(), h.ticks()[-1], "svc"))
    finally:
        h.close()
    for reason, ev, tick, sig in cases:
        ok_sig = sig.fullmatch(tick[5]) if isinstance(sig, re.Pattern) else tick[5] == sig
        shown = sig.pattern if isinstance(sig, re.Pattern) else sig
        check(ev == [f"WAKE {reason}"] and len(tick) == 6 and tick[2] == "1" and bool(ok_sig),
              f"wake {reason}: the tick line has woke=1 and wake_sig {shown} although wake() exits",
              f"{ev} {tick}")
    h = Hook()
    try:
        h.run()
        tick = h.ticks()[-1]
        check(h.events() == ["SILENT wechat bridge healthy"] and tick[2] == "0" and tick[5] == "-",
              "control: a healthy run without a wake still logs woke=0, sig -", str(tick))
    finally:
        h.close()


def t_linger_rerun_wake_counted():
    h = Hook()
    try:
        (h.br / "state/tuning.env").write_text("WECHAT_BRIDGE_HOOK_LINGER_MS=3000\n")
        p = subprocess.Popen(["bash", str(HOOK)], env=h.env, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        time.sleep(0.8)
        h.task(78, "pending_execution")
        (h.br / "state/.inbox_signal").write_text(str(int(time.time() * 1000)))
        p.wait(timeout=60)
        ticks = h.ticks()
        check(len(ticks) == 2 and ticks[0][2:5] == ["0", "3000", "0"]
              and ticks[1][2] == "1" and ticks[1][4] == "1" and ticks[1][5] == cksum('["78"]'),
              "linger: the run that saw the message logs linger 3000, its re-run woke=1 rerun=1 with the "
              "task's sig", str(ticks))
        b, _ = load("rerun")
        b.HOOK_TICKS_FILE.write_text("\n".join(" ".join(t) for t in ticks) + "\n")
        s = b.hook_tick_stats()
        check(s["reruns"] == 1 and s["linger_wakes"] == 1 and s["wakes"] == 1 and s["duplicate_wakes"] == 0,
              "stats: reruns 1 = linger_wakes 1, wakes 1, no duplicate (the online check of item 1)", str(s))
    finally:
        h.close()


def t_shims_and_script():
    lines = HOOK.read_text().splitlines()
    calls = [i for i, ln in enumerate(lines) if re.match(r'\s*wake\s+"', ln)]
    check(len(calls) == 5 and all(lines[i - 1].strip().startswith("woke=1; wake_sig=") for i in calls),
          "hook script: all 5 wake calls are directly preceded by woke=1; wake_sig=...")
    bad = []
    for f in ("test_hook.py", "test_v5.py", "test_perf_20261105.py"):
        m = re.search(r"^wake\(\)\s*\{(.*)\}\s*$", (HERE / "tests" / f).read_text(), re.M)
        if not m or "exit 0" not in m.group(1):
            bad.append(f)
    check(not bad, "the other hook tests' runtime stubs exit on wake() like the real runtime", str(bad))


# ------------------------------------------------------------------ 2 retention
def t_retention_and_window():
    h = Hook()
    try:
        now = int(time.time() * 1000)
        t0 = now - 30_000 * 5000                          # 30000 runs, ~41.7 h back
        log = h.br / "state/hook_ticks.log"
        log.write_text("".join(f"{t0 + i * 5000} {t0 + i * 5000 + 80} 0 0 0 -\n" for i in range(30_000)))
        size_before = log.stat().st_size
        h.run()
        kept = log.read_text().splitlines()
        check(size_before > 1_048_576 and len(kept) == 18_000 and int(kept[-1].split()[0]) >= now - 60_000,
              "hook: at 1 MiB the tick log keeps the last 18000 lines (25 h), the newest run included",
              f"{size_before} B -> {len(kept)} lines")
        b, _ = load("window")
        shutil.copy(log, b.HOOK_TICKS_FILE)
        s = b.hook_tick_stats(hours=72)
        check(s["window_h"] == 24 and s["span_h"] >= 24.0 and s["n"] >= 17_200,
              "stats: a 72 h request is capped at 24 h and the lines really reach 24 h back "
              "(was 2000 lines = 2.8 h)", json.dumps({k: s.get(k) for k in ("n", "window_h", "span_h")}))
    finally:
        h.close()
    b, _ = load("cli")
    now = b.now_ms()
    b.HOOK_TICKS_FILE.write_text("".join(f"{now - i * 5000} {now - i * 5000 + 80} 0 0 0 -\n"
                                         for i in range(20_000, 0, -1)))
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = b.cmd_stats(SimpleNamespace(hours=72))
    ht = json.loads(buf.getvalue())["evidence"]["hook_ticks"]
    check(rc == 0 and ht["n"] >= 17_200 and ht["window_h"] == 24 and "reruns" in ht and "span_h" in ht,
          "`stats` evidence.hook_ticks reads past the old 5000-line cap and shows window_h/span_h/reruns",
          json.dumps({k: ht.get(k) for k in ("n", "window_h", "span_h", "reruns")}))


def t_backup_skips_tick_log():
    d = Path(tempfile.mkdtemp(prefix="wb-audit-bk-"))
    try:
        (d / "state").mkdir()
        (d / "state/credentials.json").write_text('{"bot_token": "fake"}')
        (d / "state/hook_ticks.log").write_text("1 2 0 0 0 -\n" * 1000)
        (d / "state/hook_ticks.log.tmp").write_text("x\n")
        (d / "bridge.py").write_text("# fake\n")
        (d / "wechat-bridge.service").write_text("[Unit]\n")
        env = {**os.environ, "BRIDGE_DIR": str(d), "HOOKS_DIR": str(d / "nohooks"),
               "WECHAT_BRIDGE_CRON_SRC": str(d / "nocron.md")}
        r = subprocess.run(["bash", str(HERE / "ops/backup.sh")], env=env, capture_output=True,
                           text=True, timeout=120)
        snaps = [p for p in (d / "backups").iterdir() if p.is_dir()] if (d / "backups").exists() else []
        st = snaps[0] / "state" if len(snaps) == 1 else None
        check(r.returncode == 0 and st is not None and (st / "credentials.json").exists()
              and not (st / "hook_ticks.log").exists() and not (st / "hook_ticks.log.tmp").exists(),
              "ops/backup.sh: the snapshot keeps state/ but not the hook tick log (diagnostic, changes "
              "every 5 s)", (r.stdout + r.stderr)[-300:])
    finally:
        shutil.rmtree(d, ignore_errors=True)


# ------------------------------------------------------------------ 3 deploy
def t_definition_file():
    hook = json.loads((HERE / "hooks/wechat-bridge-inbox.json").read_text(encoding="utf-8"))
    txt = (HERE / "hooks/wechat-bridge-inbox.prompt.txt").read_text(encoding="utf-8")
    rec = (HERE / "hooks/wechat-bridge-inbox.prompt.sha256").read_text(encoding="utf-8").split()
    check(hook.get("delivery") == {"surface": "main"},
          "hooks/*.json keeps delivery main (the live definition is side_chat: never copy one into the other)",
          str(hook.get("delivery")))
    check(hook.get("prompt") == txt and rec[0] == hashlib.sha256(txt.encode()).hexdigest()
          and int(rec[1]) == len(txt),
          "JSON prompt = prompt.txt = sha256 record (the file the Step 2 deploy left stale)")


# ------------------------------------------------------------------ 4 temp dirs
def t_run_all_isolates_and_cleans():
    parent = Path(tempfile.mkdtemp(prefix="wb-audit-tmp-"))
    proj = Path(tempfile.mkdtemp(prefix="wb-audit-proj-"))
    try:
        (proj / "tests").mkdir()
        (proj / "tests/test_a.py").write_text(
            "import tempfile, os\nd = tempfile.mkdtemp(prefix='wb-test-')\n"
            "open(os.path.join(d, 'big'), 'wb').write(os.urandom(3_000_000))\n"
            "open('tmpdir.txt', 'w').write(tempfile.gettempdir())\nprint('A OK')\n")
        (proj / "tests/test_b.py").write_text(
            "import tempfile, sys\ntempfile.mkdtemp(prefix='v3-')\nprint('FAIL  b')\nsys.exit(1)\n")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = run_all.main(["--no-sweep"], root=proj, tmp_parent=parent)
        out = buf.getvalue().splitlines()
        tmp_line = next((ln for ln in out if ln.startswith("ok   test_a.py")), "")
        check(rc == 1 and tmp_line.endswith("A OK") and any(ln.startswith("FAIL test_b.py") for ln in out)
              and out[-1] == "FAILED: test_b.py",
              "run_all: same per-file lines, exit 1 and the FAILED line as before", "\n".join(out))
        seen = (proj / "tmpdir.txt").read_text()
        check(seen.startswith(str(parent / run_all.RUN_PREFIX)) and seen != str(parent),
              "run_all: a test file's tempfile.gettempdir() is its own dir inside the run root", seen)
        check(list(parent.iterdir()) == [] and any(ln.startswith("temp: ") and "largest 3.0 MB (test_a.py)"
                                                   in ln for ln in out),
              "run_all: each file's temp dirs are gone right after the run (nothing left in the temp dir)",
              f"{[p.name for p in parent.iterdir()]} {out[-3:]}")
    finally:
        shutil.rmtree(parent, ignore_errors=True)
        shutil.rmtree(proj, ignore_errors=True)


def t_sweep_only_stale_test_dirs():
    parent = Path(tempfile.mkdtemp(prefix="wb-audit-sweep-"))
    try:
        old = time.time() - 3600
        names = {"v3-abcdefgh": True, "wb-perf-gaps-abcd1234": True, "v4t-zz_9yy8x": True,
                 "vframes-abcdefgh": False, "tmpabcdefgh": False, "hook-abcdefgh": False,
                 "wb-repro-old-abcdefgh": False, "v3-abcdefg": False, "notours-abcdefgh": False,
                 "wb-runall-deadpid0": True, "wb-runall-notours0": True, "wb-runall-liverun0": False}
        for n in names:
            (parent / n).mkdir()
            (parent / n / "f").write_text("x")
        (parent / "wb-runall-deadpid0" / run_all.MARKER).write_text("999999999\n")
        (parent / "wb-runall-notours0" / run_all.MARKER).write_text(f"{os.getpid()}\n")  # pid reused
        live = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", "run_all.py"])
        (parent / "wb-runall-liverun0" / run_all.MARKER).write_text(f"{live.pid}\n")
        (parent / "v3-filexxxx").write_text("a file, not a dir")
        os.symlink(parent / "notours-abcdefgh", parent / "v3-symlinkx")
        for p in parent.iterdir():
            os.utime(p, (old, old), follow_symlinks=False)
        (parent / "v5t-freshdir").mkdir()                   # a test that is running right now
        gone, _ = run_all.sweep(parent)
        live.kill()
        want = sorted(n for n, rm in names.items() if rm)
        check(sorted(gone) == want and (parent / "wb-runall-liverun0").exists()
              and (parent / "vframes-abcdefgh").exists() and (parent / "tmpabcdefgh").exists()
              and (parent / "v5t-freshdir").exists() and (parent / "v3-filexxxx").exists()
              and (parent / "v3-symlinkx").is_symlink(),
              "sweep: removes stale test dirs and roots of runs that are gone (dead or reused pid) only - "
              "never a live run, vframes-*, tmp*, hook-*, repro, fresh dirs, files or symlinks",
              f"gone={sorted(gone)}")
    finally:
        shutil.rmtree(parent, ignore_errors=True)


def t_prefixes_covered():
    """Every mkdtemp call under tests/ that lands in the system temp dir has
    a prefix the sweep matches (run_all's real regex) or one listed as
    deliberately not swept."""
    uncovered, bare = set(), []
    for f in sorted((HERE / "tests").glob("*.py")):
        for m in re.finditer(r"tempfile\.mkdtemp\(([^)]*)\)", f.read_text(encoding="utf-8")):
            args = m.group(1)
            if "dir=" in args:                       # not in the system temp dir
                continue
            if not re.search(r"\bprefix\s*=", args):
                bare.append(f.name)
                continue
            pm = re.search(r"prefix\s*=\s*f?[\"']([^\"']*)[\"']", args)
            if not pm:                               # prefix from a variable/template:
                continue                             # not checkable statically
            pre = re.sub(r"\{[^}]*\}", "tag", pm.group(1))   # wb-perf-{tag}- -> wb-perf-tag-
            if not (run_all.SWEEP_RE.match(pre + "abcd_123")
                    or any(pm.group(1).startswith(x) for x in run_all.NOT_SWEPT)):
                uncovered.add(pm.group(1))
    check(not uncovered, "every test mkdtemp prefix is either swept or deliberately not (run_all lists)",
          str(sorted(uncovered)))
    check(not bare, "no test makes an unprefixed temp dir in the system temp dir (unsweepable tmp*)",
          str(bare))
    check("vframes-" not in run_all.TEST_PREFIXES and not run_all.SWEEP_RE.match("vframes-abcdefgh"),
          "the bridge's own vframes-* dirs are never swept")


# ------------------------------------------------------------------ 5 wording
def _live_warm_wait():
    env = HERE / "state/tuning.env"
    src = env.read_text(encoding="utf-8") if env.exists() else (
        (HERE / "TUNING.md").read_text(encoding="utf-8") if (HERE / "TUNING.md").exists() else "")
    m = re.findall(r"^\s*WECHAT_BRIDGE_WARM_WAIT_S\s*=\s*(\d+)", src, re.M)
    return m[-1] if m else None


def t_warm_wait_wording():
    core = (HERE / "EXECUTOR.md").read_text(encoding="utf-8")
    pr = (HERE / "hooks/wechat-bridge-inbox.prompt.txt").read_text(encoding="utf-8")
    live = _live_warm_wait()
    for name, s in (("EXECUTOR.md", core), ("hook prompt", pr)):
        bare = [s[max(0, m.start() - 2):m.end()] for m in re.finditer("默认 45 秒", s)
                if s[max(0, m.start() - 2):m.start()] != "代码"]
        check("代码默认 45 秒" in s and not bare and (live is None or f"线上 {live} 秒" in s),
              f"{name}: warm wait reads 代码默认 45 秒、线上 {live} 秒 (no bare 默认 45 秒)", str(bare))
    check(len(core.encode()) < 8000, "EXECUTOR.md still under 8000 bytes", f"{len(core.encode())}")


for fn in (t_every_wake_logs_woke, t_linger_rerun_wake_counted, t_shims_and_script,
           t_retention_and_window, t_backup_skips_tick_log, t_definition_file,
           t_run_all_isolates_and_cleans, t_sweep_only_stale_test_dirs, t_prefixes_covered,
           t_warm_wait_wording):
    try:
        fn()
    except Exception as e:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        check(False, f"{fn.__name__} raised {type(e).__name__}: {e}")

for _d in _made:
    shutil.rmtree(_d, ignore_errors=True)
print("\nALL AUDIT CHECKS PASSED" if not fails else f"\n{len(fails)} FAILED: {fails}")
sys.exit(1 if fails else 0)
