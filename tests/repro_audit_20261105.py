#!/usr/bin/env python3
"""Repro of the 2026-11-05 audit findings (Claude-WeChatBridge.md section 8,
items 1, 2 and 4): the PRE-FIX hook script and run_all.py
(tests/fixtures/before-audit-20261105/, copied unchanged from the package of
2026-11-05) next to the fixed ones. Isolated: temp dirs only, a stub hook
runtime whose wake() EXITS like the real one, stubbed systemctl/cp, no
network, no live paths. Not in run_all.py (not a test_*.py); run it on purpose:

    python3 tests/repro_audit_20261105.py            # about 5-20 s
    python3 tests/repro_audit_20261105.py --keep     # keep the temp dirs
    python3 tests/repro_audit_20261105.py --old-hook <path>   # e.g. a backup copy

Checks (each failed check prints BUG and counts):
  1 woke    old: every wake path logs woke=0 and a linger re-run that woke
            counts reruns 1 / linger_wakes 0 - the live symptom (Muse: 4769
            tick lines, none with woke=1; linger_wakes 0 after 4 re-runs);
            new: woke=1 + wake_sig on all 5 wake paths, linger_wakes = reruns
  2 window  old: after a rotation the tick log holds 2000 lines, the 24 h
            window reaches back ~2.8 h; new: 18000 lines, it reaches 24 h
  4 tmp     old run_all.py leaves its tests' temp dirs behind; new leaves none
"""
import argparse, hashlib, importlib.util, json, os, shutil, subprocess, sys, tempfile, time
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
FIX = HERE / "tests/fixtures/before-audit-20261105"
NEW_HOOK, NEW_RUN_ALL = HERE / "hooks/wechat-bridge-inbox.sh", HERE / "tests/run_all.py"
RUNTIME = r'''
hook_state_get() { [[ -f "$HOOK_STATE" ]] && cat "$HOOK_STATE" || echo '{}'; }
hook_state_set() { printf '%s' "$1" > "$HOOK_STATE"; }
log()    { echo "LOG $1" >> "$HOOK_EVENTS"; }
wake()   { echo "WAKE $1" >> "$HOOK_EVENTS"; exit 0; }
silent() { echo "SILENT $1" >> "$HOOK_EVENTS"; exit 0; }
'''
bugs, made = [], []


def check(cond, msg, detail=""):
    print(("OK    " if cond else "BUG   ") + msg + (f"\n      {detail}" if detail and not cond else ""))
    if not cond:
        bugs.append(msg)


def tmp(prefix):
    d = Path(tempfile.mkdtemp(prefix=f"wb-repro-{prefix}-"))
    made.append(d)
    return d


class Hook:
    def __init__(self, script, serve=True, creds=True):
        self.script, self.home = script, tmp("hook")
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
                {"serve_pid": self.proc.pid, "serve_started_at": now - 600_000, "last_poll_ok_at": now - 1000}))
        self.env = {**os.environ, "HOME": str(self.home), "PATH": f"{stub}:{os.environ['PATH']}",
                    "HATCH_HOOK_RUNTIME": str(self.home / "runtime.sh"),
                    "HOOK_STATE": str(self.home / "state.json"), "HOOK_EVENTS": str(self.home / "events")}

    def task(self, mid, status):
        (self.br / "inbox" / f"{mid}.json").write_text(json.dumps(
            {"message_id": str(mid), "status": status, "kind": "text", "text": "x",
             "execution": None, "send": None}, indent=2))

    def run(self):
        subprocess.run(["bash", str(self.script)], env=self.env, capture_output=True, timeout=120)

    def ticks(self):
        f = self.br / "state/hook_ticks.log"
        return [ln.split() for ln in f.read_text().splitlines()] if f.exists() else []

    def close(self):
        if self.proc:
            self.proc.kill()


def bridge():
    d = tmp("bridge")
    shutil.copy(HERE / "bridge.py", d / "bridge.py")
    spec = importlib.util.spec_from_file_location(f"bp_repro_{len(made)}", d / "bridge.py")
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    b.STATE_DIR.mkdir(parents=True, exist_ok=True)
    return b


def wake_paths(script):
    """(reason, tick line) for the 5 wake calls of the hook script."""
    out = []
    setups = [("wechat_task", {}, lambda h: h.task(101, "pending_execution")),
              ("wechat_send_uncertain", {}, lambda h: h.task(5, "send_uncertain")),
              ("wechat_auth_alert (credentials gone)", {"creds": False},
               lambda h: (h.br / "state/login_status.json").write_text('{"status": "confirmed"}')),
              ("wechat_auth_alert (stale token)", {},
               lambda h: (h.br / "state/auth_alert.json").write_text("{}")),
              ("wechat_service_unhealthy", {"serve": False},
               lambda h: (h.home / "state.json").write_text(json.dumps(
                   {"inline_recovery_since_ms": int(time.time() * 1000) - 100_000,
                    "last_inline_recovery_ms": int(time.time() * 1000), "inline_attempts": 1})))]
    for reason, kw, prep in setups:
        h = Hook(script, **kw)
        try:
            prep(h)
            h.run()
            ev = [e for e in ((h.home / "events").read_text().splitlines()
                              if (h.home / "events").exists() else []) if e.startswith("WAKE")]
            out.append((reason, ev, (h.ticks() or [[]])[-1]))
        finally:
            h.close()
    return out


def linger(script):
    h = Hook(script)
    try:
        (h.br / "state/tuning.env").write_text("WECHAT_BRIDGE_HOOK_LINGER_MS=3000\n")
        p = subprocess.Popen(["bash", str(script)], env=h.env, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        time.sleep(0.8)
        h.task(78, "pending_execution")
        (h.br / "state/.inbox_signal").write_text(str(int(time.time() * 1000)))
        p.wait(timeout=60)
        ticks = h.ticks()
    finally:
        h.close()
    b = bridge()
    b.HOOK_TICKS_FILE.write_text("\n".join(" ".join(t) for t in ticks) + "\n")
    return ticks, b.hook_tick_stats()


def retention(script):
    h = Hook(script)
    try:
        now = int(time.time() * 1000)
        t0 = now - 30_000 * 5000
        log = h.br / "state/hook_ticks.log"
        log.write_text("".join(f"{t0 + i * 5000} {t0 + i * 5000 + 80} 0 0 0 -\n" for i in range(30_000)))
        h.run()
        b = bridge()
        shutil.copy(log, b.HOOK_TICKS_FILE)
        return len(log.read_text().splitlines()), b.hook_tick_stats(hours=24)
    finally:
        h.close()


def leftovers(run_all_py):
    proj, parent = tmp("proj"), tmp("tmpdir")
    (proj / "tests").mkdir()
    shutil.copy(run_all_py, proj / "tests/run_all.py")
    for name, pre in (("test_x.py", "v3-"), ("test_y.py", "wb-test-"), ("test_z.py", "v5t-")):
        (proj / "tests" / name).write_text(
            f"import os, tempfile\nd = tempfile.mkdtemp(prefix={pre!r})\n"
            "open(os.path.join(d, 'blob'), 'wb').write(os.urandom(1_000_000))\nprint('OK')\n")
    r = subprocess.run([sys.executable, str(proj / "tests/run_all.py"), "--no-sweep"], cwd=proj,
                       env={**os.environ, "TMPDIR": str(parent)}, capture_output=True, text=True, timeout=300)
    left = [p.name for p in parent.iterdir()]
    size = sum(f.stat().st_blocks * 512 for p in parent.rglob("*") for f in [p] if p.is_file())
    return r.returncode, left, size


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="keep the temp dirs")
    ap.add_argument("--old-hook", default=str(FIX / "wechat-bridge-inbox.sh"))
    ap.add_argument("--old-run-all", default=str(FIX / "run_all.py"))
    a = ap.parse_args()
    old_hook, old_run_all = Path(a.old_hook), Path(a.old_run_all)
    for p in (old_hook, NEW_HOOK, old_run_all, NEW_RUN_ALL):
        print(f"{hashlib.sha256(p.read_bytes()).hexdigest()[:12]}  {p.relative_to(HERE) if HERE in p.parents else p}")
    t0 = time.time()
    try:
        print("\n1 woke - every wake() call (runtime stub exits like the real one)")
        for (reason, ev_o, old), (_, ev_n, new) in zip(wake_paths(old_hook), wake_paths(NEW_HOOK)):
            check(len(ev_o) == 1 and len(old) == 6 and old[2] == "0",
                  f"old {reason}: woke=0 although it woke (reproduced)", f"{ev_o} {old}")
            check(len(ev_n) == 1 and len(new) == 6 and new[2] == "1" and new[5] not in ("-", ""),
                  f"new {reason}: woke=1, wake_sig {new[5] if len(new) == 6 else '?'}", f"{ev_n} {new}")
        (to, so), (tn, sn) = linger(old_hook), linger(NEW_HOOK)
        check(so.get("reruns") == 1 and so.get("linger_wakes") == 0 and so.get("wakes") == 0,
              f"old linger: re-run woke for the message, stats reruns {so.get('reruns')} / linger_wakes "
              f"{so.get('linger_wakes')} / wakes {so.get('wakes')} (reproduced)", str(to))
        check(sn.get("reruns") == 1 and sn.get("linger_wakes") == 1 and sn.get("wakes") == 1,
              f"new linger: reruns {sn.get('reruns')} = linger_wakes {sn.get('linger_wakes')}, wakes "
              f"{sn.get('wakes')}", str(tn))
        print("\n2 window - 30000 tick lines (41.7 h, >1 MiB), then one hook run")
        (lo, so), (ln, sn) = retention(old_hook), retention(NEW_HOOK)
        check(lo == 2000 and so["span_h"] < 3,
              f"old: {lo} lines kept, the 24 h window reaches back {so['span_h']} h (reproduced)")
        check(ln == 18000 and sn["span_h"] >= 24.0 and sn["window_h"] == 24,
              f"new: {ln} lines kept, the 24 h window reaches back {sn['span_h']} h")
        print("\n4 tmp - three tiny test files, 1 MB temp data each, TMPDIR = an empty dir")
        rco, lo, szo = leftovers(old_run_all)
        rcn, ln, szn = leftovers(NEW_RUN_ALL)
        check(rco == 0 and len(lo) == 3,
              f"old run_all: {len(lo)} temp dirs ({szo / 1e6:.1f} MB) left behind (reproduced)", str(lo))
        check(rcn == 0 and ln == [], f"new run_all: {len(ln)} left ({szn / 1e6:.1f} MB)", str(ln))
    finally:
        if a.keep:
            print("\nkept:", ", ".join(str(d) for d in made))
        else:
            for d in made:
                shutil.rmtree(d, ignore_errors=True)
    print(f"\nwall {time.time() - t0:.1f} s")
    print(f"BUGS: {len(bugs)}" + (f" {bugs}" if bugs else ""))
    return 1 if bugs else 0


if __name__ == "__main__":
    sys.exit(main())
