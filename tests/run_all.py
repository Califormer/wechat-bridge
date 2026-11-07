#!/usr/bin/env python3
"""Run every test file in tests/ (each in its own process, with the
arguments it expects) and print one line per file. Exit code 1 if any
failed. Run from anywhere: python3 tests/run_all.py [--quick] [--keep-tmp] [--no-sweep]
--quick skips test_offline's benchmark. Added 2026-11-04 so that "all
green" is one reproducible command (CI and the live host run the same).

Temp dirs (2026-11-05 audit, section 8 item 4). The test files mkdtemp()
freely and never clean up: one full run left ~280 MB in /tmp, and /tmp on the
live host is a 512 MB tmpfs, so the second full run died with "No space left
on device". Only the test runner changes for this, not the bridge:
  * every test file runs with TMPDIR set to its own directory inside one
    private root, <system temp>/wb-runall-XXXXXXXX; that directory is deleted
    as soon as the file has finished, the root at the end. Peak use is one
    file's temp data and nothing is left behind. --keep-tmp keeps them (the
    root is printed) for debugging.
  * before the run, stale leftovers in the system temp dir are swept: roots
    of earlier runs whose run_all.py is gone, and directories that earlier
    runs or tests run on their own left behind - exact test prefixes
    (TEST_PREFIXES, TAGGED) plus mkdtemp's 8-character suffix, owned by this
    user, untouched for 10 minutes. Never swept: vframes-* (the bridge's own
    keyframe dirs), NOT_SWEPT, anything else. --no-sweep skips the sweep.
"""
import os, re, shutil, subprocess, sys, tempfile, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ARGS = {"test_proxy_failover.py": ["bridge.py"], "test_offline.py": ["--bench"]}
RUN_PREFIX = "wb-runall-"
MARKER = ".run_all.pid"
SWEEP_AGE_S = 600
# mkdtemp prefixes used under tests/. tests/test_audit_20261105.py fails when a
# test uses a prefix that is in none of these three lists.
TEST_PREFIXES = ("e2e4-", "hook-perf-", "hook5-", "v3-", "v3tls-", "v4t-", "v5e2e-",
                 "v5t-", "vframes-test-", "wb-aes-", "wb-aes-bad-", "wb-bench-",
                 "wb-dec-", "wb-home-", "wb-launch-", "wb-redir-", "wb-redir-tls-",
                 "wb-test-", "wb-tune-", "wbe2e6-")
TAGGED = ("wb-audit-", "wb-batch-", "wb-first-", "wb-perf-", "wb-review-")   # then "<tag>-"
# Too generic to be sure they are ours (and the tests remove most of them
# themselves), or kept on purpose (repro --keep): never swept.
NOT_SWEPT = ("hook-", "wd-", "e2e-", "wb-repro-")
_SUFFIX = r"[a-z0-9_]{8}"                    # tempfile's random part
SWEEP_RE = re.compile(r"^(?:(?:%s)|(?:%s)[A-Za-z0-9_]+-)%s$" % (
    "|".join(map(re.escape, TEST_PREFIXES)), "|".join(map(re.escape, TAGGED)), _SUFFIX))
ROOT_RE = re.compile(r"^%s%s$" % (re.escape(RUN_PREFIX), _SUFFIX))


def _du(path) -> int:
    """Bytes actually allocated (like du), not apparent size: some tests
    make large sparse files that take no room on the tmpfs."""
    total, seen = 0, set()
    for dp, _dn, fn in os.walk(path, onerror=lambda e: None):
        for f in fn:
            try:
                st = os.lstat(os.path.join(dp, f))
            except OSError:
                continue
            if (st.st_dev, st.st_ino) not in seen:      # count hard links once
                seen.add((st.st_dev, st.st_ino))
                total += st.st_blocks * 512
    return total


def _run_all_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        return b"run_all" in Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return True                          # cannot tell: leave it alone


def sweep(tmp_parent, now=None):
    """Remove stale test temp dirs directly under tmp_parent. Returns
    (names removed, bytes freed)."""
    now = time.time() if now is None else now
    gone, freed = [], 0
    try:
        entries = list(os.scandir(tmp_parent))
    except OSError:
        return gone, freed
    for e in entries:
        is_root = bool(ROOT_RE.match(e.name))
        if not (is_root or SWEEP_RE.match(e.name)):
            continue
        try:
            st = e.stat(follow_symlinks=False)
        except OSError:
            continue
        if not e.is_dir(follow_symlinks=False) or st.st_uid != os.getuid():
            continue
        if is_root:
            try:
                pid = int((Path(e.path) / MARKER).read_text().split()[0])
            except (OSError, ValueError, IndexError):
                pid = 0
            if (pid and _run_all_alive(pid)) or (not pid and now - st.st_mtime < SWEEP_AGE_S):
                continue
        elif now - st.st_mtime < SWEEP_AGE_S:
            continue
        freed += _du(e.path)
        shutil.rmtree(e.path, ignore_errors=True)
        gone.append(e.name)
    return gone, freed


def main(argv=None, root=ROOT, tmp_parent=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    args = dict(ARGS)
    if "--quick" in argv:
        args["test_offline.py"] = []
    keep = "--keep-tmp" in argv
    tmp_parent = Path(tmp_parent or tempfile.gettempdir())
    if "--no-sweep" not in argv:
        gone, freed = sweep(tmp_parent)
        if gone:
            print(f"sweep: removed {len(gone)} stale test temp dirs ({freed / 1e6:.1f} MB) "
                  f"from {tmp_parent}", flush=True)
    run_root = Path(tempfile.mkdtemp(prefix=RUN_PREFIX, dir=tmp_parent))
    (run_root / MARKER).write_text(f"{os.getpid()}\n")
    failed, peak = [], (0, "-")
    try:
        for i, t in enumerate(sorted((Path(root) / "tests").glob("test_*.py"))):
            tdir = Path(tempfile.mkdtemp(prefix=f"{i:02d}-", dir=run_root))
            t0 = time.time()
            try:
                r = subprocess.run([sys.executable, str(t), *args.get(t.name, [])], cwd=root,
                                   env={**os.environ, "TMPDIR": str(tdir)},
                                   capture_output=True, text=True, timeout=900)
            finally:
                peak = max(peak, (_du(tdir), t.name))
                if not keep:
                    shutil.rmtree(tdir, ignore_errors=True)
            lines = [ln for ln in (r.stdout + r.stderr).splitlines() if ln.strip()]
            tail = lines[-1][:100] if lines else ""
            print(f"{'ok  ' if r.returncode == 0 else 'FAIL'} {t.name:42} {time.time() - t0:5.1f}s  {tail}",
                  flush=True)
            if r.returncode:
                failed.append(t.name)
                for ln in lines:
                    if ln.startswith(("FAIL", "Traceback")) or "Error" in ln:
                        print("       " + ln[:160])
    finally:
        if not keep:
            shutil.rmtree(run_root, ignore_errors=True)
    print("temp: every file had its own TMPDIR, "
          + (f"kept under {run_root}" if keep else "removed right after it")
          + f"; largest {peak[0] / 1e6:.1f} MB ({peak[1]})")
    print(f"\n{'ALL TEST FILES PASSED' if not failed else 'FAILED: ' + ', '.join(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
