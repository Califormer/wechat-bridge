#!/usr/bin/env python3
"""Tail batch tests (2026-11-02): --prune aligned with the upload-first
rule, snapshot timestamps unified to America/Caracas, UTC-stamped
snapshots renamed. Run: python3 tests/test_tail_20261102.py"""
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
fails = []


def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    if not cond:
        fails.append(name)


sh = (HERE / "ops" / "backup.sh").read_text()
check("backup.sh forces Caracas timestamps", 'SNAP_TZ="America/Caracas"' in sh
      and "snap_date" in sh)
check("backup.sh prune keeps state/", "state/ is always kept" in sh
      and '! -path "$BACKUPS/$d/state/*"' in sh
      and "rm -rf" not in sh)
check("backup.sh prune-list excludes state-only shells",
      "state-only" in sh and "prune_candidates" in sh)
# 2026-11-04 review: live-deployment checks only where backups/ exists
# (the open-source package and its CI have none).
if (HERE / "backups").is_dir():
    check("no UTC-stamped dated snapshots remain",
          not any(p.name.startswith("20261103-") for p in (HERE / "backups").iterdir()
                  if p.is_dir() and p.name[0].isdigit()))
    # 2026-11-06 (机主定): the three renamed snapshots were deleted with the
    # 96 excess dated snapshots after the desensitised package went to iCloud
    # (icloud-upload/wechat-bridge-backups-excess-20261106.zip, 11041847
    # bytes, sha256 0cffd605...). The old "they exist" check is obsolete; the
    # replacement guards the same rule: they must be gone *because* the
    # upload exists, not silently lost.
    check("renamed snapshots removed after iCloud upload",
          not any((HERE / "backups" / n).exists() for n in
                  ("20261102-222244", "20261102-212241", "20261102-202236"))
          and (HERE / "icloud-upload"
               / "wechat-bridge-backups-excess-20261106.zip").is_file())
else:
    print("SKIP  live-deployment checks (no backups/ here: not the live bridge)")
v4 = (HERE / "tests" / "test_v4.py").read_text()
check("test_v4 native voice behind opt-in",
      'WECHAT_BRIDGE_NATIVE_VOICE"] = "1"' in v4)
check("test_v4 checks default file-card voice",
      "default voice goes out as a file card" in v4)
check("test_v4 old point in archive",
      'archive-old-versions" / "bridge.py.v3"' in v4)
ops = (HERE / "OPS.md").read_text()
check("OPS tail section", "收尾三件小事" in ops)

print("ALL TAIL CHECKS PASSED" if not fails else f"FAILURES: {fails}")
raise SystemExit(1 if fails else 0)
