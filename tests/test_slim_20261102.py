#!/usr/bin/env python3
"""Slim batch tests (2026-11-02): total-snapshot dedup, old-version
archive, snapshot transient exclusions, hard-link dedup in backup.sh,
and the orphan-media prune function. Run: python3 tests/test_slim_20261102.py"""
import glob
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
fails = []


def check(name, cond):
    print(("PASS " if cond else "FAIL ") + name)
    if not cond:
        fails.append(name)


# 2026-11-04 review: checks of the LIVE deployment's backups/ and rollback
# files only run where they exist. In the open-source package and its CI
# there is no backups/ (and archive-old-versions/ is gitignored), so these
# used to fail on every clean checkout.
LIVE = (HERE / "backups").is_dir()
if not LIVE:
    print("SKIP  live-deployment checks (no backups/ here: not the live bridge)")

# 1. Only one total snapshot remains.
if LIVE:
    totals = glob.glob(str(HERE / "backups" / ".total-before-prune-*.tar.gz"))
    check("only one total snapshot", len(totals) == 1)

# 2. v1/v3 archived out of the project root; v2/v4 remain as rollback points.
if (HERE / "archive-old-versions").is_dir():
    check("v1 archived", (HERE / "archive-old-versions" / "bridge.py.v1").exists())
    check("v3 archived", (HERE / "archive-old-versions" / "bridge.py.v3").exists())
check("v1 not in root", not (HERE / "bridge.py.v1").exists())
check("v3 not in root", not (HERE / "bridge.py.v3").exists())
if LIVE:
    check("v2 kept", (HERE / "bridge.py.v2").exists())
    check("v4 kept", (HERE / "bridge.py.v4").exists())

# 3. backup.sh excludes transient state files and hard-links bridge.py.
sh = (HERE / "ops" / "backup.sh").read_text()
check("backup.sh excludes lock/pid/scan", "*.lock" in sh and "serve.pid" in sh
      and ".hook_full_scan" in sh)
check("backup.sh hard-links bridge.py", 'ln "$BACKUPS/$PREV/bridge.py"' in sh
      and "cmp -s" in sh)

# 4. No transient files in the cleaned state-only shells.
for d in ("20261102-192215", "20261102-202219", "20261102-203729",
          "20261102-203901", "20261102-212224", "20261102-213157"):
    st = HERE / "backups" / d / "state"
    if st.exists():
        names = {p.name for p in st.iterdir()}
        check(f"{d} state has no transient files",
              not any(n.endswith(".lock") or n in ("serve.pid",
              ".hook_full_scan", ".hook_scan_mark") for n in names))
        check(f"{d} state keeps credentials+cursor",
              "credentials.json" in names and "cursor.json" in names)

# 5. Orphan-media prune exists and is called from the hourly prune block.
src = (HERE / "bridge.py").read_text()
check("prune_orphan_media defined", "def prune_orphan_media" in src)
check("prune_orphan_media called hourly",
      "prune_orphan_media()" in src)
check("orphan sweep limited to MEDIA_DIR, not FILES_DIR",
      "for f in entries" in src and "MEDIA_DIR.iterdir" in src)

# 6. OPS slim section exists.
ops = (HERE / "OPS.md").read_text()
check("OPS slim section", "瘦身（2026-11-02" in ops)

print("ALL SLIM CHECKS PASSED" if not fails else f"FAILURES: {fails}")
raise SystemExit(1 if fails else 0)
