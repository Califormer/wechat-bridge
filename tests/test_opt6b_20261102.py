#!/usr/bin/env python3
"""2026-11-02 六-4/5 continuation tests (offline):
4. backup retention rule is documented, not auto-deleting (see reply list)
5. stats gains a scale line; doctor gains a scale check
Run: python3 tests/test_opt6b_20261102.py"""
import io, json, sys
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_v3 as h

NEW = HERE / "bridge.py"
fails = []

def check(cond, msg, detail=""):
    print(("PASS  " if cond else "FAIL  ") + msg + (f"\n      {detail}" if detail else ""))
    if not cond:
        fails.append(msg)

b = h.load(NEW, "opt6b")
h.set_creds(b, token="<redacted>")
# fake scale inputs inside the temp copy's dirs
(b.INBOX_DIR).mkdir(parents=True, exist_ok=True)
(b.INBOX_DIR / "a.json").write_text("{}")
(b.INBOX_DIR / "b.json").write_text("{}")
(b.HISTORY_DIR).mkdir(parents=True, exist_ok=True)
(b.HISTORY_FILE).write_text('{"at": 1, "text": "x"}\n{"at": 2, "text": "y"}\n')
(b.HISTORY_DIR / "archive-2026-11.jsonl").write_text('{"at": 3}\n')
backups = Path(b.__file__).resolve().parent / "backups"
backups.mkdir(parents=True, exist_ok=True)
(backups / "dummy").mkdir(exist_ok=True)
(backups / "dummy" / "f.txt").write_text("12345")
sc = b.scale_stats()
check(sc["inbox_files"] == 2, "scale counts inbox json files", str(sc))
check(sc["history_lines"] == 2, "scale counts live history lines", str(sc))
check(sc["archive_files"] == 1 and sc["archive_lines"] == 1,
      "scale counts archive files/lines", str(sc))
check(sc["backup_entries"] >= 1 and sc["backup_bytes"] >= 5,
      "scale counts backup entries/bytes", str(sc))
check(isinstance(sc["claim_context_chars"], int) and sc["claim_context_chars"] >= 0,
      "scale reports claim context chars (proxy, not billing)", str(sc))
check("quote_rows" in sc, "scale includes quote rows", str(sc))
# cmd_stats output carries scale
buf = io.StringIO()
with redirect_stdout(buf):
    rc = b.cmd_stats(SimpleNamespace(hours=72))
out = json.loads(buf.getvalue())
check(rc == 0 and "scale" in out and out["scale"]["inbox_files"] == 2,
      "stats output includes scale", buf.getvalue()[:300])
# doctor has a scale check and calls it (static, no live run)
doc = (HERE / "ops" / "doctor.py").read_text(encoding="utf-8")
check("def check_scale" in doc and "check_scale()" in doc,
      "doctor.py has and calls check_scale", "")
check("claim_context" in doc and "backups=" in doc,
      "doctor scale line covers backups and claim context", "")

print(f"\n{len(fails)} failures")
sys.exit(1 if fails else 0)
