#!/usr/bin/env python3
"""2026-11-02 六 optimisation tests (offline):
1. EXECUTOR.md split (core smaller, all sections present across files)
2. claim hint per kind (media/voice only, text gets none)
3. history truncation archives instead of deleting, search finds archives
Run: python3 tests/test_opt6_20261102.py"""
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

# --- 1. manual split -------------------------------------------------------
core = (HERE / "EXECUTOR.md").read_text(encoding="utf-8")
combined = "".join((HERE / f).read_text(encoding="utf-8") for f in
                    ["EXECUTOR.md", "EXECUTOR-media.md", "EXECUTOR-attachments.md",
                     "EXECUTOR-commands.md", "EXECUTOR-service.md"])
orig = (HERE / "backups" / "opt6-20261102-155222" / "EXECUTOR.md").read_text(encoding="utf-8") if (HERE / "backups" / "opt6-20261102-155222" / "EXECUTOR.md").exists() else None
check(len(core.encode()) < 8000, "EXECUTOR.md core under 8KB after split", f"{len(core.encode())} bytes")
check("EXECUTOR-media.md" in core and "45" in core, "core points to media split and keeps 45s wait")
if orig:
    import re
    for part in re.split(r'(?m)^## ', orig)[1:]:
        body = part.split("\n", 1)[1].strip() if "\n" in part else ""
        check(body[:80] in combined, f"original section kept: {part.splitlines()[0][:30]}")

# --- 2. claim hint ---------------------------------------------------------
b = h.load(NEW, "opt6")
h.set_creds(b, token="<redacted>")
b.settings_set({"media_wait": True, "progress_notes": True, "reply_style": "normal"})
v = b._executor_view({"message_id": "m1", "kind": "image", "text": "", "media_path": "/tmp/x.jpg"})
check("hint" in v and "30 秒" in v["hint"] and "EXECUTOR-media.md" in v["hint"], "no-text image gets 30s wait hint", str(v))
b.settings_set({"media_wait": False, "progress_notes": True, "reply_style": "normal"})
v = b._executor_view({"message_id": "m2", "kind": "file", "text": "", "media_path": "/tmp/x.pdf"})
check("hint" in v and "不等追问" in v["hint"], "media_wait off changes hint to direct reply", str(v))
b.settings_set({"media_wait": True, "progress_notes": True, "reply_style": "normal"})
v = b._executor_view({"message_id": "m3", "kind": "text", "text": "hello"})
check("hint" not in v, "text task gets no hint (core already covers it)", str(v))
v = b._executor_view({"message_id": "m4", "kind": "voice", "text": "", "media_path": "/tmp/x.wav", "voice_wav": True})
check("hint" in v and "复述" in v["hint"], "voice gets transcribe hint", str(v))

# --- 3. history archive ----------------------------------------------------
b.HISTORY_MAX_BYTES = 300
b.HISTORY_KEEP_LINES = 2
for i in range(8):
    b.history_append("user", f"归档测试词条{i} uniqueopt6", at=1793634600000 + i * 1000)
live = b.history_read()
arch = b.history_read_all()
check(len(live) <= 3, "live history stays bounded after truncation", f"live={len(live)}")
check(any("uniqueopt6" in (r.get("text") or "") for r in arch), "archived lines still readable via history_read_all")
check(len(arch) > len(live), "archive holds more than live file", f"live={len(live)} all={len(arch)}")
# search path uses archives
buf = io.StringIO()
with redirect_stdout(buf):
    rc = b.cmd_history(SimpleNamespace(search="uniqueopt6", limit=50, json=False, hours=None))
check(rc == 0 and "uniqueopt6" in buf.getvalue(), "history --search finds archived lines", buf.getvalue()[:200])
# conversation context still uses live only (no blow-up): function exists and returns bounded
print(f"\n{len(fails)} failures")
sys.exit(1 if fails else 0)
