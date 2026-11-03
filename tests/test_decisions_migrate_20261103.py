#!/usr/bin/env python3
"""Decisions migration tests (v5.0.3): a legacy-only deployment must not
lose its decisions. v5.0.2 wired the legacy fallback into claim only, so
`decisions` show printed empty and the first --add seeded the runtime
file with just the new entry. All three paths now share _decisions_read.
Run: python3 tests/test_decisions_migrate_20261103.py
"""
import importlib.util, io, shutil, sys, tempfile
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
fails = []


def check(cond, name):
    print(("PASS  " if cond else "FAIL  ") + name)
    if not cond:
        fails.append(name)


def load(name):
    d = Path(tempfile.mkdtemp(prefix="wb-dec-"))
    shutil.copy(HERE / "bridge.py", d / "bridge.py")
    spec = importlib.util.spec_from_file_location(name, d / "bridge.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.log = lambda msg: None
    return mod, d


LEGACY = "# 决策清单\n\n- 旧决定 A：备份只传脱敏包。\n- 旧决定 B：语音默认发文件卡片。\n"

# 1. Legacy-only: show prints the legacy text, --add seeds state with it.
b, d = load("dec_mig1")
(d / "DECISIONS.md").write_text(LEGACY, encoding="utf-8")
check(b._decisions_read() == LEGACY, "legacy-only _decisions_read returns legacy text")
buf = io.StringIO()
with redirect_stdout(buf):
    b.cmd_decisions(SimpleNamespace(add=None))
check("旧决定 A" in buf.getvalue(), "legacy-only show prints legacy decisions")
b.cmd_decisions(SimpleNamespace(add="新决定 C | 测试 | 已做 | 无"))
state_text = (d / "state" / "DECISIONS.md").read_text(encoding="utf-8")
check("旧决定 A" in state_text and "旧决定 B" in state_text
      and "新决定 C" in state_text,
      "first --add seeds state file from legacy text, then appends")
check(b._decisions_read() == state_text,
      "after migration, reader prefers the state file")
buf = io.StringIO()
with redirect_stdout(buf):
    b.cmd_decisions(SimpleNamespace(add=None))
check("旧决定 A" in buf.getvalue() and "新决定 C" in buf.getvalue(),
      "show after migration prints both old and new decisions")

# 2. State present: legacy is ignored (state is canonical).
b2, d2 = load("dec_mig2")
(d2 / "DECISIONS.md").write_text(LEGACY, encoding="utf-8")
(d2 / "state").mkdir()
(d2 / "state" / "DECISIONS.md").write_text("- 现行决定 X。\n", encoding="utf-8")
check(b2._decisions_read() == "- 现行决定 X。\n",
      "state file wins over legacy file when both exist")

# 3. Neither file: empty, no crash.
b3, d3 = load("dec_mig3")
check(b3._decisions_read() == "", "no decisions file reads as empty")

print()
if fails:
    print(f"{len(fails)} FAILED: {fails}")
    sys.exit(1)
print("ALL DECISIONS-MIGRATION CHECKS PASSED")
