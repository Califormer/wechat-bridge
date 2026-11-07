#!/usr/bin/env python3
"""§8 of the 2026-11-07 brief (smarter / faster), each behind its own switch:
file_peek in the claim (8.2.4), the 「统计」 command (8.2.1), 「进度」 over a
whole relay chain (8.2.5), the warm-wait what-if in stats (8.3.1), the CLI
call log behind `stats --task-id` (8.3.3); plus the doc / prompt guards
(prompt in sync, EXECUTOR.md < 8000 bytes, the relay manual, TUNING.md).
Run: python3 tests/test_s8_20261107.py"""
import hashlib
import io
import json
import sys
import time
from contextlib import redirect_stdout

from relay_fixtures import (HERE, boot, say, flush, claim, progress, complete, plan_json,
                            check, finish)


def t_peek():
    b, _ = boot("s8_peek")
    base = b.BASE_DIR
    (base / "README.md").write_text("# 微信桥\n\n桥把微信消息转给 Muse 执行。\n"
                                    + "".join(f"第 {i} 行\n" for i in range(100)), encoding="utf-8")
    (base / "state" / "notes.md").write_text("私有\n", encoding="utf-8")
    (base / "secret_notes.md").write_text("x\n", encoding="utf-8")
    (base / "config.md").write_text("password: hunter2hunter2\n", encoding="utf-8")
    outside = base.parent / f"outside-{base.name}.md"
    outside.write_text("外面的文件\n", encoding="utf-8")
    try:
        mid = say(b, "读一下 README 开头告诉我一句话")
        flush(b)
        rc, out = claim(b)
        v = out["tasks"][0]
        pk = v.get("file_peek") or []
        check(len(pk) == 1 and pk[0]["path"] == "README.md" and pk[0]["head"].startswith("# 微信桥")
              and pk[0]["lines"] == 103 and pk[0]["truncated"] and "file_peek" in v.get("hint", "")
              and "long_hint" not in v,
              "「读一下 README 开头」: the start of README.md is in the claim (no read call)", v)
        complete(b, mid, claim_id=out["claim_id"], result="桥把微信消息转给 Muse 执行。")
        for text, why in (("看下 state/notes.md", "inside state/"),
                          ("看下 secret_notes.md", "a secret-looking name"),
                          ("看下 config.md", "secret-looking content"),
                          (f"看下 {outside}", "outside the bridge directory"),
                          ("看下 ../../x.md", "a path escaping the bridge directory"),
                          ("桥是干什么用的", "no file named")):
            m = say(b, text)
            flush(b)
            rc, out = claim(b)
            v = out["tasks"][0]
            check("file_peek" not in v and "hint" not in v, f"no peek: {why}", v)
            complete(b, m, claim_id=out["claim_id"], result="好")
    finally:
        outside.unlink()
    named, plain = ({"kind": "text", "text": "读一下 README 开头告诉我一句话"},
                    {"kind": "text", "text": "桥是干什么用的"})
    t0 = time.perf_counter()
    for _ in range(200):
        b.file_peeks(named)
    ms = (time.perf_counter() - t0) / 200 * 1000
    t0 = time.perf_counter()
    for _ in range(2000):
        b.file_peeks(plain)
    us = (time.perf_counter() - t0) / 2000 * 1e6
    check(ms < 5 and us < 100, f"cost: {ms:.2f} ms when a file is named, {us:.1f} us when none is")
    off, _ = boot("s8_peek_off", WECHAT_BRIDGE_PEEK="0")
    (off.BASE_DIR / "README.md").write_text("# x\n", encoding="utf-8")
    say(off, "读一下 README 开头")
    flush(off)
    rc, out = claim(off)
    check("file_peek" not in out["tasks"][0], "WECHAT_BRIDGE_PEEK=0: no peek")


def _cmap(b):
    return next(v for v in vars(b).values()
                if isinstance(v, dict) and v.get("最近对话") == "recent_dialogue")


def t_stats_command():
    b, clk = boot("s8_stats")
    check(_cmap(b).get("统计") == "stats" and _cmap(b).get("/stats") == "stats"
          and "· 统计" in b.HELP_TEXT, "「统计」 is an exact-match bridge command, listed in 帮助")
    check(b.run_command("stats", "owner").startswith("最近 72 小时还没有"), "「统计」 with no data says so")
    for i in range(3):
        m = say(b, f"问题 {i}")
        flush(b)
        rc, out = claim(b)
        clk.adv(0.5)
        complete(b, m, claim_id=out["claim_id"], result=f"答 {i}")
        clk.adv(1)
    txt = b.run_command("stats", "owner")
    st = b.latency_stats(hours=72)
    check(st["n"] == 3 and txt.startswith("最近 72 小时 3 条")
          and f"{st['total']['p50'] / 1000:.1f} 秒" in txt and "· 处理" in txt,
          "「统计」 copies the stats numbers as they are", txt)
    say(b, "统计")
    flush(b)
    rc, out = claim(b)
    check(not out.get("tasks"), "「统计」 wakes no executor: the bridge answers it", out)
    off, _ = boot("s8_stats_off", WECHAT_BRIDGE_CMD_STATS="0")
    check("统计" not in _cmap(off), "WECHAT_BRIDGE_CMD_STATS=0: 「统计」 is ordinary text for Muse")


def t_progress_chain():
    b, clk = boot("s8_prog")
    mid = say(b, "跑一遍全套测试")
    flush(b)
    rc, out = claim(b)
    steps = [{"step_id": s, "do": f"跑第 {i} 组测试", "est_min": 4} for i, s in enumerate("abc", 1)]
    progress(b, mid, claim_id=out["claim_id"], plan=plan_json(steps))
    clk.adv(4)
    progress(b, mid, claim_id=out["claim_id"], step_done="a")
    complete(b, mid, claim_id=out["claim_id"], cont=True)
    txt = b.progress_command("owner")
    check("交棒中" in txt and "第 2/3 步" in txt and "第 2 棒" in txt,
          "「进度」 between legs: the whole chain (step, leg)", txt)
    claim(b)
    clk.adv(2)
    txt = b.progress_command("owner")
    check("处理中" in txt and "第 2/3 步" in txt and "预计还要约 8 分钟" in txt,
          "「进度」 during a leg: step k/N, leg, time left", txt)


def t_whatif():
    b, _ = boot("s8_whatif")
    rows = [{"role": "bot", "at": 0}, {"role": "user", "at": 70_000},
            {"role": "bot", "at": 100_000}, {"role": "user", "at": 210_000},
            {"role": "bot", "at": 300_000}]
    w = b._warm_wait_whatif(rows, [70_000, 110_000])
    check(w["caught"]["90s"] == 0.5 and w["caught"]["120s"] == 1.0
          and w["wait_per_reply_s"]["90s"] == round((70 + 90 + 90) / 3, 1) and w["replies"] == 3,
          "what-if: 90 s catches 1 of 2 follow-ups, 120 s both; waiting per reply counted", w)
    fg = b.followup_gaps()
    check("whatif" in fg and set(fg["within"]) == {"45s", "90s", "180s", "300s"},
          "stats followup_gaps keeps its old keys and adds whatif", fg)


def _main(b, *argv):
    old, sys.argv = sys.argv, ["bridge.py", *argv]
    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            rc = b.main()
    finally:
        sys.argv = old
    return rc, buf.getvalue()


def t_cli_log():
    b, _ = boot("s8_cli")
    mid = say(b, "问一句")
    flush(b)
    rc, txt = _main(b, "claim", "--limit", "5")
    cid = json.loads(txt)["claim_id"]
    _main(b, "heartbeat", "--task-id", mid, "--claim-id", cid)
    _main(b, "history", "--limit", "3")
    _main(b, "history", "--limit", "3")
    _main(b, "complete", "--task-id", mid, "--claim-id", cid, "--result", "好的呀")
    ev = b.task_evidence(mid)
    cmds = [c["cmd"] for c in ev.get("cli_calls") or []]
    check(cmds == ["claim", "heartbeat", "history", "history", "complete"] and ev.get("cli_count") == 5
          and ev.get("cli_repeats") == {"history": 2},
          "stats --task-id lists the message's bridge calls in order and flags repeats", ev.get("cli_calls"))
    raw = b.CLI_LOG_FILE.read_text(encoding="utf-8")
    check(all(len(x.split("\t")) == 5 for x in raw.splitlines()) and "好的呀" not in raw
          and (b.CLI_LOG_FILE.stat().st_mode & 0o777) == 0o600,
          "logs/cli_calls.log: 5 fields per call, 0600, never arguments or text", raw[:300])


def t_docs():
    core = (HERE / "EXECUTOR.md").read_text(encoding="utf-8")
    relay = (HERE / "EXECUTOR-relay.md").read_text(encoding="utf-8")
    hook = json.loads((HERE / "hooks/wechat-bridge-inbox.json").read_text(encoding="utf-8"))
    txt = (HERE / "hooks/wechat-bridge-inbox.prompt.txt").read_text(encoding="utf-8")
    rec = (HERE / "hooks/wechat-bridge-inbox.prompt.sha256").read_text(encoding="utf-8").split()
    pr = hook.get("prompt") or ""
    check(len(core.encode()) < 8000 and "EXECUTOR-relay.md" in core,
          f"EXECUTOR.md points to EXECUTOR-relay.md, under 8000 bytes ({len(core.encode())})")
    check(pr == txt and rec[0] == hashlib.sha256(pr.encode()).hexdigest() and int(rec[1]) == len(pr),
          "prompt: JSON = prompt.txt = sha256 record")
    check("long_hint" in pr and "complete --continue" in pr and "EXECUTOR-relay.md" in pr
          and len(pr) < 2450, f"prompt carries the relay line ({len(pr)} chars)")
    check(hook.get("poll_interval_secs") == 5 and hook.get("script_timeout_secs") == 600,
          "hook settings untouched: poll 5 s, timeout 600 s")
    miss = [k for k in ("--plan", "--step-done", "--artifact", "complete --continue", "--plan-from",
                        "≤7", "state/plans", "resume_plan", "WECHAT_BRIDGE_RELAY=0") if k not in relay]
    check(not miss, "EXECUTOR-relay.md covers the whole protocol", miss)
    tun = (HERE / "TUNING.md").read_text(encoding="utf-8")
    miss = [k for k in ("WECHAT_BRIDGE_RELAY", "WECHAT_BRIDGE_RELAY_MAX_LEGS", "WECHAT_BRIDGE_PEEK",
                        "WECHAT_BRIDGE_CMD_STATS", "WECHAT_BRIDGE_CLI_LOG") if k not in tun]
    check(not miss, "TUNING.md lists the new switches", miss)


for fn in (t_peek, t_stats_command, t_progress_chain, t_whatif, t_cli_log, t_docs):
    print(f"--- {fn.__name__}")
    fn()
sys.exit(finish("S8"))
