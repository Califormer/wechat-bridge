#!/usr/bin/env python3
"""长任务接力 (2026-11-07), acceptance §5 of the brief except the end-to-end
regression (tests/test_relay_e2e_20261107.py): long_hint on the 5 cases,
short tasks unchanged, plan rules (<= 7 min per step, 0600, no credentials),
complete --continue (new wake key, no supersede, old leg refused, warm claims
keep off), and the old semantics around it - superseded exit 4, 「取消」,
--hold-minutes, reply-slot reserve, batch 5 -> 1, the off switch - plus the
limits, 「继续」 and the stall guard.
Run: python3 tests/test_relay_20261107.py
WB_OLD_BRIDGE=<5.0.5 bridge.py> also compares short-task claims byte for byte."""
import json
import os
import sys
import time

from relay_fixtures import (boot, say, flush, claim, progress, complete, stats,
                            hook_keys, plan_json, check, finish)

STEPS2 = [{"step_id": "a", "do": "跑前一半测试", "est_min": 5, "outputs": ["a.log"]},
          {"step_id": "b", "do": "跑后一半测试", "est_min": 5, "outputs": ["b.log"]}]
SHORT = ("桥是干什么用的", "`df -h` 还剩多少", "读一下 README 开头告诉我一句话")


def t_rules():
    b, _ = boot("rl_rules")
    five = [(SHORT[0], False), (SHORT[1], False), ("把最新的微信桥去敏开源吧", True),
            ("跑一遍全套测试", True), (SHORT[2], False)]
    for text, want in five:
        got = b.long_hint_rules({"kind": "text", "text": text})
        check(bool(got) == want, f"long_hint 「{text}」 -> {'long' if want else 'short'}", got)
    t0 = time.perf_counter()
    for _ in range(1000):
        for text, _w in five:
            b.long_hint_rules({"kind": "text", "text": text})
    us = (time.perf_counter() - t0) / 5000 * 1e6
    check(us < 1000, f"long_hint is a pure rule: {us:.1f} us per message (< 1 ms, no model call)")
    check(not b.long_hint_rules({"kind": "text", "text": "开源是什么意思"})
          and b.long_hint_rules({"kind": "file", "text": "", "file_name": "bridge.zip",
                                 "media_bytes": 494533})
          and b.long_hint_rules({"kind": "text", "text": "帮我把这 30 个文件都改成 UTF-8"}),
          "guards: asking about 开源 is short; a zip, or 30 files, is long")
    view = [{"message_id": f"f{i}", "kind": "file"} for i in range(3)]
    b._relay_claim_extras(view, False)
    check("3 个文件" in view[-1].get("long_hint", ""), "a batch of 3 files is long (hint on the last)")


def t_short_unchanged():
    b, _ = boot("rl_short", WECHAT_BRIDGE_PEEK="0")
    for text in SHORT:
        mid = say(b, text)
        flush(b)
        n0 = len(b.SENT)
        rc, out = claim(b)
        v = (out.get("tasks") or [{}])[0]
        check(rc == 0 and set(v) == {"message_id", "kind", "text"},
              f"short 「{text}」: the claim view is message_id/kind/text only", v)
        complete(b, mid, claim_id=out.get("claim_id"), result="好的")
        check(len(b.SENT) == n0 + 1, f"short 「{text}」: one message out (the reply), no progress note",
              b.SENT[n0:])
    check(not b.PLAN_DIR.exists(), "short tasks leave no state/plans")
    old = os.environ.get("WB_OLD_BRIDGE")
    if not old:
        print("SKIP  byte-for-byte vs the old bridge (set WB_OLD_BRIDGE=<5.0.5 bridge.py>)")
        return
    o, _ = boot("rl_old", src=old, WECHAT_BRIDGE_PEEK="0")
    n, _ = boot("rl_new", WECHAT_BRIDGE_PEEK="0")
    fixed = [int(time.time() * 1000)]
    o.now_ms = n.now_ms = lambda: fixed[0]
    for i, text in enumerate(SHORT):
        outs = []
        for m in (o, n):
            say(m, text, mid=f"cmp{i}")
            flush(m)
            rc, out = claim(m)
            for k in ("claim_id", "batch_id"):
                out.pop(k, None)
            outs.append(json.dumps(out, ensure_ascii=False, sort_keys=True))
            complete(m, f"cmp{i}", result="好的")
        check(outs[0] == outs[1], f"short 「{text}」: claim output byte-identical to the 5.0.5 bridge", outs)
        fixed[0] += 60_000


def t_plan_rules():
    b, _ = boot("rl_plan")
    mid = say(b, "把最新的微信桥去敏开源吧")
    flush(b)
    rc, out = claim(b)
    v, cid = out["tasks"][0], out["claim_id"]
    check("去敏" in v.get("long_hint", "") and "complete --continue" in v["long_hint"],
          "long task: the claim carries long_hint with the relay protocol", v)
    rc, po = progress(b, mid, claim_id=cid,
                      plan=plan_json([{"step_id": "all", "do": "扫全部 24 个文件", "est_min": 12}]))
    check(rc == 2 and "split" in po.get("error", "") and not b._plan_path(mid).exists(),
          "a step over 7 min is refused (split it); nothing is written", po)
    for steps, why in (([{"do": "读 state/credentials.json", "est_min": 1}], "credentials file"),
                       ([{"do": "登录", "est_min": 1, "inputs": ["bot_token=abcdef123456"]}], "token")):
        rc, po = progress(b, mid, claim_id=cid, plan=plan_json(steps))
        check(rc == 2 and "credential" in po.get("error", ""), f"a plan naming a {why} is refused", po)
    pf = b.BASE_DIR / "plan.json"
    pf.write_text(plan_json(STEPS2, "跑全套测试"), encoding="utf-8")
    n0 = len(b.SENT)
    rc, po = progress(b, mid, claim_id=cid, plan=str(pf), notify=True)
    fm, dm = b._plan_path(mid).stat().st_mode & 0o777, b.PLAN_DIR.stat().st_mode & 0o777
    check(rc == 0 and po["plan"]["steps"] == 2 and po["plan"]["next"]["step_id"] == "a"
          and fm == 0o600 and dm == 0o700,
          f"plan from a file: kept 0600 in a 0700 dir ({oct(fm)}, {oct(dm)}), next = a", po)
    check(len(b.SENT) == n0 + 1 and "分 2 步" in b.SENT[-1] and "预计" in b.SENT[-1],
          "--plan --notify: the first note is the bridge-written outline (steps + estimate)", b.SENT[n0:])
    rc, po = progress(b, mid, claim_id=cid, step_done="nope")
    check(rc == 2 and "unknown step_id" in po.get("error", ""), "an unknown step id is refused", po)
    rc, po = progress(b, mid, claim_id=cid, step_done="a", artifact=[str(pf)])
    stage = b.load_task(mid)["execution"].get("stage", "")
    check(rc == 0 and po["plan"]["done"] == 1 and po["plan"]["next"]["step_id"] == "b"
          and stage.startswith("第 1/2 步做完"),
          "--step-done: the plan moves on and the stage is written for 「进度」", (po, stage))
    pl = b.plan_load(mid)
    check(pl["steps"][0]["artifacts"][0]["bytes"] > 0 and pl["user"] == "owner",
          "the artifact is recorded with its size", pl["steps"][0])
    rc, po = progress(b, mid, claim_id=cid, plan=plan_json(
        [STEPS2[0], {"step_id": "b", "do": "跑后一半", "est_min": 3},
         {"step_id": "c", "do": "汇总", "est_min": 1}]))
    check(rc == 0 and po["plan"]["done"] == 1 and po["plan"]["steps"] == 3,
          "a re-plan keeps the finished steps", po)


def t_continue():
    b, clk = boot("rl_cont")
    b.batching_on(True)
    mid = say(b, "跑一遍全套测试")
    flush(b)
    rc, out = claim(b)
    cid = out["claim_id"]
    progress(b, mid, claim_id=cid, plan=plan_json(STEPS2), notify=True)
    clk.adv(5)
    rc, co = complete(b, mid, claim_id=cid, cont=True, step_done="a", then_wait=90)
    t = b.load_task(mid)
    check(rc == 0 and co.get("continued") and co.get("next_leg") == 2 and co.get("steps_left") == ["b"]
          and "then_wait_ignored" in co, "complete --continue: leg 1 hands off, b is left", co)
    check(t["status"] == "pending_execution" and t.get("relay_pending")
          and t.get("wake_key") == f"{mid}~r2" and f"{mid}~r2" in hook_keys(b),
          "back to pending with a new wake key: the hook wakes a fresh executor", t.get("wake_key"))
    ex = t.get("execution") or {}
    check(not ex.get("supersedes") and not ex.get("superseded_claim_ids"),
          "a hand-off is not a supersede (the chain does not interrupt itself)", ex)
    rc, po = progress(b, mid, claim_id=cid, stage="还在做")
    check(rc == 3 and po.get("stale_claim") and "handed off" in po.get("error", ""),
          "the old leg's late progress is refused (exit 3, not 4)", po)
    rc, co2 = complete(b, mid, claim_id=cid, result="迟到的结果")
    check(rc != 0 and "handed off" in co2.get("error", "")
          and b.load_task(mid)["status"] == "pending_execution",
          "the old leg cannot send a result after handing off", co2)
    t0 = time.monotonic()
    rc, w = claim(b, wait=3, context_since=b.now_ms())
    check(w.get("tasks") == [] and time.monotonic() - t0 < 2.5 and not b._waiter_alive(),
          "a warm (then-wait) claim leaves the leg alone and stops waiting at once",
          (w, round(time.monotonic() - t0, 2)))
    rc, out2 = claim(b)
    v = (out2.get("tasks") or [{}])[0]
    pv = v.get("plan") or {}
    check(v.get("message_id") == mid and "superseded" not in v and "long_hint" not in v
          and (pv.get("next") or {}).get("step_id") == "b"
          and [d["step_id"] for d in pv.get("done") or []] == ["a"]
          and "主动交棒" in v.get("resume_from", ""),
          "leg 2 (cold claim) gets the plan: done a, next b, resume_from says handed off", v)
    t = b.load_task(mid)
    check(not t.get("relay_pending")
          and t["execution"].get("heartbeat_at") == t["execution"].get("claimed_at"),
          "claiming the leg clears relay_pending; the claim is its first heartbeat", t["execution"])
    cid2 = out2["claim_id"]
    rc, po = progress(b, mid, claim_id=cid2, step_done="b")
    check("next" not in po["plan"] and "complete --result" in po["plan"]["advice"],
          "all steps done: progress says send the final reply", po)
    rc, co = complete(b, mid, claim_id=cid2, cont=True)
    check(rc == 2 and co.get("all_done"), "--continue with nothing left is refused", co)
    n0 = len(b.SENT)
    rc, co = complete(b, mid, claim_id=cid2, result="全套测试：全部通过")
    check(rc == 0 and b.load_task(mid)["status"] == "sent" and b.plan_load(mid)["state"] == "done"
          and len(b.SENT) == n0 + 1, "the final complete sends one reply; plan done", co)
    rc, st = stats(b, [mid])
    legs = (st["tasks"][0].get("relay") or {}).get("legs") or []
    check([lg["how"] for lg in legs] == ["continue", "complete"] and legs[0]["steps_done"] == ["a"]
          and all(lg["start"] and lg["end"] and lg["ms"] is not None for lg in legs),
          "stats --task-id: one row per leg (start, end, ms, steps, checkpoint)", legs)
    b2, clk2 = boot("rl_warmlong")
    say(b2, "把最新的微信桥去敏开源吧")
    flush(b2)
    got = b2.claim_tasks(limit=5, wait_s=1, context_since=b2.now_ms())
    check("热接" in ((got.get("tasks") or [{}])[0].get("long_hint") or ""),
          "a long task caught by a warm turn is told to plan, then hand off at once", got)


def t_quota_gap():
    b, clk = boot("rl_quota")
    b.RELAY_NOTIFY_GAP_MIN = 1
    mid = say(b, "跑一遍全套测试")
    flush(b)
    rc, out = claim(b)
    cid = out["claim_id"]
    progress(b, mid, claim_id=cid, plan=plan_json(STEPS2))
    clk.adv(2)
    real = b.choose_context_token
    b.choose_context_token = lambda u, c: (c, 3)
    rc, co = complete(b, mid, claim_id=cid, cont=True, step_done="a")
    b.choose_context_token = real
    check(rc == 0 and co.get("continued")
          and "last reply slots" in str((co.get("notice") or {}).get("skipped")),
          "hand-off note held back when only the reserved reply slots are left", co)
    rc, out = claim(b)
    clk.adv(2)
    n0 = len(b.SENT)
    rc, co = complete(b, mid, claim_id=out["claim_id"], cont=True)
    check(rc == 0 and (co.get("notice") or {}).get("ok") and len(b.SENT) == n0 + 1
          and "1/2 步做完" in b.SENT[-1], "once the gap has passed, one progress note at a hand-off",
          (co, b.SENT[n0:]))


def t_interrupts():
    b, clk = boot("rl_intr")
    b.batching_on(True)                 # as serve does: batches + supersede
    mid = say(b, "把最新的微信桥去敏开源吧")
    flush(b)
    rc, out = claim(b)
    cid = out["claim_id"]
    progress(b, mid, claim_id=cid, plan=plan_json(STEPS2), notify=True)
    progress(b, mid, claim_id=cid, step_done="a")
    m2 = say(b, "顺便看下 df -h")
    rc, po = progress(b, mid, claim_id=cid, stage="做 b")
    t = b.load_task(mid)
    check(rc == 4 and po.get("superseded") and t["relay"]["legs"][-1]["how"] == "superseded"
          and t.get("relay_pending"),
          "user writes mid-leg: superseded (exit 4) as before; the leg is recorded, the plan kept", po)
    flush(b)
    rc, out2 = claim(b)
    tasks = out2.get("tasks") or []
    v = next((x for x in tasks if x["message_id"] == mid), {})
    check(sorted(x["message_id"] for x in tasks) == sorted([mid, m2]) and "merge_hint" in out2
          and "superseded" in v and [d["step_id"] for d in (v.get("plan") or {}).get("done") or []] == ["a"],
          "next round: the long task (plan intact) and the new message together", out2)
    rc, co = complete(b, mid, claim_id=out2["claim_id"], cont=True)
    check(rc == 0 and co.get("continued") and co.get("merged") == [m2]
          and b.load_task(m2)["send"]["merged_into"] == mid,
          "--continue folds the follow-up into the chain: the one final reply answers both", co)
    b.cancel_command("owner")
    rc, out3 = claim(b)
    check(b.load_task(mid)["status"] == "cancelled" and not out3.get("tasks"),
          "「取消」 between legs: the chain stops, nothing more is claimed")
    c, _ = boot("rl_cancel2")
    m = say(c, "跑一遍全套测试")
    flush(c)
    rc, out = claim(c)
    progress(c, m, claim_id=out["claim_id"], plan=plan_json(STEPS2))
    c.cancel_command("owner")
    rc, co = complete(c, m, claim_id=out["claim_id"], cont=True, step_done="a")
    check(rc == 3 and co.get("cancelled"), "「取消」 during a leg: --continue says cancelled (exit 3)", co)
    d, dclk = boot("rl_hold")
    m = say(d, "跑一遍全套测试")
    flush(d)
    rc, out = claim(d)
    progress(d, m, claim_id=out["claim_id"], plan=plan_json(STEPS2))
    progress(d, m, claim_id=out["claim_id"], stage="长的一步", hold_minutes=15)
    dclk.adv(11)
    d._reclaim_stale()
    check(d.load_task(m)["status"] == "executing", "--hold-minutes still keeps a planned leg from reclaim")
    dclk.adv(6)
    d._reclaim_stale()
    t = d.load_task(m)
    check(t["status"] == "pending_execution" and t.get("relay_pending")
          and t["relay"]["legs"][-1]["how"] == "reclaimed" and t.get("wake_key") == f"{m}~r2",
          "after the lease: reclaimed into the next leg (plan kept, new wake key)", t.get("relay"))


def t_limits_resume_stall():
    b, clk = boot("rl_lim")
    b.RELAY_MAX_LEGS = 2
    steps3 = [{"step_id": s, "do": f"扫第 {i} 组", "est_min": 3} for i, s in enumerate("abc", 1)]
    mid = say(b, "把这 30 个文件逐项扫一遍")
    flush(b)
    rc, out = claim(b)
    progress(b, mid, claim_id=out["claim_id"], plan=plan_json(steps3, "逐项扫 30 个文件"), notify=True)
    rc, co = complete(b, mid, claim_id=out["claim_id"], cont=True, step_done="a")
    rc, out = claim(b)
    n0 = len(b.SENT)
    rc, co = complete(b, mid, claim_id=out["claim_id"], cont=True, step_done="b")
    msg = b.SENT[-1] if len(b.SENT) > n0 else ""
    check(rc == 0 and co.get("stopped") and "做到第 2/3 步" in msg and "还剩" in msg and "继续" in msg
          and b.plan_load(mid)["state"] == "stopped" and b.load_task(mid)["status"] == "sent",
          "at the leg limit: stop and tell the user where it is, what is left, how to go on", (co, msg))
    m3 = say(b, "继续")
    flush(b)
    rc, out = claim(b)
    v, cid = out["tasks"][0], out["claim_id"]
    check(mid in v.get("resume_plan", ""), "「继续」: the claim points at the stopped plan", v)
    n0 = len(b.SENT)
    rc, po = progress(b, m3, claim_id=cid, plan_from=mid, notify=True)
    check(rc == 0 and po["plan"]["done"] == 2 and po["plan"]["next"]["step_id"] == "c"
          and b.plan_load(mid)["state"] == "adopted"
          and "前面 2 步已做完" in (b.SENT[-1] if len(b.SENT) > n0 else ""),
          "--plan-from adopts it: done steps kept, the note says so", (po, b.SENT[n0:]))
    progress(b, m3, claim_id=cid, step_done="c")
    rc, co = complete(b, m3, claim_id=cid, result="30 个文件都扫完了")
    check(rc == 0 and b.plan_load(m3)["state"] == "done", "the adopted chain ends with one reply")
    s, _ = boot("rl_stall")
    m = say(s, "跑一遍全套测试")
    flush(s)
    for leg in (1, 2, 3):
        rc, out = claim(s)
        if leg == 1:
            progress(s, m, claim_id=out["claim_id"], plan=plan_json(STEPS2))
        rc, co = complete(s, m, claim_id=out["claim_id"], cont=True)
    check(co.get("stopped") and "连续两棒" in co.get("reason", ""),
          "two legs in a row with no finished step: stop instead of spinning", co)


def t_off_and_batch():
    off, _ = boot("rl_off", WECHAT_BRIDGE_RELAY="0")
    m = say(off, "把最新的微信桥去敏开源吧")
    flush(off)
    rc, out = claim(off)
    v, cid = out["tasks"][0], out["claim_id"]
    rc1, p1 = progress(off, m, claim_id=cid, plan=plan_json(STEPS2))
    rc2, c2 = complete(off, m, claim_id=cid, cont=True)
    check(not off.RELAY_ON and "long_hint" not in v and rc1 == 2 and rc2 == 2
          and not off.PLAN_DIR.exists(),
          "WECHAT_BRIDGE_RELAY=0: no long_hint; --plan / --continue refused (v5.0.5 behaviour)", (v, p1, c2))
    b, _ = boot("rl_batch5")
    ids = [say(b, f"第 {i} 句话") for i in range(1, 6)]
    flush(b)
    rc, out = claim(b)
    got = [x["message_id"] for x in out.get("tasks") or []]
    check(sorted(got) == sorted(ids) and "merge_hint" in out
          and not any("long_hint" in x or "plan" in x for x in out["tasks"]),
          "a burst of 5 is still one claim with merge_hint", out)
    n0 = len(b.SENT)
    rc, co = complete(b, got[-1], claim_id=out["claim_id"], merge_ids=",".join(got[:-1]), result="一起答")
    check(rc == 0 and len(b.SENT) == n0 + 1 and sorted(co.get("merged") or []) == sorted(got[:-1]),
          "5 messages -> 1 reply", co)


for fn in (t_rules, t_short_unchanged, t_plan_rules, t_continue, t_quota_gap, t_interrupts,
           t_limits_resume_stall, t_off_and_batch):
    print(f"--- {fn.__name__}")
    fn()
sys.exit(finish("RELAY"))
