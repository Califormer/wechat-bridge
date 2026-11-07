#!/usr/bin/env python3
"""v1.5 + v1.6 (2026-11-07) background improvements taken from open-source designs:
the background lane (OpenClaw sub-agents: one queue lane capped by
maxConcurrent), quote-bound follow-ups (ilink-hub quote routing / OpenClaw
thread-bound sessions) and the resume notice (OpenClaw restart recovery).
Uses the real quote store and send path. Run: python3 tests/test_bg_v15_20261107.py"""
import argparse
import importlib.util
import io
import json
import sys
from contextlib import redirect_stdout

from relay_fixtures import (HERE, boot, say, flush, claim, complete, lt, tick, notices,
                            check, finish)

LONG = "把最新的微信桥去敏开源吧"
_q = [0]


def wake_raw(b):
    buf = io.StringIO()
    with redirect_stdout(buf):
        b.cmd_long_tasks(argparse.Namespace(action="wake-check", id=None))
    return (buf.getvalue().splitlines() + ["", "", ""])[:3]


def say_quote(b, text, svr):
    """A WeChat message that quotes (ref_msg) the message with server id svr."""
    _q[0] += 1
    mid = f"q{_q[0]}"
    b.ingest_message({"ilink_user_id": "owner"}, {
        "message_id": mid, "from_user_id": "owner", "message_type": 1, "context_token": f"CQ{_q[0]}",
        "item_list": [{"type": 1, "text_item": {"text": text},
                       "ref_msg": {"svr_id": svr, "message_item": {"type": 1, "text_item": {"text": "…"}}}}]})
    return mid


def t_lane():
    b, clk = boot("v15_lane", WECHAT_BRIDGE_BACKGROUND="1")
    b.batching_on(True)
    tick(b)
    a = say(b, LONG)
    flush(b)
    rc, oa = lt(b, "take", a)
    c = say(b, "把这 30 个文件逐项扫一遍")
    flush(b)
    check("前面还有 1 个后台任务" in notices(b)[-1][1], "the second long task is told its place in the lane",
          notices(b)[-1])
    check(wake_raw(b)[0] == "QUIET", "lane full (1 running): the hook does not wake the main agent for it")
    rc, oc = lt(b, "take", c)
    check(rc == 3 and oc.get("busy"), "take is refused while the lane is full", oc)
    check("第 2 位" in b.progress_command("owner"), "「进度」 shows the place in the queue",
          b.progress_command("owner"))
    clk.adv(15)
    lt(b, "progress", a, token=oa["token"], stage="还在做")
    b.check_background()
    check(b.bg_read(c)["state"] == "queued" and b.load_task(c)["status"] == "background",
          "waiting its turn 15 min: NOT sent back to the foreground (v1.4 would have)")
    rf = b.BASE_DIR / "r.md"
    rf.write_text("A 做完了", encoding="utf-8")
    lt(b, "finish", a, token=oa["token"], result_file=str(rf))
    w = wake_raw(b)
    check(w[0] == "WAKE" and c in w[2], "a slot frees: the next hook tick wakes the main agent for it", w)
    rc, oc = lt(b, "take", c)
    check(rc == 0 and oc.get("token"), "...and it can be taken now", oc)
    rc, ov = lt(b, "list")
    check(ov.get("lane") == {"max_running": 1, "running": 1}, "long-tasks shows the lane", ov.get("lane"))
    b2, clk2 = boot("v15_lane2", WECHAT_BRIDGE_BACKGROUND="1", WECHAT_BRIDGE_BG_MAX_RUNNING="2")
    b2.batching_on(True)
    tick(b2)
    x, y = say(b2, LONG), None
    flush(b2)
    y = say(b2, "把这 30 个文件逐项扫一遍")
    flush(b2)
    r1, _ = lt(b2, "take", x)
    r2, _ = lt(b2, "take", y)
    check(r1 == 0 and r2 == 0, "WECHAT_BRIDGE_BG_MAX_RUNNING=2: two run side by side")


def t_quotes():
    b, clk = boot("v15_quote", WECHAT_BRIDGE_BACKGROUND="1")
    b.batching_on(True)
    tick(b)
    lead = say(b, LONG)
    flush(b)
    sig = notices(b)[-1][0]
    b.deliver(sig)
    smid = f"m{len(b.SENT)}"
    rec = b.quote_get(smid) or {}
    check(rec.get("task") == sig and "引用这条回复" in b.SENT[-1],
          "the signal is in the quote store (send path) and tells the user how to add to the task", rec)
    q1 = say_quote(b, "打包格式改成 tar.gz", smid)
    flush(b)
    t1, e = b.load_task(q1), b.bg_read(lead)
    check(t1["status"] == "sent" and t1["send"].get("merged_into") == lead
          and (e.get("notes") or [{}])[-1].get("text") == "打包格式改成 tar.gz",
          "quoting the signal: the follow-up goes into the task's notes, not a new task", (t1.get("send"), e.get("notes")))
    rc, out = claim(b)
    check(not out.get("tasks"), "no executor turn for it", out)
    ack = notices(b)[-1][1]
    check("已转给后台" in ack and "tar.gz" in ack, "the user gets a one-line ack", ack)
    rc, o = lt(b, "take", lead)
    check([n["text"] for n in o.get("notes") or []] == ["打包格式改成 tar.gz"] and "notes" in o.get("next", ""),
          "take hands the notes to the main agent", o.get("notes"))
    rc, p = lt(b, "progress", lead, token=o["token"], stage="s1")
    check("notes" not in p, "notes already handed over are not repeated", p)
    say_quote(b, "另外 README 也一起更新", lead)        # quotes the user's own long-task message
    flush(b)
    rc, p = lt(b, "progress", lead, token=o["token"], stage="s2")
    check([n["text"] for n in p.get("notes") or []] == ["另外 README 也一起更新"] and "notes_hint" in p,
          "quoting the original request works too; progress returns just the new note", p)
    m3 = say(b, "桥是干什么用的")
    flush(b)
    rc, out = claim(b)
    check([x["message_id"] for x in out.get("tasks") or []] == [m3], "an unquoted message is an ordinary task", out)
    complete(b, m3, claim_id=out["claim_id"], result="桥把消息转给 Muse")
    q4 = say_quote(b, "再详细一点", f"m{len(b.SENT)}")
    flush(b)
    rc, out = claim(b)
    check([x["message_id"] for x in out.get("tasks") or []] == [q4],
          "quoting an ordinary (finished) reply stays an ordinary follow-up", out)
    complete(b, q4, claim_id=out["claim_id"], result="好")
    q5 = say_quote(b, "密码是 password: hunter2hunter2", smid)
    flush(b)
    check(b.load_task(q5)["status"] != "sent" and len(b.bg_read(lead)["notes"]) == 2,
          "a secret-looking follow-up is never written into the queue")
    rf = b.BASE_DIR / "res.md"
    rf.write_text("去敏开源包已打好（tar.gz），README 也更新了。", encoding="utf-8")
    n0 = len(b.SENT)
    lt(b, "finish", lead, token=o["token"], result_file=str(rf))
    check(len(b.SENT) == n0 + 1 and b.load_task(q1)["status"] == "sent",
          "one final reply answers the task and its quoted follow-ups")


def t_resume_notice_docs():
    b, clk = boot("v15_resume", WECHAT_BRIDGE_BACKGROUND="1")
    b.batching_on(True)
    tick(b)
    m = say(b, "跑一遍全套测试")
    flush(b)
    lt(b, "take", m)
    clk.adv(31)
    b.check_background()
    check(any("中断了一下" in x[1] and "重新排队" in x[1] for x in notices(b)),
          "taken but silent: requeued AND the user is told it will resume (OpenClaw restart notice)")
    bgm = (HERE / "BACKGROUND.md").read_text(encoding="utf-8")
    tun = (HERE / "TUNING.md").read_text(encoding="utf-8")
    check("notes" in bgm and "busy" in bgm and "WECHAT_BRIDGE_BG_MAX_RUNNING" in tun,
          "BACKGROUND.md covers notes and the lane; TUNING.md the new switch")


# ---- v1.6: the review of v1.5 (Claude-wechat-bridge-v15-fix.md) -------------
def _bgb(name, **env):
    b, clk = boot(name, WECHAT_BRIDGE_BACKGROUND="1", **env)
    b.batching_on(True)
    tick(b)
    return b, clk


def _fin(b, mid, tok, text):
    rf = b.BASE_DIR / f"res-{mid}.md"
    rf.write_text(text, encoding="utf-8")
    return lt(b, "finish", mid, token=tok, result_file=str(rf))


def _doctor_bg(b):
    (b.BASE_DIR / "ops").mkdir(exist_ok=True)
    (b.BASE_DIR / "ops" / "doctor.py").write_text((HERE / "ops" / "doctor.py").read_text(encoding="utf-8"))
    spec = importlib.util.spec_from_file_location(f"doc_{id(b)}", b.BASE_DIR / "ops" / "doctor.py")
    doc = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(doc)
    doc.results.clear()
    doc.check_background()
    return doc.results[-1]


def t16_bug1_notes_offset():
    b, clk = _bgb("v16_notes")
    lead = say(b, LONG)
    flush(b)
    rc, o = lt(b, "take", lead)
    for i in range(1, 21):
        say_quote(b, f"补充 {i}", lead)
    flush(b)
    rc, p = lt(b, "progress", lead, token=o["token"], stage="读补充")
    check(len(p.get("notes") or []) == 20, "20 follow-ups, all handed over by one progress",
          len(p.get("notes") or []))
    say_quote(b, "补充 21", lead)
    flush(b)
    rc, p = lt(b, "progress", lead, token=o["token"], stage="再看")
    check([n["text"] for n in p.get("notes") or []] == ["补充 21"],
          "Bug 1: with 20 already read, the 21st still reaches the main agent", p.get("notes"))
    for i in range(22, 27):
        say_quote(b, f"补充 {i}", lead)
    flush(b)
    rc, p = lt(b, "progress", lead, token=o["token"], stage="又看")
    check([n["text"] for n in p.get("notes") or []] == [f"补充 {i}" for i in range(22, 27)],
          "...and the next five too: an unread follow-up is never trimmed", p.get("notes"))
    acks = [x for x in notices(b) if "已转给后台" in x[1]]
    check(len(acks) == 1, "Opt 5: 26 follow-ups, 1 ack (v1.5: 26) - reply slots kept", len(acks))
    check("已收到 26 条补充" in b.progress_command("owner"), "「进度」 shows the follow-up count instead",
          b.progress_command("owner"))


def t16_bug2_finish_gate():
    b, clk = _bgb("v16_finish")
    lead = say(b, LONG)
    flush(b)
    rc, o = lt(b, "take", lead)
    lt(b, "progress", lead, token=o["token"], stage="最后一步")
    say_quote(b, "结果里顺便列一下文件大小", lead)
    flush(b)
    n0 = len(b.SENT)
    rc, f = _fin(b, lead, o["token"], "去敏开源包已打好。")
    check(rc == 2 and [n["text"] for n in f.get("unread_notes") or []] == ["结果里顺便列一下文件大小"]
          and len(b.SENT) == n0 and b.load_task(lead)["status"] == "background",
          "Bug 2: a follow-up after the last progress stops finish - handed over, nothing sent yet", f)
    rc, f = _fin(b, lead, o["token"], "去敏开源包已打好；文件大小：pkg.zip 573 KB。")
    check(rc == 0 and len(b.SENT) == n0 + 1 and "文件大小" in b.SENT[-1],
          "...the next finish sends the one final reply, which answers it", f)
    m2 = say(b, "跑一遍全套测试")
    flush(b)
    rc, o2 = lt(b, "take", m2)
    say_quote(b, "顺便测一下 doctor", m2)
    flush(b)
    n0 = len(b.SENT)
    lt(b, "fail", m2, token=o2["token"], error="编译失败，还剩打包")
    check(len(b.SENT) == n0 + 1 and "顺便测一下 doctor" in b.SENT[-1],
          "fail names the follow-up that was never done", b.SENT[-1:])


def t16_bug3_quote_diagnosis():
    b, clk = _bgb("v16_diag")
    lead = say(b, LONG)
    flush(b)
    m = say_quote(b, "这条引用的消息桥上没有记录", "no-such-svr-id")
    flush(b)
    rc, out = claim(b)
    check([x["message_id"] for x in out.get("tasks") or []] == [m],
          "Bug 3: a quote leading to no live background task is an ordinary task, not lost", out)
    complete(b, m, claim_id=out["claim_id"], result="好")
    rc, sh = lt(b, "show", lead)
    lk = sh.get("last_quote_lookup") or {}
    check(lk.get("quote_svr_id") == "no-such-svr-id" and lk.get("found") is False
          and lk.get("store_record") is False and "ordinary" in lk.get("outcome", ""),
          "long-tasks show reports the last lookup: svr id, not found, what happened", lk)
    check(any("no live background task behind it" in x for x in getattr(b, "logs", [])),
          "...and the miss is logged")
    b2, _ = _bgb("v16_fallback")
    b2.NOTICES_ENABLED = False                       # the signal cannot go out
    l2 = say(b2, LONG)
    flush(b2)
    check((b2.bg_read(l2) or {}).get("signal") == "suppressed", "a signal that could not be queued is recorded")
    q = say_quote(b2, "改成 tar.gz", l2)             # quotes the user's own request
    flush(b2)
    rc, sh2 = lt(b2, "show", l2)
    check(b2.load_task(q)["send"].get("merged_into") == l2
          and (sh2.get("last_quote_lookup") or {}).get("via") in ("id", "svr_id")
          and ((sh2.get("entry") or {}).get("notes") or [{}])[-1].get("text") == "改成 tar.gz",
          "Bug 3 fallback: quoting the original request routes by id when no signal went out",
          sh2.get("last_quote_lookup"))
    rc, o = lt(b2, "take", l2)
    check(o.get("signal_sent") is False and "信号" in o.get("next", ""),
          "take tells the main agent the user never got the signal", o.get("signal_sent"))


def t16_bug4_turns():
    b, clk = _bgb("v16_turns")
    a = say(b, LONG)
    flush(b)
    rc, oa = lt(b, "take", a)
    bq = say(b, "把这 30 个文件逐项扫一遍")
    flush(b)
    cq = say(b, "批量把图片改名")
    flush(b)
    clk.adv(5)
    _fin(b, a, oa["token"], "A 好了")
    for _ in range(2):
        clk.adv(5.5)
        b.check_background()
    check(b.bg_read(bq)["state"] == "returned" and b.bg_read(cq)["state"] == "queued",
          "Bug 4: 11 min after the slot freed the head (B) goes back to the foreground; C, only "
          "waiting behind B, does not", (b.bg_read(bq)["state"], b.bg_read(cq)["state"]))
    clk.adv(6)
    b.check_background()
    check(b.bg_read(cq)["state"] == "queued", "C's own 10 minutes start when its turn comes")


def t16_bug5_worktime_doctor():
    b, clk = _bgb("v16_work")
    a = say(b, LONG)
    flush(b)
    rc, oa = lt(b, "take", a)
    bq = say(b, "把这 30 个文件逐项扫一遍")
    flush(b)
    e = b.bg_read(bq)
    e["queued_at"] -= 12 * 60_000                    # doctor uses the real clock
    b._bg_write(e)
    r = _doctor_bg(b)
    check(r["status"] == "WARN" and "waiting behind 1 running" in r["detail"],
          "Opt 2: doctor shows a long queue wait as WARN even while another task runs", r)
    e["queued_at"] += 12 * 60_000
    b._bg_write(e)
    for _ in range(20):                              # A works 200 min; B waits its turn
        clk.adv(10)
        lt(b, "progress", a, token=oa["token"], stage="做着")
    _fin(b, a, oa["token"], "A 做完了")
    rc, ob = lt(b, "take", bq)
    for _ in range(5):
        clk.adv(10)
        lt(b, "progress", bq, token=ob["token"], stage="做着")
    b.check_background()
    rc2, ov = lt(b, "list")
    row = (ov.get("running") or [{}])[0]
    check(rc == 0 and b.bg_read(bq)["state"] == "running" and 49 <= row.get("worked_min", 0) <= 51,
          "Bug 5: 200 min queued + 50 min of work is not 'over 240 min' - only work counts", row)


def t16_opt1_opt7_wakes():
    b, clk = _bgb("v16_wakes")
    bq = say(b, LONG)
    flush(b)
    for _ in range(3):
        wake_raw(b)
        clk.adv(3.1)
    check(b.bg_read(bq)["wakes"] == 3, "B woken 3 times, never taken")
    cq = say(b, "把这 30 个文件逐项扫一遍")
    flush(b)
    w = wake_raw(b)
    check(w[0] == "WAKE" and cq in w[2] and bq not in w[2],
          "Opt 7: B's wakes are used up, so C is woken - the free slot does not idle behind B", w)
    rc, oc = lt(b, "take", cq)
    clk.adv(2)
    _fin(b, cq, oc["token"], "C 做完了")
    w = wake_raw(b)
    check(w[0] == "WAKE" and bq in w[2] and b.bg_read(bq)["state"] == "queued",
          "Opt 1: when the slot frees, B gets a fresh set of wakes (and was not sent back)", w)


def t16_opt3_opt4_opt6():
    b, clk = _bgb("v16_notices")
    lead = say(b, LONG)
    flush(b)
    lt(b, "take", lead)
    clk.adv(31)
    b.check_background()                             # silent: requeued + "中断了一下"
    nid = next((x[0] for x in notices(b) if "中断了一下" in x[1]), None)
    check(nid and nid in (b.bg_read(lead).get("notice_ids") or []),
          "Opt 4: the requeue notice is registered on the task")
    b.deliver(nid)
    q = say_quote(b, "继续的时候顺便压缩一下", f"m{len(b.SENT)}")
    flush(b)
    check(b.load_task(q)["send"].get("merged_into") == lead, "...so quoting it routes the follow-up")
    s = say_quote(b, "服务器密码是 Abc123456", lead)
    flush(b)
    check(b.load_task(s)["status"] == "pending_execution" and any("含敏感信息" in x[1] for x in notices(b))
          and "Abc123456" not in json.dumps(b.bg_read(lead), ensure_ascii=False),
          "Opt 6: a free-text secret ('密码是 …') is not merged, and the user is told so")
    nb, _ = boot("v16_nobatch", WECHAT_BRIDGE_BACKGROUND="1")     # batching off (emergency mode)
    tick(nb)
    l2 = say(nb, LONG)
    rc, out = claim(nb)
    complete(nb, l2, claim_id=out["claim_id"], background=True)
    q2 = say_quote(nb, "改成 tar.gz", l2)
    rc, out = claim(nb)
    check(not out.get("tasks") and nb.load_task(q2)["send"].get("merged_into") == l2
          and (nb.bg_read(l2).get("notes") or [{}])[-1].get("text") == "改成 tar.gz",
          "Opt 3: with batching off a quoted follow-up still goes into the background task", out)


for fn in (t_lane, t_quotes, t_resume_notice_docs, t16_bug1_notes_offset, t16_bug2_finish_gate,
           t16_bug3_quote_diagnosis, t16_bug4_turns, t16_bug5_worktime_doctor,
           t16_opt1_opt7_wakes, t16_opt3_opt4_opt6):
    print(f"--- {fn.__name__}")
    fn()
sys.exit(finish("BACKGROUND v1.5 + v1.6"))
