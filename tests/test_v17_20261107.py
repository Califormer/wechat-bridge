#!/usr/bin/env python3
"""v1.7 (2026-11-07): quote routing by every id the quote carries (official
inbound.ts falls back from ref_msg.svr_id to message_item.msg_id; ilink-hub
reads message_item.msg_id / message_id), the echo links a server id to the
task that sent it, a sent message is registered even without a server id and
can be matched by its quoted text; finish cannot lose a follow-up that came
in between its two locks; follow-ups follow a task back to the foreground;
essential notices do not fill the hourly cap; the queue and inbox caches never
pin a summary taken while a file was hot. Each check names the v1.6 failure it
covers. Run: python3 tests/test_v17_20261107.py"""
import json
import os
import time

from relay_fixtures import boot, say, flush, claim, complete, lt, tick, notices, check, finish

LONG = "把最新的微信桥去敏开源吧"
_n = [0]


def bgb(name, **env):
    b, clk = boot(name, os.environ.get("WB_TEST_BRIDGE"), WECHAT_BRIDGE_BACKGROUND="1", **env)
    b.batching_on(True)
    tick(b)
    return b, clk


def no_server_ids(b):
    """sendmessage as the official SendMessageResp before 2.4.9: {ret, errmsg} only."""
    def fake(creds, to, text, ctx, client_id=None):
        b.SENT.append(text)
        return {"ret": 0, "_client_id": client_id}
    b.send_text = fake


def ingest(b, items, mid=None, mtype=1, **top):
    _n[0] += 1
    mid = mid or f"v17u{_n[0]}"
    m = {"message_id": mid, "from_user_id": "owner", "message_type": mtype,
         "context_token": f"V17CTX{_n[0]}", "item_list": items, **top}
    b.ingest_message({"ilink_user_id": "owner"}, m)
    return mid


def quote(b, text, ref):
    return ingest(b, [{"type": 1, "text_item": {"text": text}, "ref_msg": ref}])


def write(b, name, body):
    p = b.BASE_DIR / name
    p.write_text(body, encoding="utf-8")
    return str(p)


def t_ref_ids():
    b, _ = bgb("v17_ids")
    big = 18446744073709551557                       # a uint64 above 2**53
    ids = b._ref_ids({"svr_id": "S1", "msg_id": "R9",
                      "message_item": {"msg_id": "I2", "message_id": big, "svr_id": 0,
                                       "extra": {"msg_id": "X3"}}})
    check(ids == ["S1", "I2", str(big), "X3", "R9"],
          "every id of a quote, best first (v1.6 read ref_msg.svr_id only)", ids)
    ids = b._ref_ids({"message_item": {"msg_id": 7.0, "message_id": True}})
    check(ids == [], "floats (digits already lost), booleans and 0 are never ids", ids)
    q = b.resolve_quote([{"type": 1, "ref_msg": {"message_item": {"type": 1, "msg_id": "I2",
                                                                  "text_item": {"text": "原文"}}}}])
    check(q.get("svr_id") == "I2" and q.get("ids") == ["I2"],
          "an inline quote without svr_id still yields an id (official inbound.ts fallback)", q)


def t_quote_own_request_by_item_id():
    b, _ = bgb("v17_item")
    lead = ingest(b, [{"type": 1, "msg_id": "ITEM-LEAD-1", "text_item": {"text": LONG}}])
    flush(b)
    check(b.load_task(lead)["status"] == "background", "the long task went to the background")
    q = quote(b, "打包改成 tar.gz", {"message_item": {"type": 1, "msg_id": "ITEM-LEAD-1",
                                                     "text_item": {"text": LONG}}})
    flush(b)
    e = b.bg_read(lead) or {}
    check(b.load_task(q)["send"].get("merged_into") == lead
          and [n["text"] for n in e.get("notes") or []] == ["打包改成 tar.gz"],
          "R1-1: quoting the request by its item msg_id (no svr_id) routes to the task "
          "(v1.6: an ordinary new task)", (b.load_task(q).get("status"), e.get("notes")))


def t_quote_signal_without_server_id():
    b, _ = bgb("v17_text")
    no_server_ids(b)
    lead = ingest(b, [{"type": 1, "text_item": {"text": LONG}}])
    flush(b)
    sig = notices(b)[-1][0]
    b.deliver(sig)
    rows = b._quote_exec("SELECT id, task FROM msgs WHERE dir='out'", fetch=True) or []
    check(any(r[0].startswith("cid:") and r[1] == sig for r in rows),
          "R1-2: a sent message is registered without a server id, under its client id "
          "(v1.6 registered nothing)", rows)
    shown = b.SENT[-1][:40]                           # what WeChat shows as the quote title
    q = quote(b, "另外 README 也要更新", {"title": shown, "message_item": {"type": 1,
                                                                    "text_item": {"text": shown}}})
    flush(b)
    rc, sh = lt(b, "show", lead)
    lk = sh.get("last_quote_lookup") or {}
    check(b.load_task(q)["send"].get("merged_into") == lead and lk.get("via") == "text"
          and lk.get("found") is True,
          "R1-2: quoting the signal routes by its text when no id is known", lk)
    m = say(b, "桥是干什么用的")
    flush(b)
    rc, out = claim(b)
    complete(b, m, claim_id=out["claim_id"], result="桥把微信消息转给 Muse 处理，再把结果发回来")
    q2 = quote(b, "再说详细点", {"title": "桥把微信消息转给 Muse 处理", "message_item": {
        "type": 1, "text_item": {"text": "桥把微信消息转给 Muse 处理"}}})
    flush(b)
    rc, out = claim(b)
    check([x["message_id"] for x in out.get("tasks") or []] == [q2],
          "quoting an ordinary finished reply by text stays an ordinary follow-up", out)
    complete(b, q2, claim_id=out["claim_id"], result="好")
    short = quote(b, "嗯", {"title": "已转到后台", "message_item": {"type": 1, "text_item": {
        "text": "已转到后台"}}})
    flush(b)
    check((b.load_task(short).get("send") or {}).get("merged_into") is None,
          "a quoted text shorter than QUOTE_TEXT_MATCH_MIN never routes")


def t_text_match_ambiguous():
    b, _ = bgb("v17_amb", WECHAT_BRIDGE_BG_MAX_RUNNING="2")
    no_server_ids(b)
    a = ingest(b, [{"type": 1, "text_item": {"text": LONG}}])
    flush(b)
    b.deliver(notices(b)[-1][0])
    c = ingest(b, [{"type": 1, "text_item": {"text": "把这 30 个文件逐项扫一遍"}}])
    flush(b)
    b.deliver(notices(b)[-1][0])
    common = "是个长任务，已转到后台处理（不占前台，你照常发消息就行）"
    q = quote(b, "改一下", {"title": common, "message_item": {"type": 1, "text_item": {"text": common}}})
    flush(b)
    check((b.load_task(q).get("send") or {}).get("merged_into") is None
          and not (b.bg_read(a) or {}).get("notes") and not (b.bg_read(c) or {}).get("notes"),
          "text that fits two live background tasks routes to neither (no guessing)")


def t_echo_links_task():
    b, _ = bgb("v17_echo")
    no_server_ids(b)
    lead = ingest(b, [{"type": 1, "text_item": {"text": LONG}}])
    flush(b)
    sig = notices(b)[-1][0]
    b.deliver(sig)
    led = b.read_json(b.ECHO_LEDGER_FILE, {}) or {}
    cid = next((k for k, v in led.items() if v.get("task") == sig), None)
    check(bool(cid), "R1-3: the echo ledger knows which task sent a client id (v1.6: no task)", led)
    ingest(b, [{"type": 1, "msg_id": "ECHO-ITEM-1", "text_item": {"text": b.SENT[-1]}}],
           mid="880000000000000001", mtype=2, client_id=cid, to_user_id="owner")
    rec = b.quote_get("ECHO-ITEM-1") or {}
    check(rec.get("task") == sig, "R1-3: the echoed copy is stored with its task", rec)
    q = quote(b, "改成 tar.gz", {"message_item": {"type": 1, "msg_id": "ECHO-ITEM-1"}})
    flush(b)
    rc, sh = lt(b, "show", lead)
    lk = sh.get("last_quote_lookup") or {}
    check(b.load_task(q)["send"].get("merged_into") == lead and lk.get("via") == "task",
          "R1-1 + R1-3: an ID-only quote of the echoed signal routes to the task", lk)


def t_finish_race():
    b, _ = bgb("v17_race")
    lead = say(b, LONG)
    flush(b)
    rc, o = lt(b, "take", lead)
    lt(b, "progress", lead, token=o["token"], stage="s1")
    orig = b.cmd_complete

    def racing(ns):                 # the follow-up lands between finish's two locks
        b.cmd_complete = orig
        _n[0] += 1
        b.ingest_message({"ilink_user_id": "owner"}, {
            "message_id": f"race{_n[0]}", "from_user_id": "owner", "message_type": 1,
            "context_token": "RACE", "item_list": [{"type": 1, "text_item": {"text": "顺便更新 CHANGELOG"},
                                                    "ref_msg": {"svr_id": lead}}]})
        flush(b)
        return orig(ns)
    b.cmd_complete = racing
    rf = write(b, "r.md", "去敏包打好了")
    n0 = len(b.SENT)
    rc, out = lt(b, "finish", lead, token=o["token"], result_file=rf)
    check(rc == 2 and [n["text"] for n in out.get("unread_notes") or []] == ["顺便更新 CHANGELOG"]
          and len(b.SENT) == n0 and b.load_task(lead)["status"] == "background",
          "R2: a follow-up arriving inside finish is handed over, nothing sent "
          "(v1.6 sent the result and dropped it)", (rc, out))
    rc, out = lt(b, "finish", lead, token=o["token"], result_file=rf)
    check(rc == 0 and len(b.SENT) == n0 + 1, "...the next finish sends the one final reply", (rc, out))


def t_followups_to_foreground():
    b, clk = bgb("v17_back")
    lead = say(b, LONG)
    flush(b)
    q = quote(b, "格式改成 tar.gz", {"svr_id": lead})
    flush(b)
    check(b.load_task(q)["send"].get("merged_into") == lead, "the follow-up joined the queued task")
    clk.adv(11)
    b.check_background()
    t = b.load_task(lead)
    check(t["status"] == "pending_execution" and (t.get("bg") or {}).get("state") == "returned",
          "not taken in 10 min: back to the foreground")
    rc, out = claim(b)
    v = (out.get("tasks") or [{}])[0]
    check(v.get("followups") == ["格式改成 tar.gz"] and "followups" in v.get("hint", ""),
          "R2: the follow-up is in the foreground claim (v1.6 dropped it)", v)
    rc, bo = complete(b, lead, claim_id=out.get("claim_id"), background=True)
    check(rc == 2 and "came back from the background" in bo.get("error", ""),
          "S2: a task the background never took is not queued again (no loop)", bo)
    m = say(b, "桥是干什么用的")
    flush(b)
    rc, out = claim(b)
    mine = [x for x in out.get("tasks") or [] if x.get("message_id") == m]
    check(len(mine) == 1 and "followups" not in mine[0] and "hint" not in mine[0],
          "short tasks: the claim view is unchanged", out)


def t_notice_cap():
    b, _ = bgb("v17_cap")
    say(b, "你好")
    flush(b)
    for i in range(b.NOTICE_HOURLY_CAP + 2):
        b.create_notice("owner", f"信号 {i}", reason="progress", essential=True)
    nid = b.create_notice("owner", "文件没发出去，回「重发」再试", reason="queue_failed")
    check(bool(nid), "S1: essential notices do not fill the hourly cap (v1.6 muted this alert)")
    for i in range(b.NOTICE_HOURLY_CAP):
        b.create_notice("owner", f"提醒 {i}", reason="auto_progress")
    check(b.create_notice("owner", "第 13 条", reason="auto_progress") is None,
          "...ordinary notices are still capped")


def t_caches():
    b, _ = bgb("v17_cache")
    lead = say(b, LONG)
    flush(b)
    check(b._bg_lane()["queued"] == [lead], "lane from the entry cache")
    e = b.bg_read(lead)
    e["state"] = "done"
    e["finished_at"] = b.now_ms()
    b._bg_write(e)
    check(b._bg_lane()["queued"] == [] and b.bg_position(lead) == 0,
          "a rewrite is seen at once (a hot file is never served from the cache)")
    # the inbox index: a signature taken within one mtime tick is not trusted
    t = b.load_task(lead)
    p = b.task_path(lead)
    hot = time.time() - 0.03                            # scanned 30 ms after the write
    os.utime(p, (hot, hot))
    os.utime(b.INBOX_DIR, (hot, hot))
    st_f, st_d = os.stat(p), os.stat(b.INBOX_DIR)
    before = {x["message_id"]: x["status"] for x in b.scan_task_index()}
    raw = p.read_text(encoding="utf-8")
    t2 = dict(t, status="x" * len(t["status"]))
    body = json.dumps(t2, ensure_ascii=False, indent=2)
    if len(body.encode()) != len(raw.encode()):
        check(False, "test setup: could not build a same-size rewrite", (len(body), len(raw)))
        return
    with open(p, "r+", encoding="utf-8") as f:            # same inode, same size ...
        f.write(body)
    os.utime(p, ns=(st_f.st_atime_ns, st_f.st_mtime_ns))  # ... same mtime tick
    os.utime(b.INBOX_DIR, ns=(st_d.st_atime_ns, st_d.st_mtime_ns))
    time.sleep(0.25)
    after = {x["message_id"]: x["status"] for x in b.scan_task_index()}
    check(after.get(lead) == t2["status"] and before.get(lead) == t["status"],
          "inbox index: a file rewritten in the same mtime tick is not served stale "
          "(v1.6 pinned the summary taken while it was hot)", (before.get(lead), after.get(lead)))


def t_persistent_index():
    b, _ = bgb("v17_tix")
    if not hasattr(b, "TASK_INDEX_FILE"):
        check(False, "persistent task index (v1.7) not present")
        return
    ids = [say(b, f"历史消息 {i}") for i in range(90)]
    flush(b)
    old = time.time() - 60
    for f in b.INBOX_DIR.glob("*.json"):
        os.utime(f, (old, old))
    b._SCAN_CACHE.clear()
    b._scan_state.update(dir_mtime=None, out=[], full_at=0.0)
    first = {x["message_id"]: x["status"] for x in b.scan_task_index()}
    check(b.TASK_INDEX_FILE.exists() and (b.TASK_INDEX_FILE.stat().st_mode & 0o077) == 0,
          "a scan that parsed 64+ files writes state/task_index.json (0600)")

    def fresh_process_scan():
        n = [0]
        orig = b.read_json

        def counting(path, default=None):
            if str(path).startswith(str(b.INBOX_DIR)):
                n[0] += 1
            return orig(path, default)
        b.read_json = counting
        try:
            b._SCAN_CACHE.clear()
            b._tix.update(loaded=False, dirty=0)
            b._scan_state.update(dir_mtime=None, out=[], full_at=0.0)
            res = {x["message_id"]: x["status"] for x in b.scan_task_index()}
        finally:
            b.read_json = orig
        return res, n[0]
    res, n = fresh_process_scan()
    check(res == first and n == 0, "a new process starts from the index: 0 task files parsed "
          f"(v1.6 parsed all {len(first)})", n)
    t = b.load_task(ids[5])
    t["status"] = "cancelled"
    b.save_task(t)
    os.utime(b.task_path(ids[5]), (old, old))
    res, n = fresh_process_scan()
    check(res.get(ids[5]) == "cancelled" and n == 1,
          "...a changed file is caught by its signature and parsed alone", (res.get(ids[5]), n))
    b.TASK_INDEX_FILE.write_text("{not json", encoding="utf-8")
    res, n = fresh_process_scan()
    check(res.get(ids[5]) == "cancelled" and n == len(res),
          "a corrupt index is ignored (everything parsed, nothing wrong)", n)


if __name__ == "__main__":
    for fn in (t_ref_ids, t_quote_own_request_by_item_id, t_quote_signal_without_server_id,
               t_text_match_ambiguous, t_echo_links_task, t_finish_race,
               t_followups_to_foreground, t_notice_cap, t_caches, t_persistent_index):
        print(f"-- {fn.__name__}")
        try:
            fn()
        except Exception as ex:  # noqa: BLE001 - report, keep going
            check(False, f"{fn.__name__} raised {type(ex).__name__}: {ex}")
    raise SystemExit(finish("v1.7"))
