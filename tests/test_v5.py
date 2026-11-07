#!/usr/bin/env python3
"""v5 tests (offline): reply budget + 24 h window, WeChat Markdown and
fence/table-aware splitting, the quote store, local commands, warm executors
(claim --wait) and burst merging, keep-alive without a proxy, fast connect
retries, Range-resumed downloads, auto-attach, upload shrinking, sd_notify,
and the hook (waiter, burst coalescing, SIGHUP-first recovery). Where it
shows the old behaviour, the same scenario runs on bridge.py.v4.
Run: python3 tests/test_v5.py"""
import hashlib, http.server, json, os, shutil, socket, subprocess, sys
import tempfile, threading, time
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_v3 as h  # noqa: E402  fixtures only

NEW, OLD = HERE / "bridge.py", HERE / "bridge.py.v4"
HAVE_OLD = OLD.exists()
TMP = Path(tempfile.mkdtemp(prefix="v5t-"))
fails = []


def check(cond, msg, detail=""):
    print(("PASS  " if cond else "FAIL  ") + msg + (f"\n      {detail}" if detail else ""))
    if not cond:
        fails.append(msg)


def now_ms():
    return int(time.time() * 1000)


def load(name, src=NEW):
    b = h.load(src, name)
    h.set_creds(b, token="SECRET-TOKEN-123")
    return b


class Fake:
    """bot_post + CDN stand-in enforcing the ClawBot reply budget: `quota`
    sendmessage calls per context_token, then {"ret": -2}."""

    def __init__(self, b, quota=10, minus2=None):
        self.b, self.quota, self.minus2 = b, quota, minus2
        self.used, self.sent, self.calls, self.seq = {}, [], [], 0
        b.bot_post = self.bot_post
        b._cdn_post_file = lambda url, path, size: (200, {"x-encrypted-param": "DL"}, b"")
        b.CDN_RETRY_DELAYS_SEC = (0.01, 0.01)
        b.QUEUE_RETRY_DELAYS_MS = (30,) * 5

    def bot_post(self, creds, ep, body, timeout):
        name = ep.rsplit("/", 1)[1]
        self.calls.append(name)
        if name == "getuploadurl":
            return {"upload_full_url": "https://cdn.qq.com/u"}
        if name != "sendmessage":
            return {"ret": 0}
        if self.minus2 is not None:
            return dict(self.minus2)
        msg = body["msg"]
        tok = msg.get("context_token")
        if self.used.get(tok, 0) >= self.quota:
            return {"ret": -2}
        self.used[tok] = self.used.get(tok, 0) + 1
        self.seq += 1
        self.sent.append((tok, msg["item_list"][0]))
        return {"ret": 0, "message_id": f"srv-{self.seq}"}

    def items(self, tok=None):
        return [it for t, it in self.sent if tok is None or t == tok]

    def texts(self, tok=None):
        return [it["text_item"]["text"] for it in self.items(tok) if it["type"] == 1]


def say(b, mid, text, tok, items=None):
    b.ingest_message({"ilink_user_id": "owner"}, {
        "message_id": mid, "from_user_id": "owner", "message_type": 1,
        "context_token": tok, "item_list": items or [{"type": 1, "text_item": {"text": text}}]})
    return b.load_task(mid)


def complete(b, tid, **kw):
    a = dict(task_id=tid, result=None, result_file=None, error=None, attach=[],
             attach_as="auto", wait_attachments=0, force=False, merge_ids=None, raw=False)
    a.update(kw)
    return h.cli(b.cmd_complete, **a)


def claim(b, **kw):
    a = dict(limit=5, full=False, wait=0, settle_ms=None)
    a.update(kw)
    return h.cli(b.cmd_claim, **a)


def fences_ok(x):
    return sum(1 for ln in x.split("\n") if ln.strip().startswith("```")) % 2 == 0


# ---------------------------------------------------------------- Markdown
def g_markdown():
    b = load("md")
    src = "\n".join(["**粗体** *中文斜体* *english* ***重点*** _下划线_ snake_case_name",
                     "##### 五级标题", "### 三级标题", "- [ ] 待办", "- [x] 完成",
                     "![图](https://x.invalid/a.png) ![本地](/nonexistent/a.png)",
                     "`*保持*` 行内", "```python", "*不动* ##### 不动", "```",
                     "| a | b |", "|---|---|", "| 1 | 2 |"])
    want = "\n".join(["**粗体** 中文斜体 *english* 重点 下划线 snake_case_name",
                      "五级标题", "### 三级标题", "- ☐ 待办", "- ☑ 完成",
                      "[图](https://x.invalid/a.png) 本地",
                      "`*保持*` 行内", "```python", "*不动* ##### 不动", "```",
                      "| a | b |", "|---|---|", "| 1 | 2 |"])
    out = b.render_for_wechat(src)
    check(out == want, "markdown: official filter rules - bold, tables, code kept; emphasis around "
          "CJK, H5/H6 and images adapted; task lists -> ☐/☑; code untouched", "" if out == want else out)
    check(b.render_for_wechat(src, "off") == src, "markdown: WECHAT_BRIDGE_MARKDOWN=off sends the text untouched")


def g_planner():
    b = load("plan")
    code = "\n".join(f"line {i:04d} " + "x" * 30 for i in range(400))
    text = "前言说明。\n\n```python\n" + code + "\n```\n\n结尾说明：这一段不能被吞进代码块。"
    bodies = [p + text[s:e] + q for s, e, p, q in b._plan_parts(text, 0, 4000)]
    lines = [ln for x in bodies for ln in x.split("\n") if ln.startswith("line ")]
    detail = ""
    if HAVE_OLD:
        old = load("plan_old", OLD).split_text(text)
        detail = (f"v4 on the same reply: {len(old)} parts, {sum(1 for x in old if not fences_ok(x))} "
                  "with a cut code fence (the next part's ``` opened a new block that swallowed the text)")
    check(all(fences_ok(x) for x in bodies) and all(x.startswith("```python\n") for x in bodies[1:])
          and len(lines) == 400 and len(set(lines)) == 400 and fences_ok(bodies[-1])
          and bodies[-1].rstrip().endswith("结尾说明：这一段不能被吞进代码块。")
          and all(b._u16(x) <= 4000 for x in bodies),
          f"fence-aware split: {len(bodies)} parts, each valid Markdown (fence closed, reopened with its "
          "language), every code line once, the text after the block stays outside it", detail)
    table = "| 名称 | 说明 |\n|---|---|\n" + "\n".join(f"| r{i} | " + "数据" * 40 + " |" for i in range(80))
    tb = [p + table[s:e] + q for s, e, p, q in b._plan_parts(table, 0, 2000)]
    rows = [ln for x in tb for ln in x.split("\n") if ln.startswith("| r")]
    check(len(tb) > 2 and all(x.startswith("| 名称 | 说明 |\n|---|---|\n") for x in tb)
          and len(rows) == 80 and len(set(rows)) == 80,
          f"table split into {len(tb)} parts: each repeats the header row, every row once")
    emo = "😀" * 3000
    ep = b._plan_parts(emo, 0, 4000)
    check(b._u16("😀") == 2 and len(ep) == 2 and all(b._u16(emo[s:e]) <= 4000 for s, e, _, _ in ep),
          "length counted like the official textChunkLimit (UTF-16 units, an emoji is 2): no part is "
          "longer than what the official plugin sends (<= 12,000 bytes)")


# ---------------------------------------------------------------- reply budget
def g_quota():
    paras = [f"第{i:02d}段 " + "内容" * 900 for i in range(24)]
    long = "\n\n".join(paras)
    b = load("quota")
    f = Fake(b, quota=10)
    say(b, "u1", "写一篇长文", "T1")
    rc, out = complete(b, "u1", result=long)
    t = b.load_task("u1")
    s1 = f.texts("T1")
    parked = (len(s1) == 10 and "回复任意内容" in s1[-1] and t["status"] == "send_failed"
              and t["send"].get("deferred") == "quota" and t["send"].get("parked") and out.get("send_note"))
    c = say(b, "u2", "继续", "T2")
    b.flush_outbox()
    t = b.load_task("u1")
    s2 = f.texts("T2")
    joined = "".join(x.split("\n\n（微信限制")[0] for x in s1 + s2)
    detail = ""
    if HAVE_OLD:
        o = load("quota_old", OLD)
        fo = Fake(o, quota=10)
        say(o, "u1", "写一篇长文", "T1")
        complete(o, "u1", result=long)
        ot = o.load_task("u1")
        detail = (f"v4 on the same reply: {len(fo.texts())} parts arrived, the 11th got -2 and was read as a "
                  f"rate limit (status={ot['status']}, deferred={(ot.get('send') or {}).get('deferred')}); "
                  "nobody told the user the rest was waiting")
    check(parked and c["kind"] == "command" and c["status"] == "sent" and t["status"] == "sent"
          and len(s2) == 2 and all(joined.count(p[:8]) == 1 and p in joined for p in paras)
          and f.used == {"T1": 10, "T2": 2},
          "reply budget: 10 of 12 parts go out, the 10th ends with a one-line note, the rest is parked; "
          "「继续」 resumes it on the new message's budget and costs no reply of its own", detail)
    b2 = load("quota6")
    f2 = Fake(b2, quota=6)
    say(b2, "u1", "写一篇长文", "T1")
    complete(b2, "u1", result=long)
    t1 = b2.load_task("u1")
    say(b2, "u2", "好的", "T2")
    b2.flush_outbox()
    j2 = "".join(f2.texts())
    obs = [v.get("observed_limit") for v in b2._quota_load()["tokens"].values() if v.get("observed_limit")]
    check(t1["send"].get("deferred") == "quota" and b2.load_task("u1")["status"] == "sent"
          and all(j2.count(p[:8]) == 1 for p in paras) and obs == [6],
          "a server that allows only 6: the 7th -2 is classified as the budget (not a rate limit), the "
          "reply resumes on the next message without duplicates, and the real limit (6) is recorded")


def g_attach_quota():
    b = load("attq")
    f = Fake(b, quota=10)
    say(b, "a1", "把 12 个文件发我", "T1")
    files = []
    for i in range(12):
        p = TMP / f"doc{i:02d}.txt"
        p.write_text(f"file {i}\n")
        files.append(str(p))
    complete(b, "a1", result="这是你要的 12 个文件。", attach=files)
    first = (f.texts("T1") or [""])[0]
    n1 = len(f.items("T1"))
    say(b, "a2", "好", "T2")
    b.process_due_queue()
    kinds = [it["type"] for it in f.items()]
    check("3 个文件" in first and n1 == 10 and len(f.items("T2")) == 3 and kinds.count(4) == 12
          and b.queue_counts().get("sent") == 12,
          "text + 12 files on a 10-reply budget: the text and 9 files now, the text says 3 files follow; "
          "they go out after the user's next message", f"first message: {first!r}")


def g_window():
    b = load("win")
    f = Fake(b)
    say(b, "w1", "在吗", "T1")
    d = b.read_json(b.CONTEXT_TOKENS_FILE)
    d["owner"]["at"] = now_ms() - 25 * 3600 * 1000
    b.atomic_write_json(b.CONTEXT_TOKENS_FILE, d)
    h.mk_task(b, "w2", "waiting_send", result_text="一天后的回复", context_token="T1")
    n0 = f.calls.count("sendmessage")
    b.attempt_send("w2")
    t = b.load_task("w2")
    (b.BASE_DIR / "ops").mkdir(exist_ok=True)
    shutil.copy(HERE / "ops/send-direct.py", b.BASE_DIR / "ops/send-direct.py")
    r = subprocess.run([sys.executable, str(b.BASE_DIR / "ops/send-direct.py"), "--message", "报警：测试"],
                       capture_output=True, text=True, timeout=60)
    sd = json.loads((r.stdout.strip().splitlines() or ["{}"])[-1])
    nid = b.create_notice("owner", "提醒", reason="test")
    say(b, "w3", "我回来了", "T2")
    b.flush_outbox()
    check(f.calls.count("sendmessage") == n0 + 1 and t["send"].get("deferred") == "window"
          and t["send"].get("context_expired") and r.returncode == 3 and sd.get("context_expired")
          and nid is None and b.load_task("w2")["status"] == "sent",
          "24 h window closed: no futile sends (v4 retried every 90 s for an hour), send-direct reports "
          "context_expired at once, no notices queued; the user's next message delivers the parked reply")
    b3 = load("rate")
    Fake(b3, minus2={"ret": -2, "errmsg": "rate limited"})
    say(b3, "r1", "hi", "T1")
    h.mk_task(b3, "r2", "waiting_send", result_text="x", context_token="T1")
    b3.attempt_send("r2")
    t3 = b3.load_task("r2")
    first = (t3["send"]["next_retry_at"] - now_ms()) / 1000
    second = (b3.note_rate_limit(1) - now_ms()) / 1000
    check(t3["send"].get("deferred") == "rate_limit" and 85 < first < 95 and 175 < second < 185,
          f"-2 saying 'rate limited' is a real rate limit: back off {first:.0f} s, then {second:.0f} s "
          "(v4: a flat 90 s, up to 40 times)")


# ---------------------------------------------------------------- quotes
def g_quotes():
    b = load("quote")
    Fake(b)
    say(b, "q1", "原文：今天的会议改到下午三点", "T1")
    rc, out = complete(b, "q1", result="回复：甲乙丙丁戊己庚辛")
    srv = out["send"]["server_message_id"]

    def ref(r, text):
        return [{"type": 1, "text_item": {"text": text}, "ref_msg": r}]
    t2 = say(b, "q2", "", "T2", ref({"svr_id": srv}, "这句什么意思"))
    part = {"start": "甲乙", "end": "丙丁", "startindex": 3, "endindex": 7,
            "quotemd5": hashlib.md5("甲乙丙丁".encode()).hexdigest()}
    t3 = say(b, "q3", "", "T3", ref({"svr_id": srv, "partial_text": part}, "这几个字"))
    t4 = say(b, "q4", "", "T4", ref({"svr_id": srv, "partial_text": dict(part, quotemd5="0" * 32)}, "这几个字"))
    t5 = say(b, "q5", "", "T5", ref({"svr_id": "999"}, "这条呢"))
    t6 = say(b, "q6", "", "T6", ref({"svr_id": "q1"}, "我之前说的"))
    detail = ""
    if HAVE_OLD:
        o = load("quote_old", OLD)
        detail = f"v4 on an id-only quote: quote={say(o, 'q2', '', 'T2', ref({'svr_id': srv}, 'x')).get('quote')!r}"
    check(t2["quote"] == "回复：甲乙丙丁戊己庚辛" and t3["quote"] == "甲乙丙丁" and t4["quote"] == "甲乙…丙丁"
          and "本地没有记录" in t5["quote"] and "下午三点" in t6["quote"],
          "quotes by server id: the bot's reply and the user's earlier message are restored; a partial quote "
          "is cut out and md5-checked (mismatch -> only the client's start…end, nothing invented)", detail)
    img = {"type": 2, "image_item": {"media": {"encrypt_query_param": "E", "aes_key": "QUJD"}}}
    say(b, "q7", "", "T7", [img])
    pic = TMP / "q7.jpg"
    pic.write_bytes(b"\xff\xd8\xff" + os.urandom(100))
    b.quote_set_media("q7", str(pic))
    t8 = say(b, "q8", "", "T8", ref({"svr_id": "q7"}, "这张图里写的什么"))
    check(t8["kind"] == "image" and t8["status"] == "pending_execution" and t8["media_path"] == str(pic)
          and t8.get("quote_from_store"),
          "quoting an earlier picture by id: the task gets the stored image at once (no download)")


# ---------------------------------------------------------------- commands, warm executors
def g_commands():
    b = load("cmd")
    Fake(b)
    say(b, "c1", "查一下天气", "T1")
    claim(b)
    c2 = say(b, "c2", "取消", "T2")
    rc_c, out_c = complete(b, "c1", result="晴")
    rc_p, out_p = h.cli(b.cmd_progress, task_id="c1", note=None, notify=False)
    c3 = say(b, "c3", "取消", "T3")
    c4 = say(b, "c4", "进度", "T4")
    c5 = say(b, "c5", "继续", "T5")
    b.history_append("bot", "旧话题的回复")
    c6 = say(b, "c6", "新话题", "T6")
    conv = b.conversation_context(now_ms() + 1000) or {}
    check(c2["kind"] == "command" and "已取消 1 条" in (c2["result_text"] or "")
          and b.load_task("c1")["status"] == "cancelled" and rc_c == 2 and "cancelled" in out_c["error"]
          and rc_p == 3 and out_p.get("cancelled"),
          "「取消」: the running task is cancelled; the executor's complete/progress get cancelled=true")
    check(c3["kind"] == "text" and c5["kind"] == "text" and c4["kind"] == "command"
          and "排队" in (c4["result_text"] or "") and c6["kind"] == "command" and not conv.get("recent"),
          "「取消」/「继续」 with nothing to act on reach Muse as normal messages; 「进度」 lists what waits; "
          "「新话题」 drops the earlier turns from the executor's context")


def g_claim_wait():
    b = load("wait")
    f = Fake(b)
    seen = []

    def later():
        time.sleep(0.4)
        seen.append(b.WAITER_FILE.exists())
        say(b, "k1", "追问一句", "T1")
    th = threading.Thread(target=later)
    t0 = time.monotonic()
    th.start()
    rc, out = claim(b, wait=5, settle_ms=200)
    dt = time.monotonic() - t0
    th.join()
    check([x["message_id"] for x in out["tasks"]] == ["k1"] and dt < 2.0 and seen == [True]
          and not b.WAITER_FILE.exists(),
          f"claim --wait: the warm executor has the follow-up {dt:.1f}s after it began waiting (no hook "
          "tick, no cold start); its waiter file keeps the hook from waking another")

    # 2026-11-06: a burst is coalesced by serve's batch (state/pending.json,
    # 3 s pause) - the claim-side settle window is gone - so this part runs
    # with batching on, as serve does. k1 is answered first (it would
    # otherwise be pulled into the burst's batch: still unanswered).
    complete(b, "k1", result="收到。")
    n0 = len(f.texts())
    b.batching_on()

    def burst():
        time.sleep(0.3)
        say(b, "k2", "第一句", "T2")
        time.sleep(0.3)
        say(b, "k3", "补充第二句", "T3")
    th = threading.Thread(target=burst)
    th.start()
    rc, out = claim(b, wait=5, settle_ms=800)
    th.join()
    b.batching_on(False)
    rc2, out2 = complete(b, "k3", result="两句一起回答。", merge_ids="k2")
    check([x["message_id"] for x in out["tasks"]] == ["k2", "k3"] and "merge_hint" in out
          and out2.get("merged") == ["k2"] and b.load_task("k2")["status"] == "sent"
          and len(f.texts()) == n0 + 1,
          "a burst of two messages is claimed together with a merge_hint; complete --merge-ids answers "
          "both with ONE reply (one reply slot, one coherent answer)")


# ---------------------------------------------------------------- transport
def g_transport():
    h.use_proxy(None)
    srv = h.TLSServer()
    b = load("direct")
    b.PROXY_POOL.init()
    c0, r0 = b._NET_STATS["connects"], b._NET_STATS["reused"]
    for i in range(6):
        b.http_post(srv.base, "ilink/bot/getconfig", {"i": i}, b.post_headers("t"), 5)
    conns, reused = b._NET_STATS["connects"] - c0, b._NET_STATS["reused"] - r0
    detail = ""
    if HAVE_OLD:
        o = load("direct_old", OLD)
        o.PROXY_POOL.init()
        detail = f"v4 without a proxy: pool enabled={o.PROXY_POOL.enabled} (plain urlopen, a handshake per request)"
    check(b.PROXY_POOL.mode == "direct" and conns == 1 and reused == 5 and len(srv.requests) == 6,
          "no proxy configured: the keep-alive pool runs direct - 6 requests over 1 connection", detail)
    os.environ["WECHAT_BRIDGE_FAST_RETRY_MS"] = "10,10,10"
    try:
        b2 = load("fast")
        ids = []

        def post(creds, ep, body, timeout):
            ids.append(body["msg"]["client_id"])
            if len(ids) < 3:
                raise b2.ApiError("connect", "refused(ConnectionRefusedError)")
            return {"ret": 0, "message_id": "m1"}
        b2.bot_post = post
        r = b2.send_text({"bot_token": "t"}, "owner", "hi", "CTX")
    finally:
        os.environ.pop("WECHAT_BRIDGE_FAST_RETRY_MS", None)
    check(len(ids) == 3 and len(set(ids)) == 1 and b2._NET_STATS["fast_retries"] == 2
          and r.get("message_id") == "m1",
          "connect-phase failure (provably not sent): retried in-process at once with the same client_id "
          "(v4: first retry after 10 s)")
    s = b2._sanitize_bot_agent
    check(s("Muse-WeChatBridge/5.0 (env=prod;transport=direct)") == "Muse-WeChatBridge/5.0 (env=prod;transport=direct)"
          and s("中文 坏的") == "OpenClaw" and len(s("A/1 " * 200)) <= 256,
          "bot_agent: UA-style tokens, ASCII, <= 256 bytes, OpenClaw fallback (official sanitizeBotAgent)")


class _Range(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_GET(self):
        s = self.server
        rng = self.headers.get("Range")
        s.ranges.append(rng)
        start = int(rng.split("=")[1].split("-")[0]) if (rng and s.honor) else 0
        body = s.data[start:]
        self.send_response(206 if start else 200)
        if start:
            self.send_header("Content-Range", f"bytes {start}-{len(s.data) - 1}/{len(s.data)}")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if s.cut:
            s.cut = False
            self.close_connection = True
            self.wfile.write(body[:2 * 1024 * 1024 + 7])
            self.wfile.flush()
            self.connection.shutdown(socket.SHUT_RDWR)
            return
        self.wfile.write(body)
        s.served += len(body)


def g_resume():
    data = os.urandom(3 * 1024 * 1024 + 123)
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Range)
    srv.daemon_threads = True
    srv.data, srv.ranges, srv.honor, srv.cut, srv.served = data, [], True, True, 0
    srv.handle_error = lambda *a: None
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_address[1]}/f"
    h.use_proxy(None)
    b = load("resume")
    b.PROXY_POOL.init()
    dst = TMP / ".r1.part"
    err = None
    try:
        b._fetch_to_file(url, dst, 10, 10 ** 8)
    except Exception as e:  # noqa: BLE001
        err = type(e).__name__
    have = dst.stat().st_size if dst.exists() else 0
    b._fetch_to_file(url, dst, 10, 10 ** 8)
    # 2026-11-06: the stand-in adds to `served` AFTER it has written the body, so the client can
    # finish reading first; wait for the counter (<= 2 s) instead of racing it (2 of 60 runs failed
    # on the unmodified s8fix package, tests only).
    for _ in range(100):
        if srv.served == len(data) - have:
            break
        time.sleep(0.02)
    resumed = dst.read_bytes() == data and srv.ranges[-1] == f"bytes={have}-" and srv.served == len(data) - have
    srv.honor = False
    dst.write_bytes(b"junk" * 100)
    b._fetch_to_file(url, dst, 10, 10 ** 8)
    check(err is not None and have >= 2 * 1024 * 1024 and resumed and dst.read_bytes() == data
          and b._NET_STATS["resumed"] == 1,
          f"download cut at {have // 1024} KB ({err}): the retry asks for bytes={have}- and fetches only the "
          f"remaining {(len(data) - have) // 1024} KB (v4 restarted at 0); a server ignoring Range gets a clean full copy")
    srv.shutdown()


# ---------------------------------------------------------------- attachments, misc
def g_auto_attach():
    b = load("auto")
    f = Fake(b)
    say(b, "p1", "画个图给我", "T1")
    img = TMP / "chart.png"
    img.write_bytes(b"\x89PNG\r\n\x1a\n" + os.urandom(2000))
    secret = b.CREDS_FILE
    res = f"图表如下：\n\n![销量图]({img})\n\n凭据不该发：[c]({secret})\n\n*注意* **重点**"
    rc, out = complete(b, "p1", result=res)
    texts = f.texts("T1")
    want = f"图表如下：\n\n销量图\n\n凭据不该发：[c]({secret})\n\n注意 **重点**"
    check(out.get("auto_attached") == ["chart.png"] and texts == [want]
          and [it["type"] for it in f.items("T1")] == [1, 2],
          "![说明](/local/file) in a reply: the picture goes out after the text; bridge state (credentials) "
          "never qualifies; Markdown adapted in the same pass", repr(texts))


def g_compress():
    if not shutil.which("ffmpeg"):
        print("SKIP  upload shrinking: no ffmpeg")
        return
    b = load("cmp")
    # Detail-first thresholds (2026-11-02): only >8 MB images shrink.
    big = TMP / "big.jpg"
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i",
                    "nullsrc=s=5000x5000,geq=random(1)*255:128:128", "-frames:v", "1", "-q:v", "1",
                    str(big)], check=True, timeout=180)
    size = big.stat().st_size
    b.ATTACH_DIR.mkdir(parents=True, exist_ok=True)
    it = {"qid": "qcmp"}
    out = b._maybe_compress(it, big, "image")
    small = out.stat().st_size if out.exists() else 0
    check(size > b.IMAGE_COMPRESS_OVER and out != big and 0 < small < size * 0.85 and it["compressed"] == str(out),
          f"big image shrunk before the upload: {size // 1024} KB -> {small // 1024} KB")
    # Below the threshold the original must go out untouched.
    small_img = TMP / "small.jpg"
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i",
                    "nullsrc=s=800x800,geq=random(1)*255:128:128", "-frames:v", "1", "-q:v", "1",
                    str(small_img)], check=True, timeout=180)
    it2 = {"qid": "qcmp2"}
    check(b._maybe_compress(it2, small_img, "image") == small_img,
          "image below the 8 MB threshold is sent untouched, not shrunk")


def g_video_audio():
    if not shutil.which("ffmpeg"):
        print("SKIP  video audio extraction: no ffmpeg")
        return
    b = load("vaud")
    vid = TMP / "vaud.mp4"
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                    "-f", "lavfi", "-i", "testsrc=duration=2:size=320x240:rate=10",
                    "-f", "lavfi", "-i", "sine=frequency=440:duration=2",
                    "-shortest", str(vid)], check=True, timeout=180)
    out = TMP / "vaud.wav"
    import types as _types
    args = _types.SimpleNamespace(input=str(vid), output=str(out))
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = b.cmd_video_audio(args)
    import json as _json
    res = _json.loads(buf.getvalue())
    check(rc == 0 and res.get("has_audio") is True and out.is_file() and out.stat().st_size > 0,
          "video with an audio track extracts to a non-empty WAV", repr(res))
    silent = TMP / "silent.mp4"
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                    "-f", "lavfi", "-i", "testsrc=duration=1:size=320x240:rate=10",
                    str(silent)], check=True, timeout=180)
    args2 = _types.SimpleNamespace(input=str(silent), output=str(TMP / "silent.wav"))
    buf2 = io.StringIO()
    with contextlib.redirect_stdout(buf2):
        rc2 = b.cmd_video_audio(args2)
    res2 = _json.loads(buf2.getvalue())
    check(rc2 == 0 and res2.get("has_audio") is False,
          "silent video reports has_audio=false instead of a fake transcript", repr(res2))


def g_unknown():
    b = load("unk")
    items = [{"type": 1, "text_item": {"text": "看看这个"}}, {"type": 11, "tool_call_start_item": {}}]
    t = say(b, "x1", "", "T1", items)
    detail = ""
    if HAVE_OLD:
        detail = f"v4: kind={say(load('unk_old', OLD), 'x1', '', 'T1', items)['kind']} (the text got 'cannot handle this type')"
    check(t["kind"] == "text" and t["status"] == "pending_execution" and t.get("unsupported_items") == [11],
          "an unknown item type next to text: the text still reaches Muse", detail)


def g_sdnotify():
    b = load("sd")
    path = TMP / "notify.sock"
    s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    s.bind(str(path))
    s.settimeout(2)
    os.environ["NOTIFY_SOCKET"] = str(path)
    try:
        ok = b._sd_notify("READY=1")
        r1 = s.recv(64)
        b._WD["last"] = 0.0
        b._watchdog_ping()
        r2 = s.recv(64)
    finally:
        os.environ.pop("NOTIFY_SOCKET", None)
        s.close()
    check(ok and r1 == b"READY=1" and r2 == b"WATCHDOG=1" and not b._sd_notify("READY=1"),
          "systemd notify: READY and WATCHDOG datagrams; a no-op outside systemd")


def g_auto_progress_stages():
    b = load("ap")
    Fake(b)
    check(tuple(b.AUTO_PROGRESS_STAGES) == (180, 600),
          "auto-progress stages default to 180/600 s, max 2 notes (opt batch item 1)",
          repr(getattr(b, "AUTO_PROGRESS_STAGES", None)))

    def notices():
        return [s for s in b.scan_task_index() if s.get("kind") == "notice"]

    def backdate(sec):
        t = b.load_task("a1")
        ex = dict(t.get("execution") or {})
        ex["claimed_at"] = now_ms() - sec * 1000
        t["execution"] = ex
        t["received_at"] = now_ms() - sec * 1000
        b.save_task(t)

    say(b, "a1", "慢慢查一个东西", "T1")
    claim(b)
    backdate(70)
    b.check_auto_progress(b.scan_task_index())
    n0 = len(notices())  # before 180 s: no note
    backdate(200)
    b.check_auto_progress(b.scan_task_index())
    n1 = len(notices())
    b.check_auto_progress(b.scan_task_index())
    backdate(500)
    b.check_auto_progress(b.scan_task_index())
    n2 = len(notices())  # before 600 s: still 1
    backdate(650)
    b.check_auto_progress(b.scan_task_index())
    n3 = len(notices())
    b.check_auto_progress(b.scan_task_index())
    n4 = len(notices())
    stage = (b.load_task("a1").get("execution") or {}).get("auto_progress_stage")
    check((n0, n1, n2, n3, n4) == (0, 1, 1, 2, 2) and stage == 2,
          "staged auto-progress: no note before 180 s, one at 180 s, a second at 600 s, "
          "no repeat at the same stage",
          f"notices={n0},{n1},{n2},{n3},{n4} stage={stage}")


# ---------------------------------------------------------------- hook
HOOK = HERE / "hooks/wechat-bridge-inbox.sh"
# wake() exits like the real hook runtime (2026-11-05 audit §8-1: a stub that
# returned let the hook's post-wake woke=1 run here, while live it never ran).
RUNTIME = r'''
hook_state_get() { [[ -f "$HOOK_STATE" ]] && cat "$HOOK_STATE" || echo '{}'; }
hook_state_set() { printf '%s' "$1" > "$HOOK_STATE"; }
log()    { echo "LOG $1" >> "$HOOK_EVENTS"; }
wake()   { echo "WAKE $1 $(printf '%s' "$2" | tr -d ' \n')" >> "$HOOK_EVENTS"; exit 0; }
silent() { echo "SILENT $1" >> "$HOOK_EVENTS"; exit 0; }
'''


class HookEnv:
    def __init__(self):
        self.home = Path(tempfile.mkdtemp(prefix="hook5-"))
        self.b = self.home / "workspace/wechat-bridge"
        for d in ("inbox", "state", "ops"):
            (self.b / d).mkdir(parents=True)
        (self.b / "state/credentials.json").write_text("{}")
        (self.b / "ops/backup.sh").write_text("#!/usr/bin/env bash\n")
        (self.b / "ops/backup.sh").chmod(0o755)
        (self.b / "bridge.py").write_text("import sys\nsys.exit(0)\n")
        (self.b / "wechat-bridge.service").write_text("[Unit]\n")
        self.stub = self.home / "stub"
        self.stub.mkdir()
        if not shutil.which("jq"):
            (self.stub / "jq").write_text(f"#!/bin/sh\nexec {sys.executable} {HERE}/tests/jqshim.py \"$@\"\n")
            (self.stub / "jq").chmod(0o755)
        for name in ("systemctl", "cp"):
            (self.stub / name).write_text(f"#!/bin/sh\necho {name} \"$@\" >> '{self.home}/stubcalls'\n")
            (self.stub / name).chmod(0o755)
        (self.home / "runtime.sh").write_text(RUNTIME)
        self.state, self.events, self.procs = self.home / "hookstate.json", self.home / "events", []

    def serve(self, healthy=True, hup=False):
        if hup:
            code = ("import signal,sys,time\nsignal.signal(signal.SIGHUP, lambda *a: "
                    "open(sys.argv[3], 'a').write('hup\\n'))\ntime.sleep(300)\n")
            p = subprocess.Popen([sys.executable, "-c", code, "bridge.py", "serve", str(self.home / "hups")])
        else:
            p = subprocess.Popen(["bash", "-c", "exec -a 'python3 bridge.py serve' sleep 300"])
        self.procs.append(p)
        time.sleep(0.4)
        (self.b / "state/serve.pid").write_text(str(p.pid))
        now = now_ms()
        hb = {"serve_pid": p.pid, "serve_started_at": now - 600_000,
              "last_poll_ok_at": now - (1000 if healthy else 200_000),
              "poll_fail_streak": 0 if healthy else 5, "last_poll_fail_at": now - 1000}
        if hup:
            hb["hup_pid"] = p.pid
        (self.b / "state/heartbeat.json").write_text(json.dumps(hb))

    def task(self, mid, status, **kw):
        t = {"message_id": str(mid), "status": status, "kind": "text", "text": "x",
             "received_at": now_ms(), "execution": None, "send": None}
        t.update(kw)
        (self.b / "inbox" / f"{mid}.json").write_text(json.dumps(t, indent=2))

    def run(self):
        env = {**os.environ, "HOME": str(self.home), "PATH": f"{self.stub}:{os.environ['PATH']}",
               "HATCH_HOOK_RUNTIME": str(self.home / "runtime.sh"),
               "HOOK_STATE": str(self.state), "HOOK_EVENTS": str(self.events)}
        if self.events.exists():
            self.events.unlink()
        subprocess.run(["bash", str(HOOK)], env=env, capture_output=True, text=True, timeout=120)
        ev = self.events.read_text().splitlines() if self.events.exists() else []
        return [e for e in ev if not e.startswith("LOG")]

    def calls(self):
        p = self.home / "stubcalls"
        return p.read_text() if p.exists() else ""

    def close(self):
        for p in self.procs:
            p.kill()
        shutil.rmtree(self.home, ignore_errors=True)


def wakes(ev):
    return [e for e in ev if e.startswith("WAKE")]


def g_hook():
    e = HookEnv()
    try:
        e.serve()
        e.task(201, "pending_execution")
        w = subprocess.Popen(["sleep", "30"])
        e.procs.append(w)
        (e.b / "state/claim_waiter.txt").write_text(f"{w.pid} {now_ms() + 30000}\n")
        a = e.run()
        (e.b / "state/claim_waiter.txt").unlink()
        b_ = e.run()
        check(not wakes(a) and len(wakes(b_)) == 1,
              "hook A: an executor waiting in claim --wait takes the message - no second agent; without "
              "a waiter the hook wakes one as before")
    finally:
        e.close()
    e = HookEnv()
    try:
        e.serve()
        e.task(301, "pending_execution")
        a = e.run()
        e.task(302, "pending_execution")
        b_ = e.run()
        e.task(301, "executing", execution={"claimed_at": now_ms(), "by": "muse"})
        c = e.run()
        check(len(wakes(a)) == 1 and not wakes(b_) and len(wakes(c)) == 1
              and '"302"' in wakes(c)[0] and '"301"' not in wakes(c)[0],
              "hook B: a burst while the woken executor is still starting is left to it; once it has "
              "claimed, a newcomer gets an executor at once")
    finally:
        e.close()
    e = HookEnv()
    try:
        e.serve(healthy=False, hup=True)
        a = e.run()
        time.sleep(0.3)
        hup1 = (e.home / "hups").read_text() if (e.home / "hups").exists() else ""
        b_ = e.run()
        calls_ab = e.calls()
        st = json.loads(e.state.read_text())
        st["hup_at_ms"] = now_ms() - 25_000
        e.state.write_text(json.dumps(st))
        e.run()
        check(a == ["SILENT wechat bridge reloading proxy"] and hup1 == "hup\n"
              and b_ == ["SILENT wechat bridge waiting for proxy reload"] and "restart" not in calls_ab
              and "systemctl restart wechat-bridge.service" in e.calls(),
              "hook C: polls failing on a live v5 serve -> SIGHUP (re-read proxy.env, no restart, no lost "
              "first poll); a restart only if that has not helped within 20 s", f"{a} {b_} {hup1!r}")
    finally:
        e.close()


if __name__ == "__main__":
    for g in (g_markdown, g_planner, g_quota, g_attach_quota, g_window, g_quotes, g_commands,
              g_claim_wait, g_transport, g_resume, g_auto_attach, g_compress, g_video_audio, g_unknown,
              g_sdnotify, g_auto_progress_stages, g_hook):
        try:
            g()
        except Exception as e:  # noqa: BLE001
            import traceback
            traceback.print_exc()
            check(False, f"{g.__name__} crashed: {type(e).__name__}: {e}")
    print()
    print(f"{len(fails)} FAILED: {fails}" if fails else "ALL V5 CHECKS PASSED")
    sys.exit(1 if fails else 0)
