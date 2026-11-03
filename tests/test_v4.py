#!/usr/bin/env python3
"""v4 tests (offline): the problems behind the 2026-11-01 chat (an 18 s MP3
whose CDN upload was dropped three times; messages that got no WeChat reply;
voice), Muse's four requests (multi-turn context, 状态, direct delivery of
generated files, explicit failure reasons) and the v3 code-review findings.
Where it shows the old behaviour, the same scenario is run on bridge.py.v3.
Run: python3 tests/test_v4.py"""
import base64, hashlib, json, os, shutil, signal, subprocess, sys, tempfile
import time, tracemalloc, types, wave
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_v3 as h  # noqa: E402  fixtures only: load/mk_task/set_creds/cli

NEW, OLD = HERE / "bridge.py", HERE / "tests" / "fixtures" / "bridge.py.v3"
HAVE_OLD = OLD.exists()
TMP = Path(tempfile.mkdtemp(prefix="v4t-"))
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


class FakeILink:
    """bot_post + CDN stand-in: records every call, can drop CDN uploads
    (like the RemoteDisconnected in the screenshot) or refuse voice items."""

    def __init__(self, b, upload_drops=0, refuse_voice=False, upload_ret=None):
        self.b, self.sent, self.calls, self.cdn_calls = b, [], [], 0
        self.upload_drops, self.refuse_voice, self.upload_ret = upload_drops, refuse_voice, upload_ret
        b.bot_post = self.bot_post
        b._cdn_post_file = self.cdn
        b.CDN_RETRY_DELAYS_SEC = (0.01, 0.01)
        b.QUEUE_RETRY_DELAYS_MS = (30, 30, 30, 30, 30)

    def bot_post(self, creds, ep, body, timeout):
        name = ep.rsplit("/", 1)[1]
        self.calls.append((name, body))
        if name == "getuploadurl":
            if self.upload_ret is not None:
                return self.upload_ret
            return {"upload_full_url": f"https://cdn.qq.com/upload?n={len(self.calls)}"}
        if name == "sendmessage":
            item = body["msg"]["item_list"][0]
            if self.refuse_voice and item["type"] == 3:
                return {"ret": -1, "errmsg": "voice not allowed"}
            self.sent.append(item)
            return {}
        return {"ret": 0}

    def cdn(self, url, path, size):
        self.cdn_calls += 1
        if self.upload_drops > 0:
            self.upload_drops -= 1
            raise self.b.ApiError("network", "CDN upload error: closed(RemoteDisconnected: "
                                             "Remote end closed connection without response)")
        return 200, {"x-encrypted-param": "DL"}, b""

    def kinds(self):
        return [it["type"] for it in self.sent]

    def texts(self):
        return [it["text_item"]["text"] for it in self.sent if it["type"] == 1]

    def count(self, name):
        return sum(1 for n, _ in self.calls if n == name)


def say(b, mid, text, quote=None, user="owner"):
    item = {"type": 1, "text_item": {"text": text}}
    if quote:
        item["ref_msg"] = {"message_item": {"type": 1, "text_item": {"text": quote}}}
    b.ingest_message({"ilink_user_id": "owner"}, {
        "message_id": mid, "from_user_id": user, "message_type": 1,
        "context_token": "CTX", "item_list": [item]})
    return b.load_task(mid)


def notices(b):
    return [b.load_task(t["message_id"]) for t in b.scan_task_index() if t.get("kind") == "notice"]


def complete_args(task_id, **kw):
    a = dict(task_id=task_id, result=None, result_file=None, error=None, attach=[],
             attach_as="auto", wait_attachments=0, force=False)
    a.update(kw)
    return a


def wav(path, seconds, rate=24000):
    with wave.open(str(path), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(b"\x10\x00\xf0\xff" * (rate * seconds // 2))
    return path


def mp3(path, seconds):
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi",
                    "-i", f"sine=frequency=440:duration={seconds}", "-q:a", "9", str(path)],
                   check=True)
    return path


def fake_silk_module():
    m = types.ModuleType("pysilk")

    def encode(fi, fo, rate, bitrate, *a, **k):
        pcm = fi.read()
        fo.write(b"\x02#!SILK_V3" + hashlib.sha256(pcm).digest() * 4)

    def decode(fi, fo, rate, *a, **k):
        assert fi.read().startswith(b"#!SILK_V3")
        fo.write(b"\x00\x01" * rate * 2)                    # 2 s of audio
    m.encode, m.decode = encode, decode
    return m


# ---------------------------------------------------------------- 1 silent loss
def g_errcode():
    b = load("err")
    b.bot_post = lambda c, ep, body, timeout: {"errcode": -14, "errmsg": "session timeout"}
    h.mk_task(b, 1, "waiting_send", result_text="hi")
    b.attempt_send("1")
    t = b.load_task("1")
    detail = ""
    if HAVE_OLD:
        o = load("err_old", OLD)
        o.bot_post = b.bot_post
        h.mk_task(o, 1, "waiting_send", result_text="hi")
        o.attempt_send("1")
        detail = f"v3 on the same reply: status={o.load_task('1')['status']} (nothing reached the user)"
    retry_min = (t["send"].get("next_retry_at", 0) - now_ms()) / 60000
    check(t["status"] == "send_failed" and t["send"].get("deferred") == "session"
          and t["send"]["attempts"] == 0 and retry_min > 9,
          "sendmessage answered {errcode:-14}: NOT recorded as sent; waits for the session "
          "without using up retries", detail)
    b2 = load("err2")
    b2.bot_post = lambda c, ep, body, timeout: {}
    h.mk_task(b2, 2, "waiting_send", result_text="hi")
    ok = b2.attempt_send("2").get("ok") and b2.load_task("2")["status"] == "sent"
    b3 = load("err3")
    b3.bot_post = lambda c, ep, body, timeout: {"ret": -14}
    f = TMP / "x.bin"
    f.write_bytes(os.urandom(1000))
    try:
        b3._upload_media_to_cdn({"bot_token": "t"}, f, "owner", 3, 25 << 20)
        code = None
    except b3.ApiError as e:
        code = e.code
    check(ok and code == -14, "a plain {} is success; getuploadurl -14 surfaces as code -14 "
                              "(v3: 'returned no upload URL')")


def g_ingest():
    b = load("ing")
    creds = {"ilink_user_id": "owner"}

    def msg(mid, items, **kw):
        m = {"message_id": mid, "from_user_id": "owner", "context_token": "C", "item_list": items}
        m.update(kw)
        return m
    b.ingest_message(creds, msg("n1", [{"type": 1, "text_item": {"text": "没有 message_type 字段"}}]))
    b.ingest_message(creds, msg("n2", [{"type": 1, "text_item": {"text": "echo"}}], message_type=2))
    img = {"type": 2, "image_item": {"media": {"encrypt_query_param": "EQP", "aes_key": "QUJD"}}}
    b.ingest_message(creds, msg("n3", [{"type": 1, "text_item": {"text": "这张图里写的什么"},
                                        "ref_msg": {"message_item": img}}], message_type=1))
    b.ingest_message(creds, msg("n4", [{"type": 3, "voice_item": {
        "media": {"encrypt_query_param": "V"}, "encode_type": 6, "playtime": 3200}}], message_type=1))
    b.ingest_message(creds, msg("n5", [{"type": 3, "voice_item": {"playtime": 900}}], message_type=1))
    b.ingest_message(creds, msg("n6", [{"type": 9}], message_type=1))
    t = {k: b.load_task(k) for k in ("n1", "n2", "n3", "n4", "n5", "n6")}
    hist = [e["id"] for e in b.history_read() if e["role"] == "user"]
    detail = ""
    if HAVE_OLD:
        o = load("ing_old", OLD)
        o.ingest_message(creds, msg("n1", [{"type": 1, "text_item": {"text": "x"}}]))
        detail = f"v3: message without message_type stored={o.load_task('n1') is not None} (silently dropped)"
    check(t["n1"] and t["n1"]["status"] == "pending_execution" and t["n2"] is None
          and t["n3"]["kind"] == "image" and t["n3"]["status"] == "downloading" and t["n3"]["quoted_media"]
          and t["n3"]["text"] == "这张图里写的什么"
          and t["n4"]["kind"] == "voice" and t["n4"]["status"] == "downloading"
          and t["n5"]["result_text"] == b.VOICE_UNUSABLE_TEXT and t["n6"]["result_text"] == b.UNSUPPORTED_TEXT
          and hist == ["n1", "n3", "n4", "n5", "n6"],
          "ingest: missing message_type accepted (bot echo still ignored), quoted picture downloaded, "
          "untranscribed voice downloaded, explicit texts for unusable voice/unknown types, history kept",
          detail)


# ---------------------------------------------------------------- 2 commands
def g_commands():
    b = load("cmd")
    fk = FakeILink(b)
    h.mk_task(b, 10, "send_uncertain", from_user_id="owner", result_text="这是之前那条回复",
              send={"attempts": 1, "no_auto_retry": True, "uncertain": True, "done_upto": 0})
    song = TMP / "song.mp3"
    song.write_bytes(os.urandom(300_000))
    it = b.enqueue_attachment("owner", song)
    q = b.load_qitem(it["qid"])
    q.update(status="failed", final_code="exhausted")
    b.save_qitem(q)
    st, hp, rs = say(b, "c1", "状态"), say(b, "c2", "帮助"), say(b, "c3", "重发")
    t10, q = b.load_task("10"), b.load_qitem(it["qid"])
    pending_exec = [x for x in b.scan_task_index() if x["status"] == "pending_execution"]
    marked = (t10["status"] == "send_failed" and t10["send"]["attempts"] == 0 and q["status"] == "pending")
    b.flush_outbox()
    b.process_due_queue()
    delivered = b.load_task("10")["status"] == "sent" and b.load_qitem(it["qid"])["status"] == "sent"
    text = st["result_text"]
    check(st["kind"] == "command" and "微信桥接状态" in text and "· 整体：" in text
          and "SECRET-TOKEN" not in text and "owner" not in text and hp["result_text"] == b.HELP_TEXT
          and "开始重发 2 项" in rs["result_text"] and marked and not pending_exec and delivered,
          "状态/帮助/重发 answered by the bridge itself (no executor wake): status has no secrets; "
          "重发 re-queues the uncertain reply and the failed file, both then delivered",
          "状态 reply:\n      " + text.replace("\n", "\n      "))
    a1 = b.enqueue_attachment("owner", song)
    a2 = b.enqueue_attachment("owner", TMP / "x.bin")
    for x in (a1, a2):
        y = b.load_qitem(x["qid"])
        y["status"] = "failed"
        b.save_qitem(y)
    nid = b.create_notice("owner", "文件《x.bin》没能发出去：网络连接中断。回复「重发」我再试一次。",
                          reason="queue_failed", about={"qid": a2["qid"]})
    rs2 = say(b, "c4", "重发", quote=b.load_task(nid)["result_text"])
    check("开始重发 1 项" in rs2["result_text"] and "x.bin" in rs2["result_text"]
          and b.load_qitem(a1["qid"])["status"] == "failed" and b.load_qitem(a2["qid"])["status"] == "pending",
          "「重发」 quoting one failure notice resends only that item")


# ---------------------------------------------------------------- 3 explicit failures
def g_media_fail():
    b = load("mf")
    b.MEDIA_RETRY_DELAYS_SEC = (0.01,) * 5
    creds = {"ilink_user_id": "owner"}
    key_b64 = base64.b64encode(os.urandom(16)).decode()

    def file_msg(mid, size, text=None):
        items = [{"type": 4, "file_item": {"file_name": "big.zip", "len": str(size),
                                            "media": {"encrypt_query_param": "Q", "aes_key": key_b64}}}]
        if text:
            items.append({"type": 1, "text_item": {"text": text}})
        return {"message_id": mid, "from_user_id": "owner", "message_type": 1, "item_list": items}

    def run(mid, m):
        b.ingest_message(creds, m)
        b.process_media_task(mid)
        return b.load_task(mid)
    t1 = run("m1", file_msg("m1", 300 << 20))
    b._fetch_to_file = lambda url, dst, timeout, max_bytes: Path(dst).write_bytes(os.urandom(4103))
    t2 = run("m2", file_msg("m2", 4096))
    calls = []

    def flaky(item, mid):
        calls.append(1)
        raise b.ApiError("network", "reset(ConnectionResetError)")
    b.download_image = flaky
    img = {"message_id": "m3", "from_user_id": "owner", "message_type": 1,
           "item_list": [{"type": 2, "image_item": {"media": {"encrypt_query_param": "Q"}}}]}
    t3 = run("m3", img)
    b.download_image = lambda item, mid: (_ for _ in ()).throw(RuntimeError("weird"))
    t4 = run("m4", {**img, "message_id": "m4"})
    t5 = run("m5", file_msg("m5", 300 << 20, text="帮我总结一下"))
    check(t1["status"] == "waiting_send" and "300.0 MB" in t1["result_text"] and "100.0 MB" in t1["result_text"]
          and t2["status"] == "waiting_send" and t2["result_text"] == b.FILE_DECRYPT_HINT
          and t3["status"] == "waiting_send" and "试了 6 次" in t3["result_text"] and len(calls) == 6
          and t4["status"] == "pending_execution" and "weird" in t4["media_error"] and not t4.get("media_error_hint")
          and t5["status"] == "pending_execution" and "超过了" in t5.get("media_error_hint", ""),
          "inbound media failures: too large (refused before download, real size), CDN decrypt bug, "
          "network exhausted -> the bridge tells the user why and what to do; unknown errors and "
          "messages with text still go to the executor",
          f"too large: {t1['result_text']}\n      network: {t3['result_text']}")


# ---------------------------------------------------------------- 4 outbound queue
def g_queue():
    b = load("q")
    fk = FakeILink(b, upload_drops=5)
    audio = TMP / "audio.mp3"
    audio.write_bytes(os.urandom(288_000))                 # ~18 s at 128 kbit/s
    h.mk_task(b, 20, "executing", from_user_id="owner", text="你做成音频发文件给我吧")
    rc, out = h.cli(b.cmd_complete, **complete_args("20", result="音频做好了，文件随后发来。",
                                                       attach=[str(audio)]))
    first = out["attachments"][0]
    for _ in range(60):
        b.process_due_queue()
        if b.load_qitem(first["queue_id"])["status"] == "sent":
            break
        time.sleep(0.05)
    q = b.load_qitem(first["queue_id"])
    detail = ""
    if HAVE_OLD:
        o = load("q_old", OLD)
        o.bot_post = lambda c, ep, body, timeout: {"upload_full_url": "https://cdn.qq.com/u"}
        o.CDN_RETRY_DELAYS_SEC = (0.01, 0.01)
        drops = {"n": 5}

        def ocdn(url, path, size):
            if drops["n"]:
                drops["n"] -= 1
                raise o.ApiError("network", "CDN upload error: closed(RemoteDisconnected)")
            return 200, {"x-encrypted-param": "DL"}, b""
        o._cdn_post_file = ocdn
        try:
            o._upload_media_to_cdn({"bot_token": "t"}, audio, "owner", 3, 25 << 20)
            res = "sent"
        except o.ApiError:
            res = f"failed after {5 - drops['n']} dropped uploads, file lost to the user until they say 'resend'"
        detail = f"v3 with the same 5 drops: {res}"
    check(rc == 0 and out["send"]["ok"] and first["status"] == "pending" and q["status"] == "sent"
          and fk.kinds() == [1, 4] and fk.count("getuploadurl") == 2 and fk.cdn_calls == 6
          and not Path(q["path"]).exists() and not notices(b),
          "complete --attach: text first; 5 dropped CDN uploads (the screenshot's RemoteDisconnected) "
          "-> round 1 fails, round 2 with fresh upload params delivers the file, nobody had to say 'resend'",
          detail)

    b2 = load("qx")
    fk2 = FakeILink(b2, upload_drops=10 ** 6)
    it = b2.enqueue_attachment("owner", audio)
    for _ in range(300):
        b2.process_due_queue()
        if b2.load_qitem(it["qid"])["status"] == "failed":
            break
        time.sleep(0.02)
    q2 = b2.load_qitem(it["qid"])
    nt = (notices(b2) or [{}])[0].get("result_text", "")
    fk2.upload_drops = 0
    say(b2, "r1", "重发")
    b2.process_due_queue()
    check(q2["status"] == "failed" and q2["rounds"] == 6 and "audio.mp3" in nt and "RemoteDisconnected" in nt
          and "重发" in nt and b2.load_qitem(it["qid"])["status"] == "sent",
          "upload never gets through: 6 rounds, then the bridge tells the user the reason; "
          "「重发」 after the network recovers delivers it", f"notice: {nt}")

    b3 = load("qu")
    fk3 = FakeILink(b3)
    real = fk3.bot_post

    def drop_msg(creds, ep, body, timeout):
        if ep.endswith("sendmessage"):
            raise b3.ApiError("network", "closed(RemoteDisconnected)")
        return real(creds, ep, body, timeout)
    b3.bot_post = drop_msg
    it3 = b3.enqueue_attachment("owner", audio)
    b3.process_due_queue()
    q3 = b3.load_qitem(it3["qid"])
    b4 = load("qr")
    FakeILink(b4, upload_ret={"ret": -2, "errmsg": "rate limited"})
    it4 = b4.enqueue_attachment("owner", audio)
    b4.process_due_queue()
    q4 = b4.load_qitem(it4["qid"])
    check(q3["status"] == "uncertain" and "不确定你收到没有" in (notices(b3) or [{}])[0].get("result_text", "")
          and q4["status"] == "pending" and q4["rounds"] == 0 and q4["deferrals"] == 1
          and q4["next_at"] - now_ms() > 80_000,
          "media message dropped mid-send -> uncertain + notice, never auto-repeated; "
          "-2 at getuploadurl -> waits 90 s without using up a round")


def g_voice():
    # Native-voice scenarios below test the opt-in path, so they run with
    # WECHAT_BRIDGE_NATIVE_VOICE=1. The live default (file card) is checked
    # separately at the end of this function with the variable unset.
    prev_voice_env = os.environ.get("WECHAT_BRIDGE_NATIVE_VOICE")
    os.environ["WECHAT_BRIDGE_NATIVE_VOICE"] = "1"
    sys.modules["pysilk"] = fake_silk_module()
    try:
        b = load("voice")
        fk = FakeILink(b)
        clip = mp3(TMP / "reply.mp3", 18)
        rc, out = h.cli(b.cmd_send_media, task_id=None, to_user=None, file=[str(clip)],
                        as_kind="voice", caption=None, context_token=None, wait=0)
        vi = next((x["voice_item"] for x in fk.sent if x["type"] == 3), {})
        up = [body for n, body in fk.calls if n == "getuploadurl"]
        check(rc == 0 and vi.get("encode_type") == 6 and vi.get("sample_rate") == 24000
              and 17_500 <= vi.get("playtime", 0) <= 18_500 and up and up[0]["media_type"] == 4,
              "voice note: 18 s MP3 -> ffmpeg -> SILK -> media_type 4 upload -> voice_item "
              "(encode_type 6, 24 kHz, playtime set)", f"voice_item fields: { {k: v for k, v in vi.items() if k != 'media'} }")

        b2 = load("voice2")
        fk2 = FakeILink(b2, refuse_voice=True)
        rc2, out2 = h.cli(b2.cmd_send_media, task_id=None, to_user=None, file=[str(clip)],
                          as_kind="voice", caption=None, context_token=None, wait=0)
        b2.process_due_queue()
        q = b2.load_qitem(out2["items"][0]["queue_id"])
        long_clip = mp3(TMP / "long.mp3", 61)
        b3 = load("voice3")
        fk3 = FakeILink(b3)
        rc3, out3 = h.cli(b3.cmd_send_media, task_id=None, to_user=None, file=[str(long_clip)],
                          as_kind="voice", caption=None, context_token=None, wait=0)
        check(fk2.kinds() == [4] and q["status"] == "sent" and "server refused" in q.get("fallback", "")
              and b2._voice_refused_recently()
              and out3["items"][0].get("fallback") == "voice->file (too_long)" and fk3.kinds() == [4],
              "server refuses the voice item -> sent as an audio file (remembered 24 h); "
              "a 61 s clip goes out as a file (WeChat voice notes stop at 60 s)")

        key = os.urandom(16)
        silk = b"\x02#!SILK_V3" + os.urandom(3000)
        enc = subprocess.run(["openssl", "enc", "-aes-128-ecb", "-K", key.hex()], input=silk,
                             capture_output=True, check=True).stdout
        b4 = load("voice4")
        b4._fetch_bytes = lambda url, timeout=45, max_bytes=0: enc
        item = {"media": {"encrypt_query_param": "V", "aes_key": base64.b64encode(key).decode()},
                "encode_type": 6, "playtime": 2000}
        r = b4.download_voice(item, "vin1")
        wav_ok = r["voice_wav"] and r["media_path"].endswith(".wav") and Path(r["media_path"]).stat().st_size > 44
    finally:
        sys.modules.pop("pysilk", None)
    b5 = load("voice5")
    fk5 = FakeILink(b5)
    rc5, out5 = h.cli(b5.cmd_send_media, task_id=None, to_user=None, file=[str(clip)],
                      as_kind="voice", caption=None, context_token=None, wait=0)
    b5._fetch_bytes = lambda url, timeout=45, max_bytes=0: enc
    r5 = b5.download_voice(item, "vin2")
    check(wav_ok and r["voice_format"] == "silk" and r5["voice_wav"] is False and r5["voice_note"] == "no_codec"
          and r5["media_path"].endswith(".silk")
          and out5["items"][0].get("fallback") == "voice->file (no_codec)" and fk5.kinds() == [4],
          "inbound untranscribed voice: SILK decoded to WAV when a codec is installed, raw .silk + "
          "voice_note=no_codec without; without a codec a voice request still delivers the audio as a file")
    if prev_voice_env is None:
        os.environ.pop("WECHAT_BRIDGE_NATIVE_VOICE", None)
    else:
        os.environ["WECHAT_BRIDGE_NATIVE_VOICE"] = prev_voice_env
    b6 = load("voice_default")
    fk6 = FakeILink(b6)
    rc6, out6 = h.cli(b6.cmd_send_media, task_id=None, to_user=None, file=[str(clip)],
                      as_kind="voice", caption=None, context_token=None, wait=0)
    check(rc6 == 0 and fk6.kinds() == [4] and not any(x["type"] == 3 for x in fk6.sent),
          "default voice goes out as a file card (native voice off unless "
          "WECHAT_BRIDGE_NATIVE_VOICE=1)")



# ---------------------------------------------------------------- 5 executor contract
def g_complete_guards():
    b = load("cg")
    fk = FakeILink(b)
    h.mk_task(b, 30, "executing")
    rc1, o1 = h.cli(b.cmd_complete, **complete_args("30", result="   "))
    h.mk_task(b, 31, "send_uncertain", result_text="第一段\n第二段",
              send={"done_upto": 3, "attempts": 1, "no_auto_retry": True})
    rc2, o2 = h.cli(b.cmd_complete, **complete_args("31", result="new"))
    h.mk_task(b, 32, "executing")
    rc3, o3 = h.cli(b.cmd_complete, **complete_args("32", result="见附件", attach=["/nonexistent/x.pdf"]))
    h.mk_task(b, 33, "sending", send={"started_at": now_ms()})
    rc4, o4 = h.cli(b.cmd_complete, **complete_args("33", result="x"))
    detail = ""
    if HAVE_OLD:
        o = load("cg_old", OLD)
        got = []
        o.bot_post = lambda c, ep, body, timeout: got.append(body["msg"]["item_list"][0]) or {}
        h.mk_task(o, 30, "executing")
        orc, _ = h.cli(o.cmd_complete, task_id="30", result="", result_file=None, error=None)
        detail = f"v3: empty complete rc={orc}, sent item text={got[0]['text_item']['text']!r}" if got else ""
    check(rc1 == 2 and "empty result" in o1["error"] and rc2 == 2 and "send --task-id" in o2["error"]
          and b.load_task("31")["result_text"] == "第一段\n第二段" and rc3 == 2
          and b.load_task("32")["status"] == "executing" and not b.list_queue()
          and rc4 == 2 and fk.sent == [],
          "complete refuses: an empty reply, clobbering a partly delivered reply, a missing attachment "
          "(nothing changes), a reply that is being sent", detail)


def g_reclaim_progress():
    b = load("rp")
    fk = FakeILink(b)
    old = now_ms() - 11 * 60_000
    h.mk_task(b, 40, "executing", text="做个很复杂的东西", execution={"claimed_at": old, "reclaims": 2})
    h.mk_task(b, 41, "executing", execution={"claimed_at": old, "heartbeat_at": now_ms() - 60_000})
    rc, out = h.cli(b.cmd_claim, limit=5, full=False)
    t40, t41 = b.load_task("40"), b.load_task("41")
    h.mk_task(b, 42, "executing", from_user_id="owner", execution={"claimed_at": now_ms()})
    rcp, op = h.cli(b.cmd_progress, task_id="42", note="正在生成音频，大约 1 分钟", notify=True)
    rcp2, op2 = h.cli(b.cmd_progress, task_id="42", note="还在生成", notify=True)
    detail = ""
    if HAVE_OLD:
        o = load("rp_old", OLD)
        h.mk_task(o, 40, "executing", execution={"claimed_at": old, "reclaims": 2})
        _, oo = h.cli(o.cmd_claim, limit=5, full=False)
        detail = f"v3: the same task claimed again = {[x['message_id'] for x in oo['tasks']]} (no cap, a new LLM run every 10 min)"
    check(out["tasks"] == [] and t40["status"] == "waiting_send" and "试了 3 次" in t40["result_text"]
          and t41["status"] == "executing" and op.get("notice", {}).get("ok")
          and "正在生成音频，大约 1 分钟" in fk.texts() and "notify_skipped" in op2
          and b.load_task("42")["execution"].get("heartbeat_at"),
          "reclaim cap: after 3 executor runs the user is told instead of a 4th run; a progress "
          "heartbeat prevents reclaim; progress --notify reaches the user (max 1/min)", detail)


def g_stale_and_resend():
    b = load("st")
    FakeILink(b)
    proc = subprocess.Popen(["bash", "-c", "exec -a 'python3 bridge.py send' sleep 30"])
    time.sleep(0.2)
    send = {"started_at": now_ms() - 200_000, "progress_at": now_ms() - 130_000, "sender_pid": proc.pid}
    h.mk_task(b, 50, "sending", from_user_id="owner", result_text="长回复", send=send)
    b.recover_stale_sending()
    kept = b.load_task("50")["status"] == "sending"
    detail = ""
    if HAVE_OLD:
        o = load("st_old", OLD)
        h.mk_task(o, 50, "sending", result_text="长回复", send=send)
        o.recover_stale_sending()
        detail = f"v3 with the sender still alive: {o.load_task('50')['status']} (false alarm)"
    proc.kill()
    proc.wait()
    b.recover_stale_sending()
    t = b.load_task("50")
    nt = (notices(b) or [{}])[0].get("result_text", "")
    check(kept and t["status"] == "send_uncertain" and t["send"].get("notified") and "不确定你收到没有" in nt,
          "a long send whose CLI process is alive is left alone; once that process is gone it becomes "
          "uncertain and the user is told", detail)

    b2 = load("rs")

    def down(*a, **k):
        raise b2.ApiError("connect", "refused")
    b2.send_text = down
    rec = {"attempts": 6, "max_attempts": 6, "exhausted": True, "no_auto_retry": True, "not_delivered": True}
    h.mk_task(b2, 61, "send_uncertain", result_text="x", send=dict(rec))
    h.cli(b2.cmd_send, task_id="61")
    t61 = b2.load_task("61")
    detail = ""
    if HAVE_OLD:
        o2 = load("rs_old", OLD)
        o2.send_text = lambda *a, **k: (_ for _ in ()).throw(o2.ApiError("connect", "refused"))
        h.mk_task(o2, 61, "send_uncertain", result_text="x", send=dict(rec))
        h.cli(o2.cmd_send, task_id="61")
        detail = f"v3 after one failed manual resend: {o2.load_task('61')['status']} (budget already spent)"
    check(t61["status"] == "send_failed" and t61["send"]["attempts"] == 1 and t61["send"].get("next_retry_at"),
          "manual resend after exhaustion starts a fresh retry budget", detail)

    b3 = load("rl")
    fk3 = FakeILink(b3)
    me = os.getpid()
    b3.update_heartbeat(serve_pid=me, handoff_pid=me)
    b3.serve_alive = lambda: me
    prev = signal.signal(signal.SIGUSR1, lambda s, f: None)
    try:
        b3.note_rate_limit()
        h.mk_task(b3, 62, "waiting_send", result_text="稍后")
        out = b3.deliver("62")
    finally:
        signal.signal(signal.SIGUSR1, prev)
    check(out.get("pending") and out.get("deferred_until_ms") and fk3.sent == []
          and b3.load_task("62")["status"] == "waiting_send",
          "during a -2 deferral `complete` leaves the reply to serve instead of sending from the CLI")


def g_conversation():
    b = load("conv")
    FakeILink(b)
    say(b, "h1", "帮我写一首关于秋天的诗")
    _, out1 = h.cli(b.cmd_claim, limit=5, full=False)
    h.cli(b.cmd_complete, **complete_args("h1", result="《秋》\n落叶铺满小径，风把黄昏吹远。"))
    h.cli(b.cmd_memo, set="用户在写秋天主题的诗，偏好短句", set_file=None, clear=False)
    time.sleep(0.01)
    say(b, "h2", "改短一点")
    _, out2 = h.cli(b.cmd_claim, limit=5, full=False)
    conv = out2.get("conversation") or {}
    recent = conv.get("recent") or []
    _, hist = h.cli(b.cmd_history, limit=10, hours=72, json=True)
    check("conversation" not in out1 and len(recent) == 2 and "用户: 帮我写一首关于秋天的诗" in recent[0]
          and "Muse: 《秋》" in recent[1] and all("改短一点" not in r for r in recent)
          and conv.get("memo", "").startswith("用户在写") and len(hist["turns"]) == 3,
          "multi-turn context: claim hands over the earlier turns (not the claimed message itself) "
          "plus the executor's memo; `history` shows the WeChat side",
          f"conversation: {json.dumps(conv, ensure_ascii=False)}")


def g_download_memory():
    b = load("dl")
    key = os.urandom(16)
    plain = os.urandom(16 << 20)
    (TMP / "plain").write_bytes(plain)
    enc = TMP / "enc"
    subprocess.run(["openssl", "enc", "-aes-128-ecb", "-K", key.hex(), "-in", str(TMP / "plain"),
                    "-out", str(enc)], check=True)
    item = {"file_name": "big.bin", "len": str(len(plain)),
            "media": {"encrypt_query_param": "Q", "aes_key": base64.b64encode(key).decode()}}
    b._fetch_to_file = lambda url, dst, timeout, max_bytes: shutil.copyfile(enc, dst) and enc.stat().st_size
    tracemalloc.start()
    res = b.download_file(item, "dl1")
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    same = hashlib.md5(Path(res["media_path"]).read_bytes()).digest() == hashlib.md5(plain).digest()
    peak_old = None
    if HAVE_OLD:
        o = load("dl_old", OLD)
        o._fetch_bytes = lambda url, timeout=45, max_bytes=0: enc.read_bytes()
        tracemalloc.start()
        o.download_file(item, "dl1")
        peak_old = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
    check(same and peak < (4 << 20) and (peak_old is None or peak_old > (30 << 20)),
          f"16 MB inbound file: Python heap peak {peak_old >> 20 if peak_old else '?'} MB (v3) -> "
          f"{peak / (1 << 20):.1f} MB (streamed like video), content identical")


if __name__ == "__main__":
    for g in (g_errcode, g_ingest, g_commands, g_media_fail, g_queue, g_voice, g_complete_guards,
              g_reclaim_progress, g_stale_and_resend, g_conversation, g_download_memory):
        try:
            g()
        except Exception as e:  # noqa: BLE001
            import traceback
            traceback.print_exc()
            check(False, f"{g.__name__} crashed: {type(e).__name__}: {e}")
    print("\nALL V4 CHECKS PASSED" if not fails else f"\n{len(fails)} FAILED: {fails}")
    sys.exit(1 if fails else 0)
