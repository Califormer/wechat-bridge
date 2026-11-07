#!/usr/bin/env python3
"""Offline tests for bridge.py (no network, no credentials, temp dirs only).
Run:  python3 tests/test_offline.py [--bench]
"""
import importlib.util, io, json, os, shutil, sys, tempfile, threading, time
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
BENCH = "--bench" in sys.argv


def load(src: Path, name: str):
    """Import a copy of bridge.py from a fresh temp dir so BASE_DIR (derived
    from __file__) points at an isolated sandbox."""
    d = Path(tempfile.mkdtemp(prefix="wb-test-"))
    shutil.copy(src, d / "bridge.py")
    spec = importlib.util.spec_from_file_location(name, d / "bridge.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.log = lambda msg: None          # keep test output clean
    mod.TMP = d
    return mod


def mk_task(b, mid, status="pending_execution", kind="text", **kw):
    t = {"message_id": str(mid), "from_user_id": "u", "to_user_id": "bot",
         "session_id": "s", "context_token": "CTX-SECRET", "received_at": int(mid) if str(mid).isdigit() else 0,
         "kind": kind, "text": f"hello {mid}", "status": status,
         "raw_items": [{"type": 1, "text_item": {"text": "x" * 1500}}],
         "result_text": None, "execution": None, "send": None}
    t.update(kw)
    b.save_task(t)
    return t


passed = []
def ok(name):
    passed.append(name); print(f"PASS  {name}")


# ---------------------------------------------------------------- 1
def test_heartbeat_threads():
    b = load(HERE / "bridge.py", "b_new")
    errs = []
    def worker(i):
        try:
            for j in range(150):
                b.update_heartbeat(**{f"k{i}": j})
        except BaseException as e:      # noqa
            errs.append(repr(e))
    ts = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    [t.start() for t in ts]; [t.join() for t in ts]
    hb = json.loads(b.HEARTBEAT_FILE.read_text())
    assert not errs, errs[:2]
    assert all(hb.get(f"k{i}") == 149 for i in range(8)), hb
    ok("heartbeat: 8 threads x150 updates, no errors, no lost fields")

    # same hammer on the ORIGINAL code, to show the bug was real
    # (v1 fixture lives in tests/fixtures/ in the published repo;
    # archive-old-versions/ on a live checkout holds the same file)
    o = load(HERE / "tests" / "fixtures" / "bridge.py.v1", "b_old")
    errs2 = []
    def worker2(i):
        try:
            for j in range(150):
                o.update_heartbeat(**{f"k{i}": j})
        except BaseException as e:      # noqa
            errs2.append(type(e).__name__)
    ts = [threading.Thread(target=worker2, args=(i,)) for i in range(8)]
    [t.start() for t in ts]; [t.join() for t in ts]
    hb2 = json.loads(o.HEARTBEAT_FILE.read_text()) if o.HEARTBEAT_FILE.exists() else {}
    lost = sum(1 for i in range(8) if hb2.get(f"k{i}") != 149)
    print(f"      (original code, same load: {len(errs2)} exceptions, {lost}/8 fields lost/stale)")


# ---------------------------------------------------------------- 2
def test_index_semantics():
    b = load(HERE / "bridge.py", "b_idx")
    for i in range(1, 6):
        mk_task(b, i)
    idx = b.scan_task_index()
    assert [t["message_id"] for t in idx] == ["1", "2", "3", "4", "5"]
    # change one file -> reflected (racy-window path: dir just modified)
    t = b.load_task("3"); t["status"] = "waiting_send"; b.save_task(t)
    st = {x["message_id"]: x["status"] for x in b.scan_task_index()}
    assert st["3"] == "waiting_send" and st["1"] == "pending_execution"
    # delete one + add one after the directory has "settled" (fast path armed)
    time.sleep(2.2)
    assert len(b.scan_task_index()) == 5          # arms fast path
    b.task_path("1").unlink(); mk_task(b, 9)
    ids = [x["message_id"] for x in b.scan_task_index()]
    assert "1" not in ids and "9" in ids, ids
    # corrupt file is skipped, never raises
    (b.INBOX_DIR / "bad.json").write_text("{not json")
    (b.INBOX_DIR / "x.json.corrupt").write_text("{}")
    assert all(x["message_id"] != "bad" for x in b.scan_task_index())
    ok("task index: add/modify/delete/corrupt all reflected correctly")


def test_index_bench():
    n = 2000
    res = {}
    for name, src in (("old", HERE / "tests" / "fixtures" / "bridge.py.v1"), ("new", HERE / "bridge.py")):
        b = load(src, "b_" + name)
        for i in range(1, n + 1):
            mk_task(b, i, status="sent")
        time.sleep(2.1)
        if name == "old":
            t0 = time.perf_counter()
            for _ in range(5):            # 5 loop ticks = 10 scans (recover + flush)
                b.list_tasks(); b.list_tasks()
            res[name] = (time.perf_counter() - t0) / 5
        else:
            b.scan_task_index()
            t0 = time.perf_counter()
            for _ in range(200):
                b.scan_task_index(); b.scan_task_index()
            res[name] = (time.perf_counter() - t0) / 200
    print(f"      sender-loop cost per 1s tick with {n} tasks: "
          f"old {res['old']*1000:.1f} ms  ->  new {res['new']*1000:.3f} ms "
          f"({res['old']/max(res['new'],1e-9):.0f}x)")
    assert res["new"] < res["old"] / 20
    ok("index bench: idle sender tick >20x cheaper")


# ---------------------------------------------------------------- 3
def fake_msg(mid, items, sender="owner"):
    return {"message_id": mid, "from_user_id": sender, "message_type": 1,
            "to_user_id": "bot", "session_id": "s", "context_token": "c",
            "item_list": items}


def test_ingest_async_media():
    b = load(HERE / "bridge.py", "b_media")
    creds = {"ilink_user_id": "owner"}
    calls = []
    def slow_video(item, mid):
        calls.append(mid); time.sleep(1.0)
        return {"media_path": f"/m/{mid}.mp4", "media_bytes": 123, "play_length": 7}
    b.download_video = slow_video
    b.download_image = lambda item, mid: (_ for _ in ()).throw(RuntimeError("boom"))
    b._stop.clear()
    for _ in range(2):
        threading.Thread(target=b.media_worker_loop, daemon=True).start()

    # text is immediate
    b.ingest_message(creds, fake_msg("101", [{"type": 1, "text_item": {"text": "hi"}}]))
    assert b.load_task("101")["status"] == "pending_execution"

    t0 = time.perf_counter()
    b.ingest_message(creds, fake_msg("102", [{"type": 5, "video_item": {"play_length": 7}}]))
    dt = time.perf_counter() - t0
    assert dt < 0.3, f"ingest blocked {dt:.2f}s"
    assert b.load_task("102")["status"] == "downloading"
    # duplicate redelivery while downloading: no second download
    b.ingest_message(creds, fake_msg("102", [{"type": 5, "video_item": {}}]))
    # failing image -> pending_execution with media_error (old behaviour)
    b.ingest_message(creds, fake_msg("103", [{"type": 2, "image_item": {}}]))
    deadline = time.time() + 5
    while time.time() < deadline and any(
            b.load_task(m)["status"] == "downloading" for m in ("102", "103")):
        time.sleep(0.05)
    v, im = b.load_task("102"), b.load_task("103")
    assert v["status"] == "pending_execution" and v["media_path"] == "/m/102.mp4" and v["play_length"] == 7
    assert im["status"] == "pending_execution" and im["media_path"] is None and "boom" in im["media_error"]
    assert calls == ["102"], calls
    # unsupported (voice) unchanged
    b.ingest_message(creds, fake_msg("104", [{"type": 3, "voice_item": {}}]))
    assert b.load_task("104")["status"] == "waiting_send"
    # unauthorized sender ignored
    b.ingest_message(creds, fake_msg("105", [{"type": 1, "text_item": {"text": "x"}}], sender="evil"))
    assert b.load_task("105") is None
    b._stop.set()
    ok(f"ingest: media no longer blocks poll (ingest {dt*1000:.0f} ms vs 1 s download), dedupe-before-download, error path kept")


def test_recover_downloading():
    b = load(HERE / "bridge.py", "b_rec")
    b._stop.clear()
    mk_task(b, 7, status="downloading", kind="file",
            raw_items=[{"type": 4, "file_item": {"file_name": "a.pdf"}}])
    b.MEDIA_DIR.mkdir(exist_ok=True)
    (b.MEDIA_DIR / ".7.part").write_bytes(b"junk")
    b.download_file = lambda item, mid: {"media_path": "/m/a.pdf", "media_bytes": 9, "file_name": "a.pdf"}
    threading.Thread(target=b.media_worker_loop, daemon=True).start()
    b.recover_downloading()
    for _ in range(100):
        if b.load_task("7")["status"] != "downloading": break
        time.sleep(0.05)
    t = b.load_task("7")
    assert t["status"] == "pending_execution" and t["file_name"] == "a.pdf"
    # v5: the partial ciphertext is kept for an HTTP Range resume; it is removed
    # after a successful download (_fetch_decrypt) or after 24 h (_prune_partials).
    assert (b.MEDIA_DIR / ".7.part").exists() == hasattr(b, "_prune_partials")
    b._stop.set()
    ok("restart recovery: interrupted download resumed and completed")


# ---------------------------------------------------------------- 4
class Boom(BaseException):
    pass


def run_receiver(b, batches, ingest):
    """Drive receiver_loop with scripted getUpdates results."""
    seq = iter(batches)
    def fake_get_updates(creds, cursor, timeout_ms):
        try:
            return next(seq)
        except StopIteration:
            b._stop.set(); raise Boom()
    b.get_updates = fake_get_updates
    b.notify = lambda creds, start: True
    b.ingest_message = ingest
    b._stop.clear()
    try:
        b.receiver_loop({"bot_token": "t"})
    except Boom:
        pass


def test_cursor_after_ingest():
    b = load(HERE / "bridge.py", "b_cur")
    b.atomic_write_json(b.CURSOR_FILE, {"get_updates_buf": "C0"})
    seen = {}
    def crashing_ingest(creds, m):
        seen["cursor_on_disk_during_ingest"] = json.loads(b.CURSOR_FILE.read_text())["get_updates_buf"]
        raise Boom()                    # process dies mid-batch
    try:
        run_receiver(b, [{"ret": 0, "msgs": [{"message_id": "1"}], "get_updates_buf": "C1"}], crashing_ingest)
    except Boom:
        pass
    # Boom is BaseException: propagates through `except Exception`
    assert json.loads(b.CURSOR_FILE.read_text())["get_updates_buf"] == "C0", "cursor advanced before ingest finished"
    assert seen["cursor_on_disk_during_ingest"] == "C0"

    stored = []
    b2 = load(HERE / "bridge.py", "b_cur2")
    run_receiver(b2, [{"ret": 0, "msgs": [{"message_id": "1"}, {"message_id": "2"}], "get_updates_buf": "C1"},
                      {"ret": 0, "msgs": [], "get_updates_buf": "C1"}],
                 lambda c, m: stored.append(m["message_id"]))
    assert stored == ["1", "2"]
    assert json.loads(b2.CURSOR_FILE.read_text())["get_updates_buf"] == "C1"
    ok("cursor: persisted only after batch ingested (crash mid-ingest => message redelivered, not lost)")


# ---------------------------------------------------------------- 5
def test_claim_view():
    b = load(HERE / "bridge.py", "b_claim")
    mk_task(b, 1)
    mk_task(b, 2, kind="video", media_path="/m/2.mp4", media_bytes=5, play_length=9,
            raw_items=[{"type": 5, "video_item": {"media": {"aes_key": "SECRET-KEY"}}}])
    stale = mk_task(b, 3, status="executing", execution={"claimed_at": int(time.time() * 1000) - 11 * 60 * 1000})
    def claim(limit, full=False):
        buf = io.StringIO()
        with redirect_stdout(buf):
            b.cmd_claim(SimpleNamespace(limit=limit, full=full))
        return json.loads(buf.getvalue())["tasks"]
    got = claim(5)
    assert [t["message_id"] for t in got] == ["1", "2", "3"], got      # stale one reclaimed, oldest first
    assert got[0] == {"message_id": "1", "kind": "text", "text": "hello 1"}
    raw = json.dumps(got)
    assert "SECRET-KEY" not in raw and "CTX-SECRET" not in raw and "raw_items" not in raw
    assert got[1]["media_path"] == "/m/2.mp4" and got[1]["play_length"] == 9 and got[1]["media_bytes"] == 5
    assert b.load_task("1")["status"] == "executing"
    assert claim(5) == []
    b.save_task({**b.load_task("1"), "status": "pending_execution"})
    full = claim(1, full=True)
    assert "raw_items" in full[0]
    slim_len = len(json.dumps(got[1])); full_len = len(json.dumps(b.load_task("2")))
    print(f"      claim payload for a video task: full {full_len} B -> slim {slim_len} B")
    ok("claim: slim executor view (no raw_items/context_token/keys), --full keeps old output, stale reclaim works")


# ---------------------------------------------------------------- 6
def test_send_paths():
    b = load(HERE / "bridge.py", "b_send")
    b.atomic_write_json(b.CREDS_FILE, {"bot_token": "t"})
    sent = []
    def fake_send(creds, to, text, ctx, client_id=None):
        sent.append(text); return {"ret": 0, "_client_id": "c", "message_id": "m"}
    b.send_text = fake_send
    mk_task(b, 1, status="waiting_send", result_text="a" * 9000)     # 3 chunks at the 4000 limit
    mk_task(b, 2, status="sent")
    mk_task(b, 3, status="sending", send={"started_at": int(time.time() * 1000) - 300_000})
    mk_task(b, 4, status="send_failed", result_text="retry me",
            send={"attempts": 1, "next_retry_at": 0})
    mk_task(b, 5, status="send_failed", result_text="no",
            send={"attempts": 1, "next_retry_at": 0, "no_auto_retry": True})
    idx = b.scan_task_index()
    b.recover_stale_sending(idx)
    res = b.flush_outbox(idx)
    ids = sorted(r["task_id"] for r in res)
    assert ids == ["1", "4"], ids
    assert len(sent) == 3 + 1
    assert b.load_task("1")["status"] == "sent" and b.load_task("4")["status"] == "sent"
    assert b.load_task("3")["status"] == "send_uncertain"
    assert b.load_task("5")["status"] == "send_failed" and b.load_task("2")["status"] == "sent"
    # second pass: only the bridge's own notice about the interrupted task 3
    # (v4: the user is told directly instead of waiting for a hook/LLM run)
    second = b.flush_outbox()
    assert [r["task_id"][:7] for r in second] == ["notice-"], second
    assert b.load_task("3")["send"].get("notified")
    assert b.flush_outbox() == []
    ok("send paths: chunking, retry gating, no_auto_retry, stale-sending -> send_uncertain, idempotent flush")


if __name__ == "__main__":
    test_heartbeat_threads()
    test_index_semantics()
    if BENCH:
        test_index_bench()
    test_ingest_async_media()
    test_recover_downloading()
    test_cursor_after_ingest()
    test_claim_view()
    test_send_paths()
    print(f"\n{len(passed)} test groups passed")
