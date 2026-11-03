#!/usr/bin/env python3
"""2026-11-02 optimisation patch tests (offline): wake split (pending_at),
stage progress + honest auto-progress wording, settings commands, topic
summaries + history --search, restart attribution, scene keyframes.
Run: python3 tests/test_opt_20261102.py"""
import io, json, os, shutil, subprocess, sys, tempfile, time
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_v3 as h  # noqa: E402  fixtures only

NEW = HERE / "bridge.py"
fails = []


def check(cond, msg, detail=""):
    print(("PASS  " if cond else "FAIL  ") + msg + (f"\n      {detail}" if detail else ""))
    if not cond:
        fails.append(msg)


def now_ms():
    return int(time.time() * 1000)


def cli(b, fn, **kw):
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = fn(SimpleNamespace(**kw))
    return rc, json.loads(buf.getvalue())


b = h.load(NEW, "opt20261102")
h.set_creds(b, token="<redacted>")

# --- settings commands ------------------------------------------------------
check(b.settings_get() == {"media_wait": True, "progress_notes": True,
                           "reply_style": "normal"}, "settings default to current behaviour")
r = b.parse_settings_command("设置 媒体不等")
check("已设置" in r and "原来" in r and b.settings_get()["media_wait"] is False,
      "设置 媒体不等 changes + confirms old->new", r)
r = b.parse_settings_command("设置 媒体不等")
check("已经是这个设置了" in r, "repeating a setting is a no-op confirmation", r)
before = b.settings_get()
r = b.parse_settings_command("设置 不存在的东西")
check("没这个设置项" in r and b.settings_get() == before,
      "unknown setting changes nothing", r)
r = b.parse_settings_command("设置 进度关")
check(b.settings_get()["progress_notes"] is False, "设置 进度关 stored", r)
r = b.parse_settings_command("设置 恢复默认")
check(b.settings_get() == {"media_wait": True, "progress_notes": True,
                           "reply_style": "normal"}, "设置 恢复默认 resets all", r)
check(b.parse_settings_command("你好") is None, "ordinary text is not a setting")
t = b.say if hasattr(b, "say") else None
# ingest path: a settings message is answered by the bridge, no executor
b.ingest_message({"ilink_user_id": "owner"}, {
    "message_id": "set1", "from_user_id": "owner", "message_type": 1,
    "context_token": "CTX1",
    "item_list": [{"type": 1, "text_item": {"text": "设置 简略"}}]})
task = b.load_task("set1")
check(task["kind"] == "command" and "已设置" in (task["result_text"] or "")
      and b.settings_get()["reply_style"] == "brief",
      "ingested 设置 简略 answered by bridge", str(task.get("result_text")))
b.settings_set({"reply_style": "normal"})

# --- progress --stage -------------------------------------------------------
mk = h.mk_task(b, "prog1", "executing",
               execution={"claimed_at": now_ms() - 5000, "by": "muse"})
rc, out = cli(b, b.cmd_progress, task_id="prog1",
              note=None, stage="正在查 bridge.py 的进度逻辑", notify=False)
task = b.load_task("prog1")
check(rc == 0 and (task["execution"] or {}).get("stage") == "正在查 bridge.py 的进度逻辑",
      "progress --stage records the step", str(out))
line = b.progress_command("owner")
check("正在：" in line, "「进度」 shows the stage", line)

# --- auto-progress honesty --------------------------------------------------
mk = h.mk_task(b, "auto1", "executing",
               execution={"claimed_at": now_ms() - 500_000, "by": "muse"})
b.check_auto_progress(b.scan_task_index())
notices = [b.load_task(s_["message_id"]) for s_ in b.scan_task_index()
           if s_.get("kind") == "notice"]
texts = " ".join((t.get("result_text") or "") for t in notices)
check(any("还在处理" in (t.get("result_text") or "") for t in notices)
      and "马上发你" not in texts,
      "auto-progress says still running, never '马上发你'", texts[:200])

# --- wake split -------------------------------------------------------------
base = now_ms() - 10_000
for mid, pen_off, cl_off in (("lat1", 1000, 6000), ("lat2", 2000, 7000)):
    t = h.mk_task(b, mid, "sent", received_at=base)
    t["pending_at"] = base + pen_off
    t["execution"] = {"claimed_at": base + cl_off,
                      "finished_at": base + cl_off + 3000}
    t["send"] = {"at": base + cl_off + 3500, "parts": 1}
    b.save_task(t)
st = b.latency_stats(hours=72, limit=300)
check(st["wake_queue"]["p50"] is not None and st["wake_wait"]["p50"] is not None,
      "latency_stats splits wake into queue + wait", str(st))
check(abs(st["wake_queue"]["p50"] - 1500) <= 1000
      and abs(st["wake_wait"]["p50"] - 5000) <= 1000,
      "wake split values are sane (queue ~1.5s, wait ~5s)", str(st))
check("进队" in b.latency_summary() and "等接手" in b.latency_summary(),
      "latency summary shows the split")

# --- topic summaries + history --search -------------------------------------
b.history_append("user", "微信桥的视频关键帧怎么做", mid="u1")
b.history_append("bot", "按场景切关键帧", mid="b1")
b.history_append("user", "那就这么定", mid="u2")
out_text = b.newtopic_command("owner")
digests = b.topic_summaries_read()
check(len(digests) >= 1 and digests[-1]["turns"] >= 2,
      "新话题 files a digest of the ended topic", str(digests[-1:]))
ctx = b.conversation_context(now_ms())
check(ctx and ctx.get("past_topics"), "claim context carries past_topics",
      str((ctx or {}).get("past_topics")))
rc, out = cli(b, b.cmd_history, limit=30, hours=72, search="关键帧", json=True)
check(any("关键帧" in t.get("text", "") for t in out["turns"]),
      "history --search finds an old turn", str(out)[:200])

# --- restart attribution ----------------------------------------------------
legacy = b.serve_starts_read.__wrapped__ if hasattr(b.serve_starts_read, "__wrapped__") else None
b.atomic_write_json(b.SERVE_STARTS_FILE, [base, {"at": base + 1000,
                                                  "reason": "repair",
                                                  "gap_ms": 5000,
                                                  "prev_uptime_ms": 1000}])
starts = b.serve_starts_read()
check(starts[0]["reason"] == "legacy" and starts[1]["reason"] == "repair",
      "serve_starts_read normalises legacy timestamps", str(starts))
b.atomic_write_json(b.HEARTBEAT_FILE,
                    {"serve_started_at": base, "last_poll_ok_at": base + 500})
b.atomic_write_json(b.SERVE_RESTART_REASON_FILE,
                    {"reason": "repair", "at": now_ms()})
entry = b.serve_start_entry(now_ms())
check(entry["reason"] == "repair" and entry["gap_ms"] is not None,
      "serve_start_entry consumes a fresh reason file", str(entry))
entry2 = b.serve_start_entry(now_ms())
check(entry2["reason"] == "unknown",
      "reason file is consumed, not reused", str(entry2))

# --- scene keyframes --------------------------------------------------------
if shutil.which("ffmpeg"):
    tmp = Path(tempfile.mkdtemp(prefix="vframes-test-"))
    red = tmp / "red.mp4"
    blue = tmp / "blue.mp4"
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                    "-f", "lavfi", "-i", "color=c=red:s=320x240:d=1",
                    str(red)], check=True)
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                    "-f", "lavfi", "-i", "color=c=blue:s=320x240:d=1",
                    str(blue)], check=True)
    lst = tmp / "list.txt"
    lst.write_text(f"file '{red}'\nfile '{blue}'\n")
    vid = tmp / "cut.mp4"
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                    "-f", "concat", "-safe", "0", "-i", str(lst),
                    "-c", "libx264", "-pix_fmt", "yuv420p", str(vid)], check=True)
    rc, out = cli(b, b.cmd_video_frames, input=str(vid), out_dir=None,
                  max_frames=8, threshold=0.35)
    check(rc == 0 and len(out.get("frames", [])) >= 2,
          "video-frames finds the red/blue scene cut", str(out)[:300])
else:
    print("SKIP  video-frames (no ffmpeg)")

print(f"\n{len(fails)} failures")
sys.exit(1 if fails else 0)
