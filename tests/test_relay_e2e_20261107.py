#!/usr/bin/env python3
"""Regression of 2026-11-06 21:13 (「把最新的微信桥去敏开源吧」 died 3 times,
redoing the 24-file comparison and scan each run), in the sandbox: one user
message, then simulated executors that follow the relay protocol exactly
(claim -> plan -> one step at a time -> complete --continue), real work on a
fixture (a "live" bridge and the v5.0.5 open-source tree: 24 changed files with
planted secrets, plus the never-public 5.0.5->20261105 patch), and leg 3
killed by the platform in the middle of a scan step. Expected: plan on disk,
every step <= 7 min, automatic hand-offs, no human message, each file scanned
once, the 24-file list built once and reused, a desensitized package that
verifies clean, exactly one final reply, and every leg visible in stats.
Run: python3 tests/test_relay_e2e_20261107.py"""
import importlib.util
import json
import re
import sys
import zipfile

from relay_fixtures import (HERE, boot, say, flush, claim, progress, complete, stats,
                            check, finish)

SECRET = re.compile(r'bot_token\s*=\s*"[A-Za-z0-9_]{8,}"|/Users/exampleuser\S*|\b1[3-9]\d{9}\b')
PER_FILE = 0.75                                     # simulated minutes per scanned file
COST = {"diff": 4.0, "pack": 4.0, "verify": 5.0}
PLAN = {"goal": "最新微信桥去敏开源", "steps": [
    {"step_id": "diff", "do": "比对线上桥与 v5.0.5 开源包，列出改动文件", "outputs": ["changed.txt"],
     "est_min": 4, "done_when": "changed.txt 写好"},
    *[{"step_id": f"scan{k}", "do": f"逐项扫敏感内容：改动文件 {8 * k - 7}–{8 * k}",
       "inputs": ["changed.txt"], "outputs": [f"scan{k}.jsonl"], "est_min": 6,
       "done_when": "这 8 个文件都有结果"} for k in (1, 2, 3)],
    {"step_id": "pack", "do": "按扫描结果去敏并打包（5.0.5→20261105 的 patch 不进包）",
     "inputs": ["scan1.jsonl", "scan2.jsonl", "scan3.jsonl"], "outputs": ["pkg.zip"], "est_min": 4},
    {"step_id": "verify", "do": "验包：解包重扫、核对文件清单", "inputs": ["pkg.zip"],
     "outputs": ["verify.txt"], "est_min": 5}]}
OUTS = {s["step_id"]: s["outputs"] for s in PLAN["steps"]}


def build_fixture(root):
    live, oss = root / "live", root / "oss-5.0.5"
    names = [f"mod{i:02d}.py" for i in range(1, 25)] + ["README.md", "EXECUTOR.md", "ops/doctor.py",
                                                        "ops/backup.sh", "tests/run_all.py", "wb"]
    for i, n in enumerate(names):
        base = f"# {n}\nVALUE = {i}\n"
        txt = base
        if i < 24:                                  # the 24 changed files
            txt += f"CHANGED = {i}\n"
            if i % 3 == 0:
                txt += 'bot_token = "tok_live_%08d"\n' % i
            if i % 5 == 0:
                txt += "# 来自 /Users/exampleuser/private/notes.txt\n"
            if i % 7 == 0:
                txt += "# 联系 13800000000\n"
        for d, body in ((live, txt), (oss, base)):
            (d / n).parent.mkdir(parents=True, exist_ok=True)
            (d / n).write_text(body, encoding="utf-8")
    p = live / "patches" / "5.0.5-to-20261105.patch"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("--- DECISIONS.md\n-私有决定（旧 DECISIONS.md 删除行）\n", encoding="utf-8")
    return live, oss


class Sim:
    """One simulated executor per leg, doing the real work on the fixture."""
    def __init__(self, b, clk, tid, live, oss):
        self.b, self.clk, self.tid, self.live, self.oss = b, clk, tid, live, oss
        self.wd = b._plan_workdir(tid)
        self.scans, self.diff_runs = {}, 0
        self.leg_min, self.step_min, self.views = [], [], []

    def work(self, sid, die_after=None):
        b, wd = self.b, self.wd
        wd.mkdir(parents=True, exist_ok=True)
        if sid == "diff":
            self.diff_runs += 1
            ch = sorted(str(p.relative_to(self.live)) for p in self.live.rglob("*") if p.is_file()
                        and "patches" not in p.parts
                        and (self.oss / p.relative_to(self.live)).read_bytes() != p.read_bytes())
            (wd / "changed.txt").write_text("\n".join(ch) + "\n", encoding="utf-8")
            return COST["diff"], False
        if sid.startswith("scan"):
            k = int(sid[4:])
            ch = (wd / "changed.txt").read_text(encoding="utf-8").split()
            out = wd / f"{sid}.jsonl"
            have = ({json.loads(x)["file"] for x in out.read_text(encoding="utf-8").splitlines()}
                    if out.exists() else set())
            used, n = 0.0, 0
            for rel in ch[(k - 1) * 8:k * 8]:
                if rel in have:
                    continue                    # scanned by an earlier leg: not again
                if die_after is not None and n >= die_after:
                    return used, True           # the platform cuts the turn here
                hits = SECRET.findall((self.live / rel).read_text(encoding="utf-8"))
                self.scans[rel] = self.scans.get(rel, 0) + 1
                with out.open("a", encoding="utf-8") as f:
                    f.write(json.dumps({"file": rel, "hits": len(hits)}) + "\n")
                n += 1
                used += PER_FILE
                if n == 1:                      # partial result on record (+ heartbeat)
                    progress(b, self.tid, claim_id=self.cid, artifact=[str(out)])
            return used, False
        if sid == "pack":
            ch = set((wd / "changed.txt").read_text(encoding="utf-8").split())
            with zipfile.ZipFile(wd / "pkg.zip", "w") as z:
                for p in sorted(self.oss.rglob("*")):
                    if p.is_file():
                        rel = str(p.relative_to(self.oss))
                        src = self.live / rel if rel in ch else p
                        z.writestr(rel, SECRET.sub("<redacted>", src.read_text(encoding="utf-8")))
            return COST["pack"], False
        with zipfile.ZipFile(wd / "pkg.zip") as z:           # verify
            names = z.namelist()
            hits = sum(len(SECRET.findall(z.read(n).decode("utf-8"))) for n in names)
        (wd / "verify.txt").write_text(
            f"files={len(names)} hits={hits} patch_in_pkg={any('patches/' in n for n in names)}\n",
            encoding="utf-8")
        return COST["verify"], False

    def leg(self, die_in=None, die_after=None):
        rc, out = claim(self.b)
        tasks = out.get("tasks") or []
        if len(tasks) != 1 or tasks[0]["message_id"] != self.tid:
            return "no-task"
        v, self.cid = tasks[0], out.get("claim_id")
        self.views.append(v)
        t0 = self.clk()
        if "plan" not in v:
            rc, po = progress(self.b, self.tid, claim_id=self.cid,
                              plan=json.dumps(PLAN, ensure_ascii=False), notify=True)
            self.clk.adv(0.5)
            nxt = po["plan"].get("next")
        else:
            nxt = v["plan"]["next"]
            progress(self.b, self.tid, claim_id=self.cid, stage=f"第 {v['plan']['leg']} 棒：从 {nxt['step_id']} 接着做")
        while nxt:
            if nxt["est_min"] > 7 - (self.clk() - t0) / 60000 + 1e-9:
                break
            used, died = self.work(nxt["step_id"], die_after if nxt["step_id"] == die_in else None)
            self.clk.adv(used)
            if died:
                return "died"
            self.step_min.append((nxt["step_id"], used))
            rc, po = progress(self.b, self.tid, claim_id=self.cid, step_done=nxt["step_id"],
                              artifact=[str(self.wd / o) for o in OUTS[nxt["step_id"]]])
            nxt = po["plan"].get("next")
            if nxt and not po["plan"].get("next_fits"):
                break
        self.leg_min.append(round((self.clk() - t0) / 60000, 2))
        if not nxt:
            rc, co = complete(self.b, self.tid, claim_id=self.cid, result=(
                "去敏开源包已打好：pkg.zip（30 个文件；24 个改动文件逐项扫过并去敏，5.0.5→20261105 "
                "的 patch 未放入）。验包：" + (self.wd / "verify.txt").read_text(encoding="utf-8").strip()))
            return "done" if rc == 0 else f"complete rc={rc}"
        rc, co = complete(self.b, self.tid, claim_id=self.cid, cont=True)
        self.clk.adv(0.2)                       # hook poll + cold start of the next executor
        return "continued" if co.get("continued") else f"stopped: {co}"


def main():
    b, clk = boot("rl_e2e")
    b.batching_on(True)                 # production path: serve batches every message
    live, oss = build_fixture(b.BASE_DIR / "sim")
    tid = say(b, "把最新的微信桥去敏开源吧")
    flush(b)
    sim = Sim(b, clk, tid, live, oss)
    results = []
    for legno in range(1, 13):
        r = sim.leg(die_in="scan2", die_after=3) if legno == 3 else sim.leg()
        results.append(r)
        if r == "died":
            clk.adv(11)                         # silent; the bridge notices after 10 min
        elif r != "continued":
            break
    check(results == ["continued", "continued", "died", "continued", "continued", "continued", "done"],
          "7 legs, no human message: hand-off x5, one leg killed mid-step and resumed, done", results)
    check("long_hint" in sim.views[0] and "去敏" in sim.views[0]["long_hint"],
          "leg 1: the claim flagged the task as long by rule", sim.views[0])
    v4 = sim.views[3]
    check("中途断了" in v4.get("resume_from", "") and v4["plan"]["next"]["step_id"] == "scan2"
          and any(p.endswith("scan2.jsonl") for p in v4["plan"]["next"].get("partial") or []),
          "leg 4 resumes scan2 from the plan with its partial results", v4)
    check(sim.diff_runs == 1 and len((sim.wd / "changed.txt").read_text().split()) == 24,
          "the 24-file list was built once and reused by every later leg", sim.diff_runs)
    check(len(sim.scans) == 24 and set(sim.scans.values()) == {1},
          "each of the 24 files scanned exactly once (no rescan after the crash)", sim.scans)
    with zipfile.ZipFile(sim.wd / "pkg.zip") as z:
        names = z.namelist()
        leak = sum(len(SECRET.findall(z.read(n).decode())) for n in names)
    check(len(names) == 30 and leak == 0 and not any("patches" in n for n in names)
          and "hits=0" in (sim.wd / "verify.txt").read_text(),
          "desensitized package: 30 files, 0 secrets, the private patch not in it, verified", names)
    check(all(s["est_min"] <= 7 for s in b.plan_load(tid)["steps"])
          and all(m <= 7 for _s, m in sim.step_min) and all(m <= 7 for m in sim.leg_min),
          f"every step and every leg <= 7 min (legs: {sim.leg_min})", sim.step_min)
    final = [s for s in b.SENT if "去敏开源包已打好" in s]
    check(len(final) == 1 and b.load_task(tid)["status"] == "sent" and b.plan_load(tid)["state"] == "done",
          "exactly one final reply; task sent; plan done", b.SENT)
    check("分 6 步" in b.SENT[0] and "预计" in b.SENT[0] and len(b.SENT) <= 6,
          f"first message is the plan outline; {len(b.SENT)} messages in all (cap ~10)", b.SENT)
    users = [x for x in b.scan_task_index() if x.get("kind") not in ("notice", "command")]
    check(len(users) == 1, "no user message after the first one (no 「继续」, no resend)", users)
    rc, st = stats(b, [tid])
    rel = st["tasks"][0].get("relay") or {}
    legs = rel.get("legs") or []
    check([lg["how"] for lg in legs] == ["continue", "continue", "reclaimed", "continue", "continue",
                                       "continue", "complete"]
          and legs[2]["steps_done"] == [] and legs[3]["steps_done"] == ["scan2"]
          and all(lg["start"] and lg["end"] and lg["ms"] is not None and "checkpoint" in lg for lg in legs)
          and rel["elapsed_ms"] <= 60 * 60_000,
          f"stats --task-id: every leg with start/end/ms/steps/checkpoint, "
          f"{round(rel.get('elapsed_ms', 0) / 60000, 1)} min in all", legs)
    rc, st2 = stats(b, recent=3)
    check(any(r.get("task_id") == tid and r.get("relay") for r in st2.get("tasks") or []),
          "stats --recent shows the relay legs too")
    (b.BASE_DIR / "ops").mkdir(exist_ok=True)
    (b.BASE_DIR / "ops" / "doctor.py").write_text((HERE / "ops" / "doctor.py").read_text(encoding="utf-8"))
    spec = importlib.util.spec_from_file_location("doc_e2e", b.BASE_DIR / "ops" / "doctor.py")
    doc = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(doc)
    doc.check_relay()
    r1 = [r for r in doc.results if r["check"] == "relay"][-1]
    b._plan_path(tid).chmod(0o644)
    doc.check_relay()
    r2 = [r for r in doc.results if r["check"] == "relay"][-1]
    check(r1["status"] == "PASS" and r2["status"] == "WARN" and "0600" in r2["detail"],
          "doctor relay check: PASS when private, WARN on a readable plan", (r1, r2))


main()
sys.exit(finish("RELAY E2E"))
