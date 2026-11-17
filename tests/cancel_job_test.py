#!/usr/bin/env python3
"""End-to-end self-test for job cancellation.

Topology mirrors docker-compose (sites on :8201/:8202, api on :8200). It
verifies the cancellation contract end to end:
  1. a running job can be cancelled; queued/unsent requests, redirects and
     retries are never sent, while in-flight responses cannot rewrite the
     cancelled conclusion;
  2. repeat cancellation is idempotent and returns the same terminal state;
  3. finished jobs (success/failed) cannot be cancelled and keep downloading;
  4. progress already collected is retained, but a cancelled job cannot
     download its corpus;
  5. cancellation during a Retry-After cooldown wakes immediately and sends
     nothing;
  6. after an API restart the cancelled job is NOT resumed, while another
     unfinished job still runs under the original quotas/cooldowns;
  7. cancelling job X never affects an independent job Y.
"""
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
API = "http://127.0.0.1:8200"
A = "http://127.0.0.1:8201"
B = "http://127.0.0.1:8202"

PY = sys.executable
FAILURES = []
PROCS = []
LOGDIR = None
DB_DIR = None


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f"  ({detail})" if detail else ""), flush=True)
    if not cond:
        FAILURES.append(f"{name} {detail}")


def start_proc(name, args, env, port):
    logfile = open(LOGDIR / f"{name}.log", "w")
    p = subprocess.Popen(args, env=env, stdout=logfile, stderr=subprocess.STDOUT)
    PROCS.append((name, p))
    deadline = time.time() + 30
    while time.time() < deadline:
        if p.poll() is not None:
            raise RuntimeError(f"{name} exited early, see {LOGDIR}/{name}.log")
        try:
            httpx.get(f"http://127.0.0.1:{port}/", timeout=1.0)
            return p
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"{name} did not start on port {port}")


def start_api():
    env = dict(os.environ, DB_PATH=str(DB_DIR / "collector.db"))
    return start_proc("api", [PY, "-m", "uvicorn", "app.main:app", "--app-dir", str(ROOT / "api"),
                              "--host", "127.0.0.1", "--port", "8200"], env, 8200)


def start_site(name, port, site_name, peer_base):
    env = dict(os.environ, SITE_NAME=site_name, PEER_BASE=peer_base)
    return start_proc(name, [PY, "-m", "uvicorn", "app:app", "--app-dir", str(ROOT / "sites"),
                             "--host", "127.0.0.1", "--port", str(port)], env, port)


def stop_api():
    for i, (name, p) in enumerate(PROCS):
        if name == "api" and p.poll() is None:
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
            PROCS.pop(i)
            return


def submit(client, urls):
    r = client.post(f"{API}/jobs", json={"urls": urls})
    assert r.status_code == 202, r.text
    return r.json()["job_id"]


def wait_for(client, job_id, status, timeout=30.0):
    t0 = time.time()
    body = {}
    while time.time() - t0 < timeout:
        body = client.get(f"{API}/jobs/{job_id}").json()
        if body["status"] == status:
            return body
        time.sleep(0.1)
    raise TimeoutError(f"job {job_id} did not reach {status}: {body.get('status')}")


def stats(base):
    return httpx.get(f"{base}/stats", timeout=5).json()["hits"]


def reset_sites():
    for base in (A, B):
        httpx.post(f"{base}/reset", timeout=5)


def main():
    global LOGDIR, DB_DIR
    work = Path(tempfile.mkdtemp(prefix="collector-cancel-"))
    LOGDIR = work / "logs"
    LOGDIR.mkdir(parents=True)
    DB_DIR = work / "db"
    DB_DIR.mkdir()
    log(f"workdir: {work}")

    start_site("site_a", 8201, "site_a", B)
    start_site("site_b", 8202, "site_b", A)
    start_api()
    client = httpx.Client(timeout=30.0)

    try:
        # ---------- phase 1: cancel while requests are queued / in flight
        log("phase 1: cancel a running job; unsent requests never leave")
        reset_sites()
        # Two long requests saturate site_a's serial pump; the remaining
        # site_a urls stay queued (unsent) at cancel time. site_b's article is
        # independent and is expected to complete -- it validates isolation.
        urls = [
            f"{A}/slow/1?delay=3",
            f"{A}/slow/2?delay=3",
            f"{A}/article/3",
            f"{A}/article/4",
            f"{B}/article/5",
        ]
        job = submit(client, urls)
        # wait until the first request is actually in flight
        deadline = time.time() + 10
        while time.time() < deadline:
            if stats(A).get("/slow/1"):
                break
            time.sleep(0.1)
        else:
            raise TimeoutError("first request never went in flight")
        t_cancel = time.time()
        r = client.post(f"{API}/jobs/{job}/cancel")
        check("p1 cancel returns 200", r.status_code == 200, str(r.status_code))
        body = wait_for(client, job, "cancelled")
        check("p1 job status cancelled", body["status"] == "cancelled", body["status"])
        check("p1 no pending/fetching left",
              body["pending"] == 0 and body["fetching"] == 0,
              f"pending={body['pending']} fetching={body['fetching']}")
        # The unsent queued requests must never hit site_a (serial pump: only
        # the first slow request can have been dispatched before cancellation).
        time.sleep(4.0)  # longer than both slow responses, so any late request would show
        hits_a, hits_b = stats(A), stats(B)
        check("p1 queued site_a articles never requested",
              "/article/3" not in hits_a and "/article/4" not in hits_a
              and "/slow/2" not in hits_a, str(hits_a))
        check("p1 site_a sent at most the one in-flight request",
              sum(len(v) for v in hits_a.values()) <= 1, str(hits_a))
        # The independent site_b request was already in flight/finished and is unaffected.
        check("p1 independent site_b request allowed to complete",
              len(hits_b.get("/article/5", [])) == 1, str(hits_b))
        b5 = [a for a in body["announcements"] if a["url"].endswith("/article/5")][0]
        check("p1 independent site_b announcement kept its success",
              b5["status"] == "success", b5["status"])
        # In-flight response must not resurrect the announcement into success.
        a1 = [a for a in body["announcements"] if a["url"].endswith("/slow/1?delay=3")][0]
        check("p1 in-flight response did not rewrite cancellation",
              a1["status"] == "cancelled", a1["status"])
        # Progress that had already completed is retained.
        check("p1 collected progress retained", body["success"] >= 1, f"success={body['success']}")
        r = client.get(f"{API}/jobs/{job}/download")
        check("p1 cancelled job cannot download (409)", r.status_code == 409, str(r.status_code))
        # idempotent repeat
        r2 = client.post(f"{API}/jobs/{job}/cancel")
        check("p1 repeat cancel 200 + same terminal state",
              r2.status_code == 200 and r2.json()["status"] == "cancelled"
              and r2.json()["message"] == "job was already cancelled", str(r2.status_code))
        body2 = client.get(f"{API}/jobs/{job}").json()
        check("p1 GET still cancelled, counts unchanged",
              body2["cancelled_announcements"] == body["cancelled_announcements"])
        check("p1 listed as cancelled",
              any(j["job_id"] == job and j["status"] == "cancelled"
                  for j in client.get(f"{API}/jobs").json()["jobs"]))

        # ---------- phase 1b: a redirect produced after cancel is never followed
        log("phase 1b: redirect/redirect-target requests after cancel never leave")
        reset_sites()
        job = submit(client, [f"{A}/to-peer/2", f"{A}/chain/3", f"{A}/article/9"])
        # cancel before the pump even dispatches the first request
        r = client.post(f"{API}/jobs/{job}/cancel")
        check("p1b immediate cancel 200", r.status_code == 200)
        wait_for(client, job, "cancelled")
        time.sleep(2.0)
        hits_a, hits_b = stats(A), stats(B)
        total_a = sum(len(v) for v in hits_a.values())
        total_b = sum(len(v) for v in hits_b.values())
        # At most the very first request may already have been in flight; the
        # two remaining URLs must never have been dispatched.
        check("p1b at most one (in-flight) request reached site_a",
              total_a <= 1 and "/chain/3" not in hits_a and "/article/9" not in hits_a,
              str(hits_a))
        check("p1b no redirect followed to site_b (302 target never requested)",
              total_b == 0, str(hits_b))
        check("p1b every announcement cancelled",
              all(a["status"] == "cancelled"
                  for a in client.get(f"{API}/jobs/{job}").json()["announcements"]))

        # ---------- phase 2: finished jobs cannot be cancelled
        log("phase 2: terminal success/failed jobs reject cancellation")
        good = submit(client, [f"{A}/article/10"])
        gbody = wait_for(client, good, "success")
        check("p2 normal job success", gbody["status"] == "success")
        r = client.post(f"{API}/jobs/{good}/cancel")
        check("p2 cancel success job -> 409", r.status_code == 409, str(r.status_code))
        gbody2 = client.get(f"{API}/jobs/{good}").json()
        check("p2 success job untouched", gbody2["status"] == "success" and gbody2["downloadable"])
        r = client.get(f"{API}/jobs/{good}/download")
        check("p2 normal job still downloads 200", r.status_code == 200, str(r.status_code))

        bad = submit(client, [f"{B}/busy"])
        bbody = wait_for(client, bad, "failed", timeout=30)
        check("p2 failing job reaches failed", bbody["status"] == "failed")
        r = client.post(f"{API}/jobs/{bad}/cancel")
        check("p2 cancel failed job -> 409", r.status_code == 409, str(r.status_code))
        check("p2 failed job still failed",
              client.get(f"{API}/jobs/{bad}").json()["status"] == "failed")
        r = client.post(f"{API}/jobs/doesnotexist/cancel")
        check("p2 cancel unknown job -> 404", r.status_code == 404, str(r.status_code))

        # ---------- phase 3: cancel during cooldown; no request after cancel
        log("phase 3: cancellation cuts a Retry-After wait short and sends nothing")
        reset_sites()
        job = submit(client, [f"{A}/flaky/7?fails=1&ra=20", f"{A}/article/8"])
        # wait for the first 429 (cooldown armed)
        deadline = time.time() + 10
        while time.time() < deadline:
            if stats(A).get("/flaky/7"):
                break
            time.sleep(0.1)
        else:
            raise TimeoutError("flaky endpoint never hit")
        t0 = time.time()
        r = client.post(f"{API}/jobs/{job}/cancel")
        check("p3 cancel during cooldown 200", r.status_code == 200)
        body = wait_for(client, job, "cancelled")
        elapsed = time.time() - t0
        check("p3 cancel settled promptly (did not wait 20s cooldown)", elapsed < 5.0, f"{elapsed:.2f}s")
        time.sleep(2.0)
        hits_a = stats(A)
        check("p3 flaky hit exactly once (no retry after cancel)",
              len(hits_a.get("/flaky/7", [])) == 1, str(hits_a.get("/flaky/7")))
        check("p3 queued article never requested",
              "/article/8" not in hits_a, str(hits_a))

        # ---------- phase 4: restart persistence + sibling job unaffected
        log("phase 4: restart does not resume cancelled job; other job keeps collecting")
        reset_sites()
        jc = submit(client, [f"{A}/article/20", f"{A}/slow/9?delay=3", f"{B}/article/21"])
        # let the first announcement finish so we can check progress retention
        deadline = time.time() + 10
        while time.time() < deadline:
            body = client.get(f"{API}/jobs/{jc}").json()
            if any(a["status"] == "success" for a in body["announcements"]):
                break
            time.sleep(0.1)
        client.post(f"{API}/jobs/{jc}/cancel")
        wait_for(client, jc, "cancelled")
        kept = client.get(f"{API}/jobs/{jc}").json()
        kept_success = kept["success"]
        check("p4 cancelled job retained some progress", kept_success >= 1, f"success={kept_success}")
        # sibling job submitted to the same origins must keep running untouched
        js = submit(client, [f"{B}/article/30", f"{A}/article/31"])
        stop_api()
        log("  api stopped, restarting...")
        time.sleep(1.0)
        start_api()
        # cancelled job stays cancelled and produces no new requests after restart
        body = wait_for(client, jc, "cancelled")
        check("p4 cancelled job still cancelled after restart", body["status"] == "cancelled")
        check("p4 retained progress survived restart", body["success"] == kept_success)
        # sibling job completes normally (its requests resume under the same quotas)
        sbody = wait_for(client, js, "success", timeout=30)
        check("p4 sibling job completes after restart", sbody["status"] == "success")
        r = client.get(f"{API}/jobs/{js}/download")
        check("p4 sibling job downloads fully", r.status_code == 200 and len(r.json()["documents"]) == 2)
        r = client.get(f"{API}/jobs/{jc}/download")
        check("p4 cancelled job still cannot download", r.status_code == 409)

        # ---------- phase 5: cancelling one job does not disturb another live job
        log("phase 5: independent jobs are isolated")
        reset_sites()
        jx = submit(client, [f"{A}/slow/5?delay=3", f"{A}/article/6", f"{A}/article/7"])
        jy = submit(client, [f"{B}/article/40", f"{B}/article/41"])
        time.sleep(0.5)
        client.post(f"{API}/jobs/{jx}/cancel")
        wait_for(client, jx, "cancelled")
        ybody = wait_for(client, jy, "success", timeout=20)
        check("p5 other job succeeds despite cancellation", ybody["status"] == "success")
        check("p5 other job downloadable",
              client.get(f"{API}/jobs/{jy}/download").status_code == 200)

    finally:
        for name, p in PROCS:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        time.sleep(0.5)
        for name, p in PROCS:
            if p.poll() is None:
                p.kill()

    print()
    if FAILURES:
        print(f"RESULT: {len(FAILURES)} FAILURE(S)")
        for f in FAILURES:
            print(f"  - {f}")
        print(f"logs kept at {LOGDIR}")
        return 1
    print("RESULT: ALL CHECKS PASSED")
    shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
