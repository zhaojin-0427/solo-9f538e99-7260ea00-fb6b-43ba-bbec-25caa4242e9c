#!/usr/bin/env python3
"""Unit-level test: when cancellation *registration* fails, the job must keep
collecting untouched.

A real DB error cannot be triggered over the HTTP boundary, so this drives the
FastAPI app in-process via httpx's ASGITransport and patches DB.cancel_job to
raise sqlite3.Error (e.g. a disk/locking failure at commit time). The API must
answer 500, leave every announcement running, and the job must still finish
successfully and remain downloadable. It also pins the DB-layer atomicity
guarantees (finished rows preserved, cancelled rows terminal, rollback).
"""
import asyncio
import sqlite3
import sys
import tempfile
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "api"))

from app.db import DB  # noqa: E402
from app.main import app  # noqa: E402


FAILURES = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f"  ({detail})" if detail else ""), flush=True)
    if not cond:
        FAILURES.append(f"{name} {detail}")


async def http_case():
    tmp = Path(tempfile.mkdtemp(prefix="collector-cancel-fail-"))
    # Point the app at a temp DB before lifespan starts.
    import app.main as main_mod
    main_mod.DB_PATH = str(tmp / "app.db")

    # httpx's ASGITransport does not run lifespan itself; drive it explicitly.
    lifespan = app.router.lifespan_context(app)
    await lifespan.__aenter__()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://test") as client:
            # create two announcements
            r = await client.post("/jobs", json={"urls": ["http://x/1", "http://x/2"]})
            assert r.status_code == 202, r.text
            job_id = r.json()["job_id"]

            original = DB.cancel_job

            def boom(self, jid):
                raise sqlite3.OperationalError("disk I/O error (simulated)")

            DB.cancel_job = boom
            try:
                r = await client.post(f"/jobs/{job_id}/cancel")
            finally:
                DB.cancel_job = original
            check("registration failure -> HTTP 500", r.status_code == 500, str(r.status_code))
            check("500 explains job continues", "continues" in r.json()["detail"],
                  r.json().get("detail"))

            # Job flag and announcement states must be untouched: not cancelled.
            db: DB = app.state.db
            job = db.get_job(job_id)
            check("job flag not set on failure", job["cancelled"] == 0)
            anns = db.list_announcements(job_id)
            check("announcements not marked cancelled",
                  all(a["status"] in ("pending", "fetching") for a in anns),
                  str([a["status"] for a in anns]))
            r = await client.get(f"/jobs/{job_id}")
            check("job still queryable and not cancelled",
                  r.json()["status"] == "running" and r.json()["cancelled"] is False,
                  r.json()["status"])
            # Repeat attempt now succeeds (registration recovered): normal cancel path.
            r = await client.post(f"/jobs/{job_id}/cancel")
            check("retry after failure cancels normally", r.status_code == 200, str(r.status_code))
            check("job now cancelled", r.json()["status"] == "cancelled")
            # And a third call is the idempotent repeat.
            r = await client.post(f"/jobs/{job_id}/cancel")
            check("repeat stays idempotent 200",
                  r.status_code == 200 and r.json()["message"] == "job was already cancelled")
    finally:
        await lifespan.__aexit__(None, None, None)


def db_layer_case():
    tmp = Path(tempfile.mkdtemp(prefix="collector-cancel-db-"))
    db = DB(str(tmp / "db.sqlite"))
    db.create_job("j1", total=3)
    ids = [db.add_announcement("j1", f"http://x/{i}") for i in range(3)]
    # one success, one failed, one still pending
    db.update_ann(ids[0], status="success", title="t", content="c")
    db.update_ann(ids[1], status="failed", error="boom")

    state, ann_ids = db.cancel_job("j1")
    check("db: running job cancels", state == "cancelled_now")
    check("db: only unfinished announcement retired",
          sorted(ann_ids) == [ids[2]], str(ann_ids))
    rows = {a["id"]: a for a in db.list_announcements("j1")}
    check("db: success row preserved", rows[ids[0]]["status"] == "success")
    check("db: failed row preserved", rows[ids[1]]["status"] == "failed")
    check("db: pending row cancelled", rows[ids[2]]["status"] == "cancelled")

    # idempotency
    state2, ids2 = db.cancel_job("j1")
    check("db: repeat cancel idempotent", state2 == "already_cancelled" and ids2 == [])

    # cancelled rows are terminal: update_ann cannot resurrect them
    db.update_ann(ids[2], status="success", title="late", content="late")
    check("db: late write cannot resurrect cancelled row",
          db.list_announcements("j1")[2]["status"] == "cancelled")

    # naturally finished job cannot be cancelled
    db.create_job("j2", total=1)
    j2id = db.add_announcement("j2", "http://y/1")
    db.update_ann(j2id, status="success", title="t", content="c")
    state3, _ = db.cancel_job("j2")
    check("db: finished job reports not_running", state3 == "not_running")
    check("db: finished job flag stays 0", db.get_job("j2")["cancelled"] == 0)
    check("db: unknown job -> None", db.cancel_job("nope") is None)

    # migration: a legacy DB without the cancelled column gains it
    legacy = Path(tempfile.mkdtemp(prefix="collector-legacy-")) / "legacy.db"
    con = sqlite3.connect(legacy)
    con.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY, created_at REAL NOT NULL, total INTEGER NOT NULL)")
    con.execute("CREATE TABLE announcements (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,"
                " url TEXT NOT NULL, current_url TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',"
                " hops INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,"
                " title TEXT, content TEXT, error TEXT, updated_at REAL NOT NULL DEFAULT 0)")
    con.execute("CREATE TABLE cooldowns (origin TEXT PRIMARY KEY, until_ts REAL NOT NULL)")
    con.execute("INSERT INTO jobs (id, created_at, total) VALUES ('old', 1.0, 1)")
    con.execute("INSERT INTO announcements (job_id, url, current_url) VALUES ('old', 'http://z/1', 'http://z/1')")
    con.commit()
    con.close()
    db2 = DB(str(legacy))
    check("db: legacy schema migrated with cancelled column",
          db2.get_job("old")["cancelled"] == 0)
    state4, ann_ids4 = db2.cancel_job("old")
    check("db: migrated legacy job cancellable",
          state4 == "cancelled_now" and len(ann_ids4) == 1)
    db.close()
    db2.close()


def main():
    db_layer_case()
    asyncio.run(http_case())
    print()
    if FAILURES:
        print(f"RESULT: {len(FAILURES)} FAILURE(S)")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("RESULT: ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
