"""SQLite persistence layer.

Stores jobs, per-announcement fetch state and per-origin cooldowns so that a
restart can resume unfinished work without re-fetching successful documents
and without losing active cooldown deadlines.
"""
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id         TEXT PRIMARY KEY,
    created_at REAL NOT NULL,
    total      INTEGER NOT NULL,
    cancelled  INTEGER NOT NULL DEFAULT 0    -- 1 once the job has been cancelled (terminal)
);
CREATE TABLE IF NOT EXISTS announcements (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL REFERENCES jobs(id),
    url         TEXT NOT NULL,           -- originally submitted URL
    current_url TEXT NOT NULL,           -- URL currently being fetched (changes on redirect)
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending|fetching|success|failed|cancelled
    hops        INTEGER NOT NULL DEFAULT 0,       -- redirects followed so far
    attempts    INTEGER NOT NULL DEFAULT 0,       -- retries used on current_url
    title       TEXT,
    content     TEXT,
    error       TEXT,
    updated_at  REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ann_job ON announcements(job_id);
CREATE TABLE IF NOT EXISTS cooldowns (
    origin   TEXT PRIMARY KEY,
    until_ts REAL NOT NULL
);
"""

# Columns added after the initial release; applied lazily to pre-existing DBs.
_MIGRATIONS = (
    ("jobs", "cancelled", "ALTER TABLE jobs ADD COLUMN cancelled INTEGER NOT NULL DEFAULT 0"),
)

_UPDATABLE = {"current_url", "status", "hops", "attempts", "title", "content", "error"}


class DB:
    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()
        with self._lock, self._conn:
            self._conn.executescript(SCHEMA)
            self._migrate()

    def _migrate(self) -> None:
        for table, column, ddl in _MIGRATIONS:
            cols = {r["name"] for r in self._conn.execute(f"PRAGMA table_info({table})")}
            if column not in cols:
                self._conn.execute(ddl)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- jobs ---------------------------------------------------------
    def create_job(self, job_id: str, total: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO jobs (id, created_at, total) VALUES (?, ?, ?)",
                (job_id, time.time(), total),
            )

    def get_job(self, job_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()

    def list_jobs(self):
        with self._lock:
            return self._conn.execute(
                """
                SELECT j.id, j.created_at, j.total, j.cancelled,
                       COALESCE(SUM(a.status = 'success'), 0)  AS success,
                       COALESCE(SUM(a.status = 'failed'), 0)   AS failed,
                       COALESCE(SUM(a.status = 'cancelled'), 0) AS cancelled_anns,
                       COALESCE(SUM(a.status IN ('pending','fetching')), 0) AS unfinished
                FROM jobs j LEFT JOIN announcements a ON a.job_id = j.id
                GROUP BY j.id ORDER BY j.created_at DESC
                """
            ).fetchall()

    def cancel_job(self, job_id: str):
        """Atomically register a cancellation.

        Returns ``(status, ann_ids)`` with ``status`` one of:
          * ``"cancelled_now"``    - the job was running; every pending/fetching
            announcement has been marked 'cancelled' in the same transaction.
            Finished announcements (success/failed) keep their state, so the
            progress already collected is preserved.
          * ``"already_cancelled"``- the job was already cancelled (idempotent).
          * ``"not_running"``      - the job reached its natural terminal state
            (success/failed) and cannot be cancelled; nothing is changed.
        Returns ``None`` when the job does not exist.
        If anything raises, the whole transaction rolls back and the job keeps
        running exactly as before.
        """
        with self._lock, self._conn:
            job = self._conn.execute(
                "SELECT cancelled FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            if job is None:
                return None
            if job["cancelled"]:
                return ("already_cancelled", [])
            unfinished = self._conn.execute(
                "SELECT COUNT(*) AS n FROM announcements"
                " WHERE job_id = ? AND status IN ('pending', 'fetching')",
                (job_id,),
            ).fetchone()["n"]
            if not unfinished:
                return ("not_running", [])
            self._conn.execute(
                "UPDATE jobs SET cancelled = 1 WHERE id = ?", (job_id,)
            )
            self._conn.execute(
                "UPDATE announcements SET status = 'cancelled',"
                " error = COALESCE(error, 'job cancelled before this request was sent'),"
                " updated_at = ?"
                " WHERE job_id = ? AND status IN ('pending', 'fetching')",
                (time.time(), job_id),
            )
            rows = self._conn.execute(
                "SELECT id FROM announcements WHERE job_id = ? AND status = 'cancelled'",
                (job_id,),
            ).fetchall()
            return ("cancelled_now", [r["id"] for r in rows])

    def cancelled_job_ids(self):
        with self._lock:
            return [r["id"] for r in self._conn.execute(
                "SELECT id FROM jobs WHERE cancelled = 1"
            )]

    # ---- announcements -------------------------------------------------
    def add_announcement(self, job_id: str, url: str) -> int:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT INTO announcements (job_id, url, current_url, status, updated_at)"
                " VALUES (?, ?, ?, 'pending', ?)",
                (job_id, url, url, time.time()),
            )
            return cur.lastrowid

    def update_ann(self, ann_id: int, **fields) -> None:
        fields = {k: v for k, v in fields.items() if k in _UPDATABLE}
        fields["updated_at"] = time.time()
        cols = ", ".join(f"{k} = ?" for k in fields)
        with self._lock, self._conn:
            # A cancelled announcement is terminal: late writes from an in-flight
            # response (success/failure/retry/redirect) must not resurrect it.
            self._conn.execute(
                f"UPDATE announcements SET {cols} WHERE id = ? AND status != 'cancelled'",
                (*fields.values(), ann_id),
            )

    def list_announcements(self, job_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM announcements WHERE job_id = ? ORDER BY id", (job_id,)
            ).fetchall()

    def unfinished_announcements(self):
        with self._lock:
            return self._conn.execute(
                "SELECT id, job_id, current_url, hops, attempts FROM announcements"
                " WHERE status IN ('pending', 'fetching') ORDER BY id"
            ).fetchall()

    def cancelled_announcement_ids(self):
        with self._lock:
            return [r["id"] for r in self._conn.execute(
                "SELECT id FROM announcements WHERE status = 'cancelled'"
            )]

    # ---- cooldowns ------------------------------------------------------
    def set_cooldown(self, origin: str, until_ts: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO cooldowns (origin, until_ts) VALUES (?, ?)"
                " ON CONFLICT(origin) DO UPDATE SET until_ts = excluded.until_ts",
                (origin, until_ts),
            )

    def active_cooldowns(self, now: float):
        with self._lock:
            return self._conn.execute(
                "SELECT origin, until_ts FROM cooldowns WHERE until_ts > ?", (now,)
            ).fetchall()
