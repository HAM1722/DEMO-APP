import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, Iterable, List, Optional

DB_PATH = "app.db"


@contextmanager
def _get_conn(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: str = DB_PATH) -> None:
    with _get_conn(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                total_records INTEGER NOT NULL,
                ok_records INTEGER NOT NULL,
                warn_records INTEGER NOT NULL,
                fail_records INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                sensor_id TEXT NOT NULL,
                value REAL NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES runs (id)
            )
            """
        )


def create_run(
    status: str,
    total_records: int,
    ok_records: int,
    warn_records: int,
    fail_records: int,
    created_at: Optional[str] = None,
    db_path: str = DB_PATH,
) -> int:
    created_at = created_at or datetime.utcnow().isoformat()
    with _get_conn(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO runs (created_at, status, total_records, ok_records, warn_records, fail_records)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (created_at, status, total_records, ok_records, warn_records, fail_records),
        )
        return int(cur.lastrowid)


def add_records(
    run_id: int,
    records: Iterable[Dict],
    db_path: str = DB_PATH,
) -> None:
    with _get_conn(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO records (run_id, sensor_id, value, status, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    record["sensor_id"],
                    record["value"],
                    record["status"],
                    record["created_at"],
                )
                for record in records
            ],
        )


def get_runs(limit: int = 50, db_path: str = DB_PATH) -> List[Dict]:
    with _get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, created_at, status, total_records, ok_records, warn_records, fail_records
            FROM runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


def get_latest_run(db_path: str = DB_PATH) -> Optional[Dict]:
    runs = get_runs(limit=1, db_path=db_path)
    return runs[0] if runs else None


def get_records_for_run(run_id: int, db_path: str = DB_PATH) -> List[Dict]:
    with _get_conn(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, run_id, sensor_id, value, status, created_at
            FROM records
            WHERE run_id = ?
            ORDER BY id ASC
            """,
            (run_id,),
        ).fetchall()
        return [dict(row) for row in rows]
