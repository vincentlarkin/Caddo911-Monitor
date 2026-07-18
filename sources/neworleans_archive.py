"""Append-only raw mirror for the official New Orleans calls-for-service dataset."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import os
import sqlite3
from typing import Callable

from . import neworleans


RAW_FIELDS = ",".join(
    (
        "nopd_item",
        "type_",
        "typetext",
        "priority",
        "initialtype",
        "initialtypetext",
        "initialpriority",
        "mapx",
        "mapy",
        "timecreate",
        "timedispatch",
        "timearrive",
        "timeclosed",
        "disposition",
        "dispositiontext",
        "selfinitiated",
        "beat",
        "block_address",
        "zip",
        "policedistrict",
        "location",
    )
)


def _connect(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    conn = sqlite3.connect(path, timeout=60, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 10000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id TEXT NOT NULL,
            dataset_year INTEGER NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            source_row_count INTEGER,
            observed_row_count INTEGER NOT NULL DEFAULT 0,
            new_call_count INTEGER NOT NULL DEFAULT 0,
            new_version_count INTEGER NOT NULL DEFAULT 0,
            changed_call_count INTEGER NOT NULL DEFAULT 0,
            error_text TEXT
        );

        CREATE TABLE IF NOT EXISTS call_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id TEXT NOT NULL,
            dataset_year INTEGER NOT NULL,
            nopd_item TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            timecreate TEXT,
            type_code TEXT,
            type_text TEXT,
            initial_type_code TEXT,
            initial_type_text TEXT,
            priority TEXT,
            initial_priority TEXT,
            self_initiated TEXT,
            disposition TEXT,
            disposition_text TEXT,
            beat TEXT,
            block_address TEXT,
            zip TEXT,
            police_district TEXT,
            latitude REAL,
            longitude REAL,
            raw_json TEXT NOT NULL,
            first_fetched_at TEXT NOT NULL,
            UNIQUE(dataset_id, nopd_item, payload_hash)
        );

        CREATE TABLE IF NOT EXISTS current_calls (
            dataset_id TEXT NOT NULL,
            nopd_item TEXT NOT NULL,
            version_id INTEGER NOT NULL,
            first_seen_run_id INTEGER NOT NULL,
            last_seen_run_id INTEGER NOT NULL,
            PRIMARY KEY(dataset_id, nopd_item),
            FOREIGN KEY(version_id) REFERENCES call_versions(id),
            FOREIGN KEY(first_seen_run_id) REFERENCES sync_runs(id),
            FOREIGN KEY(last_seen_run_id) REFERENCES sync_runs(id)
        );

        CREATE INDEX IF NOT EXISTS idx_nola_versions_timecreate
            ON call_versions(dataset_id, timecreate);
        CREATE INDEX IF NOT EXISTS idx_nola_versions_type
            ON call_versions(dataset_id, type_text);
        CREATE INDEX IF NOT EXISTS idx_nola_versions_self_initiated
            ON call_versions(dataset_id, self_initiated);
        CREATE INDEX IF NOT EXISTS idx_nola_current_version
            ON current_calls(version_id);
        CREATE INDEX IF NOT EXISTS idx_nola_runs_year
            ON sync_runs(dataset_year, started_at);
        """
    )
    conn.execute("PRAGMA user_version = 1")
    conn.commit()


def _canonical_payload(row: dict) -> tuple[str, str]:
    payload = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return payload, hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _coordinates(row: dict) -> tuple[float | None, float | None]:
    location = row.get("location")
    coordinates = location.get("coordinates") if isinstance(location, dict) else None
    if not isinstance(coordinates, (list, tuple)) or len(coordinates) < 2:
        return None, None
    try:
        longitude = float(coordinates[0])
        latitude = float(coordinates[1])
    except (TypeError, ValueError):
        return None, None
    if not (math.isfinite(latitude) and math.isfinite(longitude)):
        return None, None
    return latitude, longitude


def _year_where(year: int, *, last_item: str | None = None) -> str:
    where = (
        f"timecreate >= '{year:04d}-01-01T00:00:00' AND "
        f"timecreate < '{year + 1:04d}-01-01T00:00:00' AND "
        "nopd_item IS NOT NULL"
    )
    if last_item:
        safe_item = last_item.replace("'", "''")
        where += f" AND nopd_item > '{safe_item}'"
    return where


def _source_row_count(session, *, year: int, timeout_seconds: int) -> int:
    response = session.get(
        neworleans.FEED_URL,
        params={"$select": "count(*) as count", "$where": _year_where(year)},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    return int(payload[0]["count"]) if payload else 0


def _fetch_page(
    session,
    *,
    year: int,
    last_item: str | None,
    page_size: int,
    timeout_seconds: int,
) -> list[dict]:
    response = session.get(
        neworleans.FEED_URL,
        params={
            "$select": RAW_FIELDS,
            "$where": _year_where(year, last_item=last_item),
            "$order": "nopd_item ASC",
            "$limit": page_size,
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def mirror_year(
    year: int,
    *,
    db_path: str,
    user_agent: str,
    timeout_seconds: int = 120,
    page_size: int = 25000,
    progress: Callable[[str], None] | None = None,
) -> dict:
    """
    Mirror every official source row for a year without deleting raw history.

    New payload versions are appended to call_versions. current_calls is only a
    pointer to the most recently observed version; no prior payload is replaced.
    """
    if year < 2000 or year > 2100:
        raise ValueError("year must be between 2000 and 2100")
    if page_size < 100 or page_size > 50000:
        raise ValueError("page_size must be between 100 and 50000")

    emit = progress or (lambda _message: None)
    session = neworleans._session(user_agent=user_agent)
    source_count = _source_row_count(session, year=year, timeout_seconds=timeout_seconds)
    conn = _connect(db_path)
    _init_schema(conn)
    started_at = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        """
        INSERT INTO sync_runs(dataset_id, dataset_year, started_at, status, source_row_count)
        VALUES (?, ?, ?, 'running', ?)
        """,
        (neworleans.DATASET_ID, year, started_at, source_count),
    )
    run_id = int(cursor.lastrowid)
    conn.commit()

    observed = 0
    new_calls = 0
    new_versions = 0
    changed_calls = 0
    last_item: str | None = None
    fetched_at = started_at

    try:
        while True:
            page = _fetch_page(
                session,
                year=year,
                last_item=last_item,
                page_size=page_size,
                timeout_seconds=timeout_seconds,
            )
            if not page:
                break

            for row in page:
                item_id = neworleans._clean_ws(row.get("nopd_item"))
                if not item_id:
                    continue
                raw_json, payload_hash = _canonical_payload(row)
                latitude, longitude = _coordinates(row)
                existing_current = conn.execute(
                    "SELECT version_id FROM current_calls WHERE dataset_id = ? AND nopd_item = ?",
                    (neworleans.DATASET_ID, item_id),
                ).fetchone()

                inserted = conn.execute(
                    """
                    INSERT OR IGNORE INTO call_versions(
                        dataset_id, dataset_year, nopd_item, payload_hash, timecreate,
                        type_code, type_text, initial_type_code, initial_type_text,
                        priority, initial_priority, self_initiated, disposition,
                        disposition_text, beat, block_address, zip, police_district,
                        latitude, longitude, raw_json, first_fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        neworleans.DATASET_ID,
                        year,
                        item_id,
                        payload_hash,
                        row.get("timecreate"),
                        row.get("type_"),
                        row.get("typetext"),
                        row.get("initialtype"),
                        row.get("initialtypetext"),
                        row.get("priority"),
                        row.get("initialpriority"),
                        row.get("selfinitiated"),
                        row.get("disposition"),
                        row.get("dispositiontext"),
                        row.get("beat"),
                        row.get("block_address"),
                        row.get("zip"),
                        row.get("policedistrict"),
                        latitude,
                        longitude,
                        raw_json,
                        fetched_at,
                    ),
                )
                if inserted.rowcount == 1:
                    new_versions += 1

                version_row = conn.execute(
                    """
                    SELECT id FROM call_versions
                    WHERE dataset_id = ? AND nopd_item = ? AND payload_hash = ?
                    """,
                    (neworleans.DATASET_ID, item_id, payload_hash),
                ).fetchone()
                version_id = int(version_row["id"])
                if existing_current is None:
                    new_calls += 1
                elif int(existing_current["version_id"]) != version_id:
                    changed_calls += 1

                conn.execute(
                    """
                    INSERT INTO current_calls(
                        dataset_id, nopd_item, version_id, first_seen_run_id, last_seen_run_id
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(dataset_id, nopd_item) DO UPDATE SET
                        version_id = excluded.version_id,
                        last_seen_run_id = excluded.last_seen_run_id
                    """,
                    (neworleans.DATASET_ID, item_id, version_id, run_id, run_id),
                )
                observed += 1

            last_item = neworleans._clean_ws(page[-1].get("nopd_item"))
            conn.execute(
                """
                UPDATE sync_runs
                SET observed_row_count = ?, new_call_count = ?,
                    new_version_count = ?, changed_call_count = ?
                WHERE id = ?
                """,
                (observed, new_calls, new_versions, changed_calls, run_id),
            )
            conn.commit()
            emit(f"[NOLA RAW] Mirrored {observed:,}/{source_count:,} rows")
            if len(page) < page_size:
                break

        status = "complete" if observed == source_count else "count_mismatch"
        finished_at = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """
            UPDATE sync_runs
            SET finished_at = ?, status = ?, observed_row_count = ?,
                new_call_count = ?, new_version_count = ?, changed_call_count = ?
            WHERE id = ?
            """,
            (
                finished_at,
                status,
                observed,
                new_calls,
                new_versions,
                changed_calls,
                run_id,
            ),
        )
        conn.commit()
        quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]
        current_count = conn.execute(
            "SELECT COUNT(*) FROM current_calls WHERE dataset_id = ?",
            (neworleans.DATASET_ID,),
        ).fetchone()[0]
        version_count = conn.execute(
            "SELECT COUNT(*) FROM call_versions WHERE dataset_id = ?",
            (neworleans.DATASET_ID,),
        ).fetchone()[0]
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return {
            "db_path": os.path.abspath(db_path),
            "run_id": run_id,
            "status": status,
            "source_rows": source_count,
            "observed_rows": observed,
            "current_calls": int(current_count),
            "call_versions": int(version_count),
            "new_calls": new_calls,
            "new_versions": new_versions,
            "changed_calls": changed_calls,
            "quick_check": quick_check,
        }
    except Exception as exc:
        conn.execute(
            """
            UPDATE sync_runs
            SET finished_at = ?, status = 'failed', observed_row_count = ?,
                new_call_count = ?, new_version_count = ?, changed_call_count = ?,
                error_text = ?
            WHERE id = ?
            """,
            (
                datetime.now(timezone.utc).isoformat(),
                observed,
                new_calls,
                new_versions,
                changed_calls,
                str(exc)[:2000],
                run_id,
            ),
        )
        conn.commit()
        raise
    finally:
        conn.close()
