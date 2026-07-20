#!/usr/bin/env python3
"""
Louisiana 911 - statewide public emergency incident monitor.

Aggregates official live and delayed public-safety feeds without claiming to
be an emergency service or a replacement for calling 911.
"""

import sqlite3
import hashlib
import time
import argparse
import os
import sys
import math
import re
import json
from difflib import SequenceMatcher
from datetime import datetime, timezone, timedelta
from threading import Lock, Thread
from flask import Flask, jsonify, redirect, request, send_from_directory
from geopy.geocoders import Nominatim
from apscheduler.schedulers.background import BackgroundScheduler
from zoneinfo import ZoneInfo
from sources import caddo as caddo_source
from sources import lafayette as lafayette_source
from sources import batonrouge as batonrouge_source
from sources import neworleans as neworleans_source
from sources import neworleans_archive as neworleans_archive_source

app = Flask(__name__, static_folder='public', static_url_path='')


def _env_setting(name: str, legacy_name: str, default: str = '') -> str:
    """Read the Louisiana911 setting while preserving Caddo911 deployments."""
    primary = os.environ.get(name)
    if primary is not None:
        return primary
    legacy = os.environ.get(legacy_name)
    if legacy is not None:
        return legacy
    return default


# Database setup
def _resolve_db_path() -> str:
    """
    Pick the live SQLite database path.

    Production installs commonly keep the git checkout in ./repo and the
    persistent SQLite files in a sibling ./data directory. Keep
    LOUISIANA911_DB_PATH (or the legacy CADDO911_DB_PATH) is the strongest
    override. The stored filename remains compatible with existing installs.
    """
    configured_path = _env_setting('LOUISIANA911_DB_PATH', 'CADDO911_DB_PATH').strip()
    if configured_path:
        return os.path.expanduser(configured_path)

    app_dir = os.path.dirname(os.path.abspath(__file__))
    cwd = os.getcwd()
    configured_data_dir = _env_setting('LOUISIANA911_DATA_DIR', 'CADDO911_DATA_DIR').strip()
    candidate_dirs = [
        configured_data_dir,
        os.path.join(app_dir, 'data'),
        os.path.join(os.path.dirname(app_dir), 'data'),
        os.path.join(cwd, 'data'),
        os.path.join(os.path.dirname(cwd), 'data'),
        '/data',
    ]

    seen: set[str] = set()
    for data_dir in candidate_dirs:
        if not data_dir:
            continue
        db_path = os.path.abspath(os.path.expanduser(os.path.join(data_dir, 'caddo911.db')))
        if db_path in seen:
            continue
        seen.add(db_path)
        if os.path.exists(db_path):
            return db_path

    return 'caddo911.db'


DB_PATH = _resolve_db_path()

# Archive settings: incidents older than this many days get moved to monthly archive DBs
ARCHIVE_AFTER_DAYS = int(_env_setting('LOUISIANA911_ARCHIVE_DAYS', 'CADDO911_ARCHIVE_DAYS', '30'))
BACKUP_RETENTION_WEEKS = int(_env_setting('LOUISIANA911_BACKUP_RETENTION_WEEKS', 'CADDO911_BACKUP_RETENTION_WEEKS', '5'))
REPORT_CACHE_VERSION = 4

def _get_archive_dir() -> str:
    """Get the directory where archive DBs are stored (same dir as main DB)."""
    return os.path.dirname(os.path.abspath(DB_PATH)) or '.'

def _get_archive_db_path(year: int, month: int) -> str:
    """Get path for a monthly archive database."""
    archive_dir = _get_archive_dir()
    return os.path.join(archive_dir, f"caddo911_archive_{year:04d}_{month:02d}.db")

def _list_archive_dbs() -> list[str]:
    """List all archive database files."""
    archive_dir = _get_archive_dir()
    if not os.path.isdir(archive_dir):
        return []
    return sorted([
        os.path.join(archive_dir, f) 
        for f in os.listdir(archive_dir) 
        if f.startswith('caddo911_archive_') and f.endswith('.db')
    ])


def _get_neworleans_raw_db_path(year: int) -> str:
    """Dedicated append-only raw mirror path for an annual NOLA dataset."""
    configured = _env_setting(
        'LOUISIANA911_NOLA_RAW_DB_PATH',
        'CADDO911_NOLA_RAW_DB_PATH',
    ).strip()
    if configured:
        expanded = os.path.expanduser(configured)
        return expanded.format(year=year) if '{year}' in expanded else expanded
    return os.path.join(_get_archive_dir(), f'neworleans_calls_{year:04d}.db')


def _list_neworleans_raw_dbs() -> list[str]:
    archive_dir = _get_archive_dir()
    if not os.path.isdir(archive_dir):
        return []
    return sorted(
        os.path.join(archive_dir, name)
        for name in os.listdir(archive_dir)
        if re.fullmatch(r'neworleans_calls_\d{4}\.db', name)
    )

def _get_backup_dir() -> str:
    """Directory where periodic backup snapshots are written."""
    configured = _env_setting('LOUISIANA911_BACKUP_DIR', 'CADDO911_BACKUP_DIR').strip()
    if configured:
        return configured
    return os.path.join(_get_archive_dir(), "backups")


def _get_report_cache_dir() -> str:
    """Directory where static monthly report JSON files are written."""
    return os.path.join(_get_archive_dir(), "report_cache")

def _backup_label_for_path(path: str) -> str:
    """Stable label for backup file naming."""
    abs_target = os.path.abspath(path)
    abs_main = os.path.abspath(DB_PATH)
    if abs_target == abs_main:
        return "main"
    base = os.path.splitext(os.path.basename(path))[0]
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", base).strip("_") or "db"

def _sqlite_hot_backup(src_path: str, dst_path: str) -> None:
    """
    SQLite-consistent snapshot copy using the built-in backup API.
    Safer than raw file copies when WAL is enabled.
    """
    src_conn = sqlite3.connect(src_path, timeout=30, check_same_thread=False)
    dst_conn = sqlite3.connect(dst_path, timeout=30, check_same_thread=False)
    try:
        src_conn.execute("PRAGMA busy_timeout = 5000;")
        src_conn.backup(dst_conn)
        dst_conn.commit()
    finally:
        try:
            dst_conn.close()
        except Exception:
            pass
        try:
            src_conn.close()
        except Exception:
            pass

def _prune_old_backups(backup_dir: str) -> list[str]:
    """
    Keep only N weekly backups per database label if retention is set.
    Returns a list of deleted files.
    """
    keep = int(BACKUP_RETENTION_WEEKS)
    if keep <= 0 or not os.path.isdir(backup_dir):
        return []
    grouped: dict[str, list[str]] = {}
    for name in os.listdir(backup_dir):
        if not name.endswith(".db"):
            continue
        full = os.path.join(backup_dir, name)
        if not os.path.isfile(full):
            continue
        # Format: <label>_YYYYMMDD_HHMMSS.db
        m = re.match(r"^(?P<label>.+)_\d{8}_\d{6}\.db$", name)
        if not m:
            continue
        label = m.group("label")
        grouped.setdefault(label, []).append(full)
    removed: list[str] = []
    for _, files in grouped.items():
        files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        for stale in files[keep:]:
            try:
                os.remove(stale)
                removed.append(stale)
            except Exception:
                pass
    return removed

def create_backup_snapshot(*, include_archives: bool = True) -> dict:
    """
    Create timestamped SQLite backups.
    - Always includes the main DB.
    - Optionally includes monthly archives and annual raw NOLA mirrors.
    """
    backup_dir = _get_backup_dir()
    os.makedirs(backup_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    targets = [DB_PATH]
    if include_archives:
        targets.extend(_list_archive_dbs())
        targets.extend(_list_neworleans_raw_dbs())
    created: list[str] = []
    skipped: list[str] = []
    for src in targets:
        if not os.path.exists(src):
            skipped.append(src)
            continue
        label = _backup_label_for_path(src)
        out_path = os.path.join(backup_dir, f"{label}_{stamp}.db")
        _sqlite_hot_backup(src, out_path)
        created.append(out_path)
    removed = _prune_old_backups(backup_dir)
    return {
        "created": created,
        "skipped": skipped,
        "removed": removed,
        "backup_dir": backup_dir,
    }

def _archive_db_connect(path: str, *, row_factory: bool = False) -> sqlite3.Connection:
    """Connect to an archive database."""
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    if row_factory:
        conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn

def _init_archive_db(path: str) -> None:
    """Initialize an archive database with the same schema as main DB."""
    conn = _archive_db_connect(path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS incidents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT UNIQUE,
            agency TEXT,
            time TEXT,
            units INTEGER,
            description TEXT,
            street TEXT,
            cross_streets TEXT,
            municipality TEXT,
            source TEXT DEFAULT 'caddo',
            latitude REAL,
            longitude REAL,
            first_seen DATETIME,
            last_seen DATETIME,
            is_active INTEGER DEFAULT 0,
            geocode_source TEXT,
            geocode_quality TEXT,
            geocode_query TEXT,
            geocoded_at DATETIME,
            geocode_version INTEGER
        )
    ''')
    try:
        cursor.execute("ALTER TABLE incidents ADD COLUMN source TEXT DEFAULT 'caddo'")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE incidents ADD COLUMN geocode_version INTEGER")
    except sqlite3.OperationalError:
        pass
    cursor.execute("UPDATE incidents SET source = 'caddo' WHERE source IS NULL OR TRIM(source) = ''")
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash ON incidents(hash)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_first_seen ON incidents(first_seen)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_source ON incidents(source)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_source_first_seen ON incidents(source, first_seen DESC)')
    conn.commit()
    conn.close()

# Scraper/status metadata
SCRAPE_INTERVAL_SECONDS_DEFAULT = 60
feed_refreshed_at: str | None = None  # backwards-compatible: Caddo refresh text
feed_refreshed_by_source: dict[str, str | None] = {
    "caddo": None,
    "lafayette": None,
    "batonrouge": None,
    "neworleans": None,
}
last_scrape_started_at: str | None = None  # ISO UTC
last_scrape_finished_at: str | None = None  # ISO UTC

# Identify the scraper politely in upstream logs.
SCRAPER_USER_AGENT = "Louisiana911.com public-safety feed monitor (+https://louisiana911.com/)"

QUIET = False

# Louisiana is in US Central time. Use an IANA name so DST is handled (CST/CDT).
# On Windows, this requires the `tzdata` pip package (added to requirements.txt).
# If zoneinfo data isn't available at runtime, fall back to a fixed CST offset so we don't crash.
CENTRAL_TZ_IS_FALLBACK = False
try:
    CENTRAL_TZ = ZoneInfo("America/Chicago")
except Exception:
    CENTRAL_TZ = timezone(timedelta(hours=-6))  # CST (no DST)
    CENTRAL_TZ_IS_FALLBACK = True

def log(message: str) -> None:
    """Lightweight logger (avoids emoji for Windows terminals)."""
    if not QUIET:
        print(message, flush=True)

def db_connect(*, row_factory: bool = False) -> sqlite3.Connection:
    """
    Create a SQLite connection with sensible defaults for concurrent reader/writer usage
    (collector + web UI in the same process).
    """
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    if row_factory:
        conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn

def init_db():
    """Initialize SQLite database with incidents table"""
    conn = db_connect()
    cursor = conn.cursor()
    # Better concurrency for collector + UI
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA synchronous = NORMAL;")
    cursor.execute("PRAGMA busy_timeout = 5000;")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS incidents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT UNIQUE,
            agency TEXT,
            time TEXT,
            units INTEGER,
            description TEXT,
            street TEXT,
            cross_streets TEXT,
            municipality TEXT,
            source TEXT DEFAULT 'caddo',
            latitude REAL,
            longitude REAL,
            first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            is_active INTEGER DEFAULT 1
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash ON incidents(hash)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_active ON incidents(is_active)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_first_seen ON incidents(first_seen)')

    # Add geocoding metadata columns (safe to run on an existing DB; does NOT delete data)
    # Note: SQLite doesn't support ADD COLUMN IF NOT EXISTS in older versions, so we try/except.
    for col_name, col_type in (
        ("source", "TEXT DEFAULT 'caddo'"),
        ("geocode_source", "TEXT"),     # 'arcgis' | 'osm' | 'unresolved'
        ("geocode_quality", "TEXT"),    # 'street-segment' | 'intersection-2' | 'street+cross' | lower-confidence values
        ("geocode_query", "TEXT"),      # the query string we sent to the provider
        ("geocoded_at", "DATETIME"),    # UTC ISO timestamp
        ("geocode_version", "INTEGER"), # identifies results made by the current validation/ranking logic
    ):
        try:
            cursor.execute(f"ALTER TABLE incidents ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass

    # Backfill legacy rows that predate multi-source support.
    cursor.execute("UPDATE incidents SET source = 'caddo' WHERE source IS NULL OR TRIM(source) = ''")
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_source ON incidents(source)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_source_active_first_seen ON incidents(source, is_active, first_seen DESC)')

    # Metadata table (shared across collector + web processes)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    conn.commit()
    conn.close()

def meta_set(key: str, value: str | None) -> None:
    if value is None:
        return
    conn = db_connect()
    conn.execute(
        "INSERT INTO meta(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    conn.commit()
    conn.close()

def meta_get_many(keys: list[str]) -> dict[str, str]:
    conn = db_connect(row_factory=True)
    cursor = conn.cursor()
    placeholders = ",".join(["?"] * len(keys))
    cursor.execute(f"SELECT key, value FROM meta WHERE key IN ({placeholders})", keys)
    rows = cursor.fetchall()
    conn.close()
    return {row["key"]: row["value"] for row in rows} if rows else {}

def archive_old_incidents(*, dry_run: bool = False) -> dict:
    """
    Move incidents older than ARCHIVE_AFTER_DAYS to monthly archive databases.
    Returns stats about what was archived.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=ARCHIVE_AFTER_DAYS)
    cutoff_iso = cutoff.isoformat()
    
    conn = db_connect(row_factory=True)
    cursor = conn.cursor()
    
    # Find inactive incidents older than cutoff
    cursor.execute('''
        SELECT * FROM incidents 
        WHERE is_active = 0 AND first_seen < ?
        ORDER BY first_seen ASC
    ''', (cutoff_iso,))
    rows = cursor.fetchall()
    
    if not rows:
        conn.close()
        log(f"[ARCHIVE] No incidents older than {ARCHIVE_AFTER_DAYS} days to archive")
        return {'archived': 0, 'files': []}
    
    log(f"[ARCHIVE] Found {len(rows)} incidents to archive{' (dry run)' if dry_run else ''}")
    
    # Group by year-month based on first_seen (in Central time)
    by_month: dict[tuple[int, int], list[dict]] = {}
    for row in rows:
        row_dict = dict(row)
        first_seen = row_dict.get('first_seen')
        if not first_seen:
            continue
        dt = _parse_iso_datetime(first_seen)
        if not dt:
            continue
        # Convert to Central time for archiving by local month
        central_dt = dt.astimezone(CENTRAL_TZ)
        key = (central_dt.year, central_dt.month)
        if key not in by_month:
            by_month[key] = []
        by_month[key].append(row_dict)
    
    archived_count = 0
    archived_files = []
    hashes_to_delete = []
    
    for (year, month), incidents in sorted(by_month.items()):
        archive_path = _get_archive_db_path(year, month)
        log(f"[ARCHIVE] {year}-{month:02d}: {len(incidents)} incidents -> {os.path.basename(archive_path)}")
        
        if not dry_run:
            _init_archive_db(archive_path)
            archive_conn = _archive_db_connect(archive_path)
            archive_cursor = archive_conn.cursor()
            
            for inc in incidents:
                # Insert into archive (use INSERT OR IGNORE to handle duplicates)
                try:
                    archive_cursor.execute('''
                        INSERT OR IGNORE INTO incidents 
                        (hash, agency, time, units, description, street, cross_streets, municipality, source,
                         latitude, longitude, first_seen, last_seen, is_active,
                         geocode_source, geocode_quality, geocode_query, geocoded_at, geocode_version)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        inc.get('hash'),
                        inc.get('agency'),
                        inc.get('time'),
                        inc.get('units'),
                        inc.get('description'),
                        inc.get('street'),
                        inc.get('cross_streets'),
                        inc.get('municipality'),
                        inc.get('source') or 'caddo',
                        inc.get('latitude'),
                        inc.get('longitude'),
                        inc.get('first_seen'),
                        inc.get('last_seen'),
                        inc.get('is_active', 0),
                        inc.get('geocode_source'),
                        inc.get('geocode_quality'),
                        inc.get('geocode_query'),
                        inc.get('geocoded_at'),
                        inc.get('geocode_version'),
                    ))
                    hashes_to_delete.append(inc.get('hash'))
                    archived_count += 1
                except Exception as e:
                    log(f"[ARCHIVE] Error archiving incident {inc.get('hash')}: {e}")
            
            archive_conn.commit()
            archive_conn.close()
        else:
            archived_count += len(incidents)
        
        if archive_path not in archived_files:
            archived_files.append(archive_path)
    
    # Delete archived incidents from main DB
    if not dry_run and hashes_to_delete:
        log(f"[ARCHIVE] Removing {len(hashes_to_delete)} archived incidents from main DB...")
        for h in hashes_to_delete:
            cursor.execute('DELETE FROM incidents WHERE hash = ?', (h,))
        conn.commit()
        
        # Vacuum to reclaim space
        log("[ARCHIVE] Running VACUUM to reclaim space...")
        conn.execute("VACUUM")
    
    conn.close()
    
    log(f"[ARCHIVE] Complete! Archived {archived_count} incidents to {len(archived_files)} file(s)")
    return {'archived': archived_count, 'files': archived_files}

def _get_archive_dbs_for_date(date_str: str) -> list[str]:
    """Get archive DB paths that might contain data for a given date (YYYY-MM-DD)."""
    try:
        year, month, _ = date_str.split('-')
        archive_path = _get_archive_db_path(int(year), int(month))
        if os.path.exists(archive_path):
            return [archive_path]
    except Exception:
        pass
    return []

def _get_archive_dbs_for_month(month_str: str) -> list[str]:
    """Get archive DB paths for a given month (YYYY-MM)."""
    try:
        year, month = month_str.split('-')
        archive_path = _get_archive_db_path(int(year), int(month))
        if os.path.exists(archive_path):
            return [archive_path]
    except Exception:
        pass
    return []

def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        # We store timezone-aware ISO timestamps (UTC) via datetime.now(timezone.utc).isoformat()
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            # Treat naive timestamps as UTC for backwards compatibility.
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def _central_date_bounds_utc(date_str: str) -> tuple[str, str] | None:
    """Return UTC ISO start/end for a Central date (YYYY-MM-DD)."""
    try:
        year_s, month_s, day_s = (date_str or "").split("-")
        year, month, day = int(year_s), int(month_s), int(day_s)
        start_local = datetime(year, month, day, tzinfo=CENTRAL_TZ)
        end_local = start_local + timedelta(days=1)
        start_utc = start_local.astimezone(timezone.utc).isoformat()
        end_utc = end_local.astimezone(timezone.utc).isoformat()
        return start_utc, end_utc
    except Exception:
        return None

def _central_month_bounds_utc(month_str: str) -> tuple[str, str] | None:
    """Return UTC ISO start/end for a Central month (YYYY-MM)."""
    try:
        year_s, month_s = (month_str or "").split("-")
        year, month = int(year_s), int(month_s)
        start_local = datetime(year, month, 1, tzinfo=CENTRAL_TZ)
        if month == 12:
            end_local = datetime(year + 1, 1, 1, tzinfo=CENTRAL_TZ)
        else:
            end_local = datetime(year, month + 1, 1, tzinfo=CENTRAL_TZ)
        start_utc = start_local.astimezone(timezone.utc).isoformat()
        end_utc = end_local.astimezone(timezone.utc).isoformat()
        return start_utc, end_utc
    except Exception:
        return None

def _central_date_key(value: str | None) -> str | None:
    dt = _parse_iso_datetime(value)
    c = _to_central(dt)
    return c.date().isoformat() if c else None


def _central_month_key(value: str | None) -> str | None:
    dt = _parse_iso_datetime(value)
    c = _to_central(dt)
    return c.strftime('%Y-%m') if c else None

def _to_central(dt: datetime | None) -> datetime | None:
    if not dt:
        return None
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(CENTRAL_TZ)
    except Exception:
        return None

def _format_central_hms(dt: datetime | None) -> str | None:
    c = _to_central(dt)
    if not c:
        return None
    abbr = "CST" if CENTRAL_TZ_IS_FALLBACK else c.strftime("%Z")
    return f"{c.strftime('%H:%M:%S')} {abbr}"

def _format_central_tooltip(dt: datetime | None) -> str | None:
    c = _to_central(dt)
    if not c:
        return None
    abbr = "CST" if CENTRAL_TZ_IS_FALLBACK else c.strftime("%Z")
    return f"{c.strftime('%Y-%m-%d %H:%M:%S')} {abbr}"

# Ensure schema exists even when running under a WSGI server (e.g. gunicorn app:app)
# Safe to call multiple times.
try:
    init_db()
except Exception as e:
    # Don't crash import; the process may not have its volume mounted yet.
    log(f"[WARN] Database init failed: {e}")

# --- Security hardening (optional auth + headers) ---
AUTH_TOKEN = _env_setting('LOUISIANA911_AUTH_TOKEN', 'CADDO911_AUTH_TOKEN') or None
AUTH_USER = _env_setting('LOUISIANA911_AUTH_USER', 'CADDO911_AUTH_USER') or None
AUTH_PASS = _env_setting('LOUISIANA911_AUTH_PASS', 'CADDO911_AUTH_PASS') or None

# Simple in-process report rate limiting. This is intentionally lightweight:
# enough to slow accidental refresh loops and casual abuse without adding
# infrastructure or dependencies.
REPORT_PAGE_RATE_LIMIT = int(_env_setting('LOUISIANA911_REPORT_PAGE_RATE_LIMIT', 'CADDO911_REPORT_PAGE_RATE_LIMIT', '120'))
REPORT_API_RATE_LIMIT = int(_env_setting('LOUISIANA911_REPORT_API_RATE_LIMIT', 'CADDO911_REPORT_API_RATE_LIMIT', '90'))
REPORT_RATE_WINDOW_SECONDS = int(_env_setting('LOUISIANA911_REPORT_RATE_WINDOW_SECONDS', 'CADDO911_REPORT_RATE_WINDOW_SECONDS', '60'))
_report_rate_lock = Lock()
_report_rate_hits: dict[tuple[str, str], list[float]] = {}

CANONICAL_SITE_HOST = 'louisiana911.com'


def _first_forwarded_header_value(value: str) -> str:
    """Return the first proxy-provided value from a comma-separated header."""
    return (value or '').split(',', 1)[0].strip()


@app.before_request
def _canonical_origin_redirect():
    """Keep public traffic on the single HTTPS, apex-domain canonical origin."""
    forwarded_host = _first_forwarded_header_value(
        request.headers.get('X-Forwarded-Host', '')
    )
    public_host = (forwarded_host or request.host).split(':', 1)[0].lower()
    if public_host not in {CANONICAL_SITE_HOST, f'www.{CANONICAL_SITE_HOST}'}:
        return None

    forwarded_proto = _first_forwarded_header_value(
        request.headers.get('X-Forwarded-Proto', '')
    ).lower()
    cloudflare_visitor = request.headers.get('CF-Visitor', '').lower()
    uses_plain_http = (
        forwarded_proto == 'http'
        or ('"scheme":"http"' in cloudflare_visitor.replace(' ', ''))
    )
    if public_host == f'www.{CANONICAL_SITE_HOST}' or uses_plain_http:
        path_and_query = request.full_path
        if path_and_query.endswith('?'):
            path_and_query = path_and_query[:-1]
        return redirect(
            f'https://{CANONICAL_SITE_HOST}{path_and_query}',
            code=301,
        )
    return None

def _unauthorized():
    resp = jsonify({'error': 'unauthorized'})
    resp.status_code = 401
    if AUTH_USER and AUTH_PASS:
        resp.headers['WWW-Authenticate'] = 'Basic realm="louisiana911-live"'
    return resp

def _check_auth() -> bool:
    # No auth configured → allow
    if not AUTH_TOKEN and not (AUTH_USER and AUTH_PASS):
        return True

    if AUTH_TOKEN:
        provided = request.headers.get('X-Auth-Token')
        return bool(provided) and provided == AUTH_TOKEN

    auth = request.authorization
    return bool(auth) and auth.username == AUTH_USER and auth.password == AUTH_PASS


def _client_ip_for_rate_limit() -> str:
    forwarded_for = request.headers.get('X-Forwarded-For', '')
    if forwarded_for:
        return forwarded_for.split(',', 1)[0].strip() or 'unknown'
    return request.remote_addr or 'unknown'


def _report_rate_bucket(path: str) -> tuple[str, int] | None:
    clean_path = path.rstrip('/') or '/'
    if clean_path.startswith('/api/reports/'):
        return 'report-api', REPORT_API_RATE_LIMIT
    if clean_path == '/reports' or clean_path.startswith('/reports/'):
        return 'report-page', REPORT_PAGE_RATE_LIMIT
    return None


def _rate_limited_response(retry_after_seconds: int):
    resp = jsonify({
        'error': 'rate_limited',
        'message': 'Too many report requests. Please wait a moment and try again.',
        'retryAfterSeconds': retry_after_seconds,
    })
    resp.status_code = 429
    resp.headers['Retry-After'] = str(max(1, retry_after_seconds))
    return resp


def _check_report_rate_limit():
    if REPORT_RATE_WINDOW_SECONDS <= 0:
        return None

    bucket = _report_rate_bucket(request.path)
    if bucket is None:
        return None

    scope, limit = bucket
    if limit <= 0:
        return None

    now = time.monotonic()
    cutoff = now - REPORT_RATE_WINDOW_SECONDS
    key = (_client_ip_for_rate_limit(), scope)

    with _report_rate_lock:
        hits = [stamp for stamp in _report_rate_hits.get(key, []) if stamp > cutoff]
        if len(hits) >= limit:
            oldest = min(hits) if hits else now
            retry_after = max(1, int(math.ceil(REPORT_RATE_WINDOW_SECONDS - (now - oldest))))
            _report_rate_hits[key] = hits
            return _rate_limited_response(retry_after)

        hits.append(now)
        _report_rate_hits[key] = hits

        # Opportunistic cleanup so the dict does not grow forever.
        if len(_report_rate_hits) > 2000:
            stale_keys = [
                stored_key for stored_key, stored_hits in _report_rate_hits.items()
                if not any(stamp > cutoff for stamp in stored_hits)
            ]
            for stored_key in stale_keys:
                _report_rate_hits.pop(stored_key, None)

    return None


@app.before_request
def _auth_middleware():
    # Allow container health checks without auth
    if request.path == '/healthz':
        return None
    if not _check_auth():
        return _unauthorized()
    limited = _check_report_rate_limit()
    if limited is not None:
        return limited
    return None

@app.after_request
def _security_headers(resp):
    # Minimal hardening headers (avoid breaking external map tiles/scripts)
    resp.headers.setdefault('X-Content-Type-Options', 'nosniff')
    resp.headers.setdefault('X-Frame-Options', 'DENY')
    resp.headers.setdefault('Referrer-Policy', 'same-origin')
    resp.headers.setdefault('Permissions-Policy', 'geolocation=(self), microphone=(), camera=()')

    # The HTML and APIs stay fresh, while explicitly versioned shell assets can
    # be reused without a validation round trip. Every deploy changes the
    # version query string before these files change.
    versioned_shell_assets = {
        '/styles.css',
        '/service-worker.js',
        '/manifest.webmanifest',
    }
    if request.path in versioned_shell_assets and request.args.get('v'):
        resp.headers['Cache-Control'] = 'public, max-age=31536000, immutable'
    return resp

# Geocoder setup with caching - try ArcGIS first (better US coverage), fallback to Nominatim
from geopy.geocoders import ArcGIS
geolocator_arcgis = ArcGIS(timeout=5)
geolocator_osm = Nominatim(user_agent=SCRAPER_USER_AGENT, timeout=5)
geocode_cache = {}
geocode_intersection_cache = {}

# Increment whenever stored coordinates need to be reconsidered because the
# validation/ranking algorithm changed. Version 4 removed CAD-only road
# discriminators; version 5 added validated NOPD block/intersection fallbacks;
# version 6 adds Baton Rouge address-range and corridor normalization plus
# official City-Parish traffic-map point enrichment.
GEOCODER_VERSION = 6
ARCGIS_INTERSECTION_MIN_SCORE = 90.0
ARCGIS_STREET_MIN_SCORE = 85.0
ARCGIS_ADDRESS_MIN_SCORE = 90.0
MAX_STREET_SEGMENT_METERS = 8000.0

ROAD_TYPE_TOKENS = {
    "ALY", "ALLEY", "AV", "AVE", "AVENUE", "BLVD", "BOULEVARD",
    "CIR", "CIRCLE", "CT", "COURT", "DR", "DRIVE", "EXPY", "EXPRESSWAY",
    "FWY", "FREEWAY", "HWY", "HIGHWAY", "LN", "LANE", "LOOP", "PASSWAY",
    "PKWY", "PARKWAY", "PL", "PLACE", "RD", "ROAD", "ST", "STREET",
    "TER", "TERRACE", "TRL", "TRAIL", "WAY",
}
DIRECTION_ALIASES = {
    "N": "N", "NORTH": "N",
    "S": "S", "SOUTH": "S",
    "E": "E", "EAST": "E",
    "W": "W", "WEST": "W",
    "NE": "NE", "NORTHEAST": "NE",
    "NW": "NW", "NORTHWEST": "NW",
    "SE": "SE", "SOUTHEAST": "SE",
    "SW": "SW", "SOUTHWEST": "SW",
}

# 2020 Census parish outline for Caddo. Using the real shape keeps west Bossier
# hits from slipping through the old coarse longitude cutoff.
CADDO_PARISH_RING_LON_LAT = [
    (-94.043147, 32.693030),
    (-94.043147, 32.693031),
    (-94.042947, 32.767991),
    (-94.043027, 32.776863),
    (-94.042938, 32.780558),
    (-94.042829, 32.785277),
    (-94.042747, 32.786973),
    (-94.043026, 32.797476),
    (-94.042785, 32.871486),
    (-94.043025, 32.880446),
    (-94.042886, 32.880965),
    (-94.042886, 32.881089),
    (-94.042859, 32.892771),
    (-94.042885, 32.898911),
    (-94.043092, 32.910021),
    (-94.043067, 32.937903),
    (-94.043088, 32.955592),
    (-94.042964, 33.019219),
    (-94.041444, 33.019188),
    (-94.035839, 33.019145),
    (-94.027983, 33.019139),
    (-94.024475, 33.019207),
    (-93.814553, 33.019372),
    (-93.842597, 32.946764),
    (-93.785181, 32.857353),
    (-93.824253, 32.792451),
    (-93.783233, 32.784360),
    (-93.819169, 32.736002),
    (-93.782111, 32.712212),
    (-93.739474, 32.590773),
    (-93.767444, 32.538401),
    (-93.699506, 32.497480),
    (-93.661396, 32.427624),
    (-93.685569, 32.395498),
    (-93.659041, 32.406058),
    (-93.471249, 32.237186),
    (-93.614690, 32.237526),
    (-93.666472, 32.317444),
    (-93.791282, 32.340224),
    (-93.951085, 32.195545),
    (-94.042621, 32.196005),
    (-94.042662, 32.218146),
    (-94.042732, 32.269620),
    (-94.042733, 32.269696),
    (-94.042739, 32.363559),
    (-94.042763, 32.373332),
    (-94.042901, 32.392283),
    (-94.042923, 32.399918),
    (-94.042899, 32.400659),
    (-94.042986, 32.435507),
    (-94.042908, 32.439891),
    (-94.042903, 32.470386),
    (-94.042875, 32.471348),
    (-94.042902, 32.472906),
    (-94.042995, 32.478004),
    (-94.042955, 32.480261),
    (-94.043072, 32.484300),
    (-94.043089, 32.486561),
    (-94.042911, 32.492852),
    (-94.042885, 32.505145),
    (-94.043081, 32.513613),
    (-94.043142, 32.559502),
    (-94.043083, 32.564261),
    (-94.042919, 32.610142),
    (-94.042929, 32.618260),
    (-94.042926, 32.622015),
    (-94.042824, 32.640305),
    (-94.042780, 32.643466),
    (-94.042913, 32.655127),
    (-94.043147, 32.693030),
]

GENERIC_CROSS_TOKENS = {
    "DEAD END",
    "DEADEND",
    "EXIT",
    "EXIT INTERCHANGE ROADWAYS",
    "INTERCHANGE ROADWAYS",
    "UNKNOWN",
    "UNKNOWN NAME",
}

CAD_ROAD_DISAMBIGUATOR_TYPES = {
    "ALY", "ALLEY", "AV", "AVE", "AVENUE", "BLVD", "BOULEVARD",
    "CIR", "CIRCLE", "CT", "COURT", "DR", "DRIVE", "LN", "LANE",
    "LOOP", "PASSWAY", "PKWY", "PARKWAY", "PL", "PLACE", "RD", "ROAD",
    "ST", "STREET", "TER", "TERRACE", "TRL", "TRAIL", "WAY",
}

SOURCE_MUNICIPALITY_ALIASES = {
    "caddo": {
        "BLN": "Blanchard",
        "CADD": "",
        "GIL": "Gilliam",
        "GWD": "Greenwood",
        "HOS": "Hosston",
        "MPT": "Mooringsport",
        "SHV": "Shreveport",
        "VIV": "Vivian",
    },
}

# Source geocode profiles (bounds + fallback center).
SOURCE_GEO_PROFILES = {
    "caddo": {
        "lat_min": 32.10,
        "lat_max": 33.05,
        "lon_min": -94.10,
        "lon_max": -93.62,
        "center_lat": 32.5252,
        "center_lon": -93.7502,
        "default_city": "Shreveport",
        "county": "Caddo Parish",
        "area_sq_miles": 937.0,
        "polygon": CADDO_PARISH_RING_LON_LAT,
    },
    "lafayette": {
        "lat_min": 29.70,
        "lat_max": 30.55,
        "lon_min": -92.35,
        "lon_max": -91.70,
        "center_lat": 30.2241,
        "center_lon": -92.0198,
        "default_city": "Lafayette",
        "county": "Lafayette Parish",
        "area_sq_miles": 270.0,
    },
    "batonrouge": {
        "lat_min": 30.20,
        "lat_max": 30.75,
        "lon_min": -91.45,
        "lon_max": -90.95,
        "center_lat": 30.4515,
        "center_lon": -91.1871,
        "default_city": "Baton Rouge",
        "county": "East Baton Rouge Parish",
        "area_sq_miles": 470.0,
    },
    "neworleans": {
        "lat_min": 29.80,
        "lat_max": 30.20,
        "lon_min": -90.35,
        "lon_max": -89.60,
        "center_lat": 29.9511,
        "center_lon": -90.0715,
        "default_city": "New Orleans",
        "county": "Orleans Parish",
        "area_sq_miles": 350.0,
    },
}


def _normalize_source_name(source: str | None) -> str:
    s = (source or "caddo").strip().lower()
    return s if s in SOURCE_GEO_PROFILES else "caddo"


def _source_geo_profile(source: str | None) -> dict:
    return SOURCE_GEO_PROFILES[_normalize_source_name(source)]


def _point_in_ring(lon: float, lat: float, ring: list[tuple[float, float]]) -> bool:
    inside = False
    count = len(ring)
    if count < 3:
        return False

    for idx in range(count):
        x1, y1 = ring[idx]
        x2, y2 = ring[(idx + 1) % count]
        crosses_lat = (y1 > lat) != (y2 > lat)
        if not crosses_lat:
            continue
        x_at_lat = (x2 - x1) * (lat - y1) / ((y2 - y1) or 1e-12) + x1
        if lon < x_at_lat:
            inside = not inside
    return inside


def _is_in_source_bounds(lat: float, lon: float, source: str | None) -> bool:
    profile = _source_geo_profile(source)
    if not (
        profile["lat_min"] < lat < profile["lat_max"]
        and profile["lon_min"] < lon < profile["lon_max"]
    ):
        return False

    polygon = profile.get("polygon")
    if polygon:
        return _point_in_ring(lon, lat, polygon)
    return True


def _arcgis_in_source_scope(location, source: str | None) -> bool:
    """Validate a candidate using bounds plus ArcGIS parish metadata."""
    source_name = _normalize_source_name(source)
    profile = _source_geo_profile(source_name)
    try:
        lat = float(location.latitude)
        lon = float(location.longitude)
    except (TypeError, ValueError, AttributeError):
        return False

    if not (
        profile["lat_min"] < lat < profile["lat_max"]
        and profile["lon_min"] < lon < profile["lon_max"]
    ):
        return False

    if source_name != "caddo" or _is_in_source_bounds(lat, lon, source_name):
        return True

    # The detailed Census ring is intentionally strict around the river, but
    # ArcGIS can authoritatively identify a near-edge result as Caddo Parish.
    attrs = _arcgis_attributes(location)
    subregion = _clean_ws(attrs.get("Subregion") or "").lower()
    return subregion == "caddo parish"

def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        if not it:
            continue
        key = it.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out

def _clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _strip_cad_road_discriminator(value: str | None) -> str:
    """Remove CAD duplicate-road suffixes without touching route numbers."""
    text = _clean_ws(value or "")
    tokens = text.split()
    if (
        len(tokens) >= 3
        and tokens[-1].isdigit()
        and tokens[-2].upper() in CAD_ROAD_DISAMBIGUATOR_TYPES
    ):
        return " ".join(tokens[:-1])
    return text

def _normalize_location_token(value: str | None) -> str:
    return _clean_ws(value or "").lower()


def _is_generic_cross_token(value: str | None) -> bool:
    token = re.sub(r"[^A-Z0-9]+", " ", _clean_ws(value or "").upper()).strip()
    return token in GENERIC_CROSS_TOKENS


def _is_unknown_location(street: str | None, cross_streets: str | None) -> bool:
    street_norm = _normalize_location_token(street)
    cross_norm = _normalize_location_token(cross_streets)
    return (street_norm in ("", "unknown")) and (cross_norm in ("", "unknown"))

def _split_cross_tokens(text: str | None) -> list[str]:
    """
    Split a "cross streets" field into individual street tokens.
    Handles separators like '&', '/', ' and ', and '@'.
    """
    if not text:
        return []
    s = _clean_ws(text)
    if not s:
        return []

    # Normalize separators to '&'
    s = s.replace("/", " & ").replace("@", " & ")
    s = re.sub(r"\s+\band\b\s+", " & ", s, flags=re.IGNORECASE)
    s = _clean_ws(s)

    parts = [p.strip(" ,") for p in s.split("&")]
    out: list[str] = []
    for p in parts:
        p = _strip_cad_road_discriminator(p)
        if not p:
            continue
        if _is_generic_cross_token(p):
            continue
        out.append(p)
    return _dedupe_keep_order(out)

def _extract_street_and_crosses(street: str | None, cross_streets: str | None) -> tuple[str | None, list[str]]:
    """
    The feed sometimes puts multiple streets in the "street" field, e.g.:
      "E 70TH @ DIXIE GARDEN DR & E DIXIE MEADOW RD"
    In that case, treat the part before '@' as the main street, and fold the rest into crosses.
    """
    street_s = _strip_cad_road_discriminator(street)
    cross_s = _clean_ws(cross_streets or "")

    extra_cross = ""
    if street_s and "@" in street_s:
        main, extra = street_s.split("@", 1)
        street_s = _clean_ws(main)
        extra_cross = _clean_ws(extra)

    crosses = _split_cross_tokens(extra_cross) + _split_cross_tokens(cross_s)
    street_clean = street_s if street_s else None
    return street_clean, crosses

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters."""
    r = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _normalize_geocode_municipality(municipality: str | None, source: str | None) -> str:
    source_name = _normalize_source_name(source)
    city = _clean_ws(municipality or "")
    if not city:
        return ""

    aliases = SOURCE_MUNICIPALITY_ALIASES.get(source_name, {})
    mapped = aliases.get(city.upper())
    if mapped is not None:
        return mapped

    if re.fullmatch(r"[A-Z .'-]+", city) and len(city) > 4:
        return city.title()
    return city


def _locality_variants_for_geocoder(municipality: str | None, source: str | None) -> list[tuple[str, ...]]:
    profile = _source_geo_profile(source)
    city = _normalize_geocode_municipality(municipality, source)
    county = _clean_ws(profile.get("county") or "")
    state = "LA"
    fallback_city = _clean_ws(profile.get("default_city") or "")

    variants: list[tuple[str, ...]] = []

    def add_variant(*parts: str) -> None:
        cleaned = tuple(part for part in (_clean_ws(p) for p in parts) if part)
        if cleaned and cleaned not in variants:
            variants.append(cleaned)

    if city and county:
        add_variant(city, county, state)
    if city:
        add_variant(city, state)
    elif county:
        add_variant(county, state)

    if fallback_city and fallback_city != city:
        if county:
            add_variant(fallback_city, county, state)
        add_variant(fallback_city, state)

    if not variants:
        add_variant(fallback_city, state)

    return variants


def _road_signature(value: str | None) -> tuple[tuple[str, ...], frozenset[str]]:
    """Return comparable road-name words and explicit direction markers."""
    raw_tokens = re.findall(r"[A-Z0-9]+", _clean_ws(value or "").upper())
    name_tokens: list[str] = []
    directions: set[str] = set()

    for idx, token in enumerate(raw_tokens):
        # In names such as "St Vincent Ave", the leading ST means Saint, not
        # Street. Keeping that distinction avoids reducing the road to a single
        # overly-broad word.
        if token == "ST" and idx == 0 and len(raw_tokens) > 1:
            name_tokens.append("SAINT")
            continue

        direction = DIRECTION_ALIASES.get(token)
        if direction and (idx == 0 or idx == len(raw_tokens) - 1):
            directions.add(direction)
            continue

        if token in ROAD_TYPE_TOKENS:
            continue

        if token == "I" and idx + 1 < len(raw_tokens) and raw_tokens[idx + 1].isdigit():
            token = "INTERSTATE"
        name_tokens.append(token)

    return tuple(name_tokens), frozenset(directions)


def _road_name_matches(requested: str | None, returned: str | None) -> bool:
    """Conservatively compare a requested road with a provider road name."""
    requested_words, requested_dirs = _road_signature(requested)
    returned_words, returned_dirs = _road_signature(returned)
    if not requested_words or not returned_words:
        return False

    if requested_dirs and returned_dirs and requested_dirs.isdisjoint(returned_dirs):
        return False

    if requested_words == returned_words:
        return True

    requested_set = set(requested_words)
    returned_set = set(returned_words)
    overlap = len(requested_set & returned_set)
    union = len(requested_set | returned_set)
    if overlap >= 2 and union and overlap / union >= 0.8:
        return True

    requested_text = " ".join(requested_words)
    returned_text = " ".join(returned_words)
    return min(len(requested_text), len(returned_text)) >= 6 and SequenceMatcher(
        None, requested_text, returned_text
    ).ratio() >= 0.9


def _arcgis_attributes(location) -> dict:
    raw = getattr(location, "raw", None) or {}
    attrs = raw.get("attributes")
    return attrs if isinstance(attrs, dict) else {}


def _arcgis_score(location) -> float:
    raw = getattr(location, "raw", None) or {}
    attrs = _arcgis_attributes(location)
    value = attrs.get("Score", raw.get("score", 0))
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _arcgis_intersection_roads(location) -> list[str]:
    attrs = _arcgis_attributes(location)
    roads: list[str] = []
    for suffix in ("1", "2"):
        parts = [
            attrs.get(f"StPreDir{suffix}"),
            attrs.get(f"StPreType{suffix}"),
            attrs.get(f"StName{suffix}"),
            attrs.get(f"StType{suffix}"),
            attrs.get(f"StDir{suffix}"),
        ]
        road = _clean_ws(" ".join(str(part) for part in parts if part))
        if road:
            roads.append(road)

    if len(roads) >= 2:
        return roads[:2]

    match_address = _clean_ws(
        attrs.get("Match_addr")
        or getattr(location, "address", "")
        or (getattr(location, "raw", None) or {}).get("address", "")
    )
    street_part = match_address.split(",", 1)[0]
    split_roads = [
        _clean_ws(part)
        for part in re.split(r"\s+(?:&|AND|AT)\s+", street_part, flags=re.IGNORECASE)
        if _clean_ws(part)
    ]
    return split_roads[:2]


def _arcgis_single_road(location) -> str:
    attrs = _arcgis_attributes(location)
    road = _clean_ws(" ".join(str(part) for part in (
        attrs.get("StPreDir"),
        attrs.get("StPreType"),
        attrs.get("StName"),
        attrs.get("StType"),
        attrs.get("StDir"),
    ) if part))
    if road:
        return road

    match_address = _clean_ws(
        attrs.get("Match_addr")
        or getattr(location, "address", "")
        or (getattr(location, "raw", None) or {}).get("address", "")
    )
    return match_address.split(",", 1)[0]


def _arcgis_matches_intersection(location, road_a: str, road_b: str) -> bool:
    attrs = _arcgis_attributes(location)
    address_type = _clean_ws(attrs.get("Addr_type") or "").lower()
    if address_type not in {"streetint", "intersection"}:
        return False
    if _arcgis_score(location) < ARCGIS_INTERSECTION_MIN_SCORE:
        return False

    returned_roads = _arcgis_intersection_roads(location)
    if len(returned_roads) < 2:
        return False
    first, second = returned_roads[:2]
    return (
        _road_name_matches(road_a, first) and _road_name_matches(road_b, second)
    ) or (
        _road_name_matches(road_a, second) and _road_name_matches(road_b, first)
    )


def _arcgis_matches_single_road(location, requested_road: str) -> bool:
    attrs = _arcgis_attributes(location)
    address_type = _clean_ws(attrs.get("Addr_type") or "").lower()
    if address_type and address_type not in {
        "streetname", "streetaddress", "pointaddress", "subaddress",
    }:
        return False
    if _arcgis_score(location) < ARCGIS_STREET_MIN_SCORE:
        return False
    return _road_name_matches(requested_road, _arcgis_single_road(location))


def _split_numbered_address(value: str | None) -> tuple[str, str] | None:
    match = re.match(
        r"^\s*(\d+[A-Z]?(?:-\d+)?)\s+(.+?)\s*$",
        _clean_ws(value or ""),
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1).upper(), match.group(2)


def _split_numbered_address_range(value: str | None) -> tuple[str, str, str] | None:
    """Split a CAD block range such as ``6601 - 6799 KLEINPETER RD``."""
    match = re.match(
        r"^\s*(\d+[A-Z]?)\s*-\s*(\d+[A-Z]?)\s+(.+?)\s*$",
        _clean_ws(value or ""),
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1).upper(), match.group(2).upper(), _clean_ws(match.group(3))


BATON_ROUGE_CROSS_STREET_ALIASES = {
    # EBR CAD uses this corridor label for the Drusilla/Jefferson end of the
    # I-12 segment. Drusilla Ln is the road that actually crosses I-12.
    "DRUSILLA-JEFFERSON": "DRUSILLA LN",
}


def _normalize_baton_rouge_geocode_parts(
    street: str | None,
    crosses: list[str],
) -> tuple[str | None, list[str]]:
    """Remove EBR CAD route references and expand known corridor labels."""
    street_clean = _clean_ws(street or "")
    if street_clean:
        street_clean = re.sub(
            r"^\d+\s+(?=(?:[NSEW]\s+)?(?:INTERSTATE\s+\d+|I\s*-?\s*\d+)\b)",
            "",
            street_clean,
            flags=re.IGNORECASE,
        )

    normalized_crosses = [
        BATON_ROUGE_CROSS_STREET_ALIASES.get(_clean_ws(cross).upper(), cross)
        for cross in crosses
    ]
    return street_clean or None, normalized_crosses


def _arcgis_matches_numbered_address(location, requested_address: str) -> bool:
    requested = _split_numbered_address(requested_address)
    if not requested:
        return False

    attrs = _arcgis_attributes(location)
    address_type = _clean_ws(attrs.get("Addr_type") or "").lower()
    if address_type not in {"pointaddress", "streetaddress", "subaddress"}:
        return False
    if _arcgis_score(location) < ARCGIS_ADDRESS_MIN_SCORE:
        return False

    requested_number, requested_road = requested
    matched_number = _clean_ws(attrs.get("AddNum") or "").upper()
    if not matched_number:
        match_address = _clean_ws(
            attrs.get("Match_addr")
            or getattr(location, "address", "")
            or (getattr(location, "raw", None) or {}).get("address", "")
        )
        matched = _split_numbered_address(match_address.split(",", 1)[0])
        matched_number = matched[0] if matched else ""

    matched_road = _arcgis_single_road(location)
    matched_address_parts = _split_numbered_address(matched_road)
    if matched_address_parts:
        matched_road = matched_address_parts[1]

    return matched_number == requested_number and _road_name_matches(requested_road, matched_road)


def _as_location_list(value) -> list:
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _find_arcgis_intersection(
    road_a: str,
    road_b: str,
    locality_variants: list[tuple[str, ...]],
    source_name: str,
    attempt_state: dict | None = None,
) -> dict | None:
    cache_locality = locality_variants[0] if locality_variants else ()
    road_key = tuple(sorted((_normalize_location_token(road_a), _normalize_location_token(road_b))))
    cache_key = (source_name, road_key, cache_locality)
    cached = geocode_intersection_cache.get(cache_key)
    if cached:
        return dict(cached)

    for locality_parts in locality_variants:
        query = ", ".join((f"{road_a} & {road_b}", *locality_parts))
        try:
            locations = _as_location_list(geolocator_arcgis.geocode(
                query,
                exactly_one=False,
                timeout=3,
                out_fields="*",
            ))
            if attempt_state is not None:
                attempt_state["provider_responded"] = True
        except Exception:
            continue

        matches = []
        for location in locations[:10]:
            try:
                if not _arcgis_in_source_scope(location, source_name):
                    continue
                if not _arcgis_matches_intersection(location, road_a, road_b):
                    continue
                matches.append(location)
            except (TypeError, ValueError, AttributeError):
                continue

        if matches:
            location = max(matches, key=_arcgis_score)
            result = {
                "lat": float(location.latitude),
                "lng": float(location.longitude),
                "source": "arcgis",
                "query": query,
                "score": _arcgis_score(location),
            }
            geocode_intersection_cache[cache_key] = dict(result)
            return result
    return None


def _find_arcgis_street(
    road: str,
    quality: str,
    locality_variants: list[tuple[str, ...]],
    source_name: str,
    attempt_state: dict | None = None,
) -> dict | None:
    for locality_parts in locality_variants:
        query = ", ".join((road, *locality_parts))
        try:
            locations = _as_location_list(geolocator_arcgis.geocode(
                query,
                exactly_one=False,
                timeout=3,
                out_fields="*",
            ))
            if attempt_state is not None:
                attempt_state["provider_responded"] = True
        except Exception:
            continue

        matches = []
        for location in locations[:10]:
            try:
                if not _arcgis_in_source_scope(location, source_name):
                    continue
                if not _arcgis_matches_single_road(location, road):
                    continue
                matches.append(location)
            except (TypeError, ValueError, AttributeError):
                continue

        if matches:
            location = max(matches, key=_arcgis_score)
            return {
                "lat": float(location.latitude),
                "lng": float(location.longitude),
                "source": "arcgis",
                "quality": quality,
                "query": query,
            }
    return None


def _find_arcgis_address(
    address: str,
    locality_variants: list[tuple[str, ...]],
    source_name: str,
    attempt_state: dict | None = None,
) -> dict | None:
    for locality_parts in locality_variants:
        query = ", ".join((address, *locality_parts))
        try:
            locations = _as_location_list(geolocator_arcgis.geocode(
                query,
                exactly_one=False,
                timeout=3,
                out_fields="*",
            ))
            if attempt_state is not None:
                attempt_state["provider_responded"] = True
        except Exception:
            continue

        matches = []
        for location in locations[:10]:
            try:
                if not _arcgis_in_source_scope(location, source_name):
                    continue
                if not _arcgis_matches_numbered_address(location, address):
                    continue
                matches.append(location)
            except (TypeError, ValueError, AttributeError):
                continue

        if matches:
            location = max(matches, key=_arcgis_score)
            return {
                "lat": float(location.latitude),
                "lng": float(location.longitude),
                "source": "arcgis",
                "quality": "address",
                "query": query,
            }
    return None


def _osm_matches_road(location, requested_road: str) -> bool:
    raw = getattr(location, "raw", None) or {}
    address = raw.get("address") if isinstance(raw.get("address"), dict) else {}
    returned_road = _clean_ws(
        address.get("road")
        or address.get("pedestrian")
        or address.get("residential")
        or (getattr(location, "address", "") or "").split(",", 1)[0]
    )
    return _road_name_matches(requested_road, returned_road)

def geocode_address(street, cross_streets, municipality, source: str = 'caddo'):
    """
    Convert address to lat/lng coordinates.
    Validate exact intersections and understand two-cross-street segments.

    In the Caddo feed, a value such as:
      E BERT KOUNS INDUSTRIAL @ JUMP RUN & YOUREE DR
    means a location on E Bert Kouns bracketed by those two roads. It does not
    mean that Jump Run intersects Youree. When both named-street intersections
    validate, use their midpoint as the best estimate of the segment.
    """
    source_name = _normalize_source_name(source)
    geo_profile = _source_geo_profile(source_name)

    # Skip geocoding entirely if the location is missing/unknown.
    if _is_unknown_location(street, cross_streets):
        log("  [--] skipped | location is empty/unknown")
        return {
            'lat': None,
            'lng': None,
            'source': 'skipped',
            'quality': 'unknown-location',
            'query': None,
        }
    
    locality_variants = _locality_variants_for_geocoder(municipality, source_name)
    cache_locality = locality_variants[0][0] if locality_variants else geo_profile['default_city']

    street_clean, crosses = _extract_street_and_crosses(street, cross_streets)
    address_range = _split_numbered_address_range(street_clean)
    if source_name == 'batonrouge' and not address_range:
        street_clean, crosses = _normalize_baton_rouge_geocode_parts(
            street_clean,
            crosses,
        )
    cross1 = crosses[0] if len(crosses) > 0 else None
    cross2 = crosses[1] if len(crosses) > 1 else None

    # Cache key
    cache_key = f"{source_name}|{street_clean or ''}|{cross1 or ''}|{cross2 or ''}|{cache_locality}"
    if cache_key in geocode_cache:
        return geocode_cache[cache_key]

    attempt_state = {"provider_responded": False}

    # EBR commonly publishes a block/address range bracketed by two streets.
    # Validate both numbered endpoints and use their midpoint; if either end
    # fails, continue below using the named road and the published crosses.
    if source_name == 'batonrouge' and address_range:
        range_start, range_end, range_road = address_range
        start_address = f"{range_start} {range_road}"
        end_address = f"{range_end} {range_road}"
        start_match = _find_arcgis_address(
            start_address,
            locality_variants,
            source_name,
            attempt_state,
        )
        end_match = _find_arcgis_address(
            end_address,
            locality_variants,
            source_name,
            attempt_state,
        )
        if start_match and end_match:
            range_length = _haversine_m(
                start_match['lat'], start_match['lng'],
                end_match['lat'], end_match['lng'],
            )
            if range_length <= MAX_STREET_SEGMENT_METERS:
                result = {
                    'lat': (start_match['lat'] + end_match['lat']) / 2.0,
                    'lng': (start_match['lng'] + end_match['lng']) / 2.0,
                    'source': 'arcgis',
                    'quality': 'approximate-address-range',
                    'query': f"{start_match['query']} || {end_match['query']}",
                }
                geocode_cache[cache_key] = result
                log(
                    f"  [ARC] address-range ({range_length:.0f}m) | "
                    f"{start_address} - {range_end} -> "
                    f"({result['lat']:.5f}, {result['lng']:.5f})"
                )
                return result

        street_clean, crosses = _normalize_baton_rouge_geocode_parts(
            range_road,
            crosses,
        )
        cross1 = crosses[0] if len(crosses) > 0 else None
        cross2 = crosses[1] if len(crosses) > 1 else None

    # Lafayette and Baton Rouge commonly publish a numbered address plus a
    # nearby cross street. The address is more precise than forcing those
    # fields through intersection matching.
    if street_clean and not address_range and _split_numbered_address(street_clean):
        result = _find_arcgis_address(
            street_clean,
            locality_variants,
            source_name,
            attempt_state,
        )
        if result:
            geocode_cache[cache_key] = result
            log(f"  [ARC] address | {result['query']} -> ({result['lat']:.5f}, {result['lng']:.5f})")
            return result

    # First validate the named street against each bracketing cross street.
    # ArcGIS's Addr_type=StreetInt plus returned road components are required;
    # a high-score StreetName result is still only a fuzzy partial match.
    street_intersections: list[dict] = []
    if street_clean:
        for cross in (cross1, cross2):
            if not cross:
                continue
            match = _find_arcgis_intersection(
                street_clean,
                cross,
                locality_variants,
                source_name,
                attempt_state,
            )
            if match:
                street_intersections.append(match)

    if len(street_intersections) >= 2:
        first, second = street_intersections[:2]
        segment_length = _haversine_m(
            first['lat'], first['lng'], second['lat'], second['lng']
        )
        if segment_length <= MAX_STREET_SEGMENT_METERS:
            result = {
                'lat': (first['lat'] + second['lat']) / 2.0,
                'lng': (first['lng'] + second['lng']) / 2.0,
                'source': 'arcgis',
                'quality': 'street-segment',
                'query': f"{first['query']} || {second['query']}",
            }
            geocode_cache[cache_key] = result
            log(
                f"  [ARC] street-segment ({segment_length:.0f}m) | "
                f"{street_clean} @ {cross1} & {cross2} -> "
                f"({result['lat']:.5f}, {result['lng']:.5f})"
            )
            return result

    # One verified named-street intersection is safer than a fuzzy match of the
    # two cross streets to each other.
    if street_intersections:
        result = dict(street_intersections[0])
        result['quality'] = 'street+cross'
        result.pop('score', None)
        geocode_cache[cache_key] = result
        log(f"  [ARC] street+cross | {result['query']} -> ({result['lat']:.5f}, {result['lng']:.5f})")
        return result

    # If the street field is empty or is a place/subdivision label, the two
    # cross streets may themselves be the real intersection.
    if cross1 and cross2:
        match = _find_arcgis_intersection(
            cross1,
            cross2,
            locality_variants,
            source_name,
            attempt_state,
        )
        if match:
            result = dict(match)
            result['quality'] = 'intersection-2'
            result.pop('score', None)
            geocode_cache[cache_key] = result
            log(f"  [ARC] intersection-2 | {result['query']} -> ({result['lat']:.5f}, {result['lng']:.5f})")
            return result

    # Only use a lower-confidence road centroid when the feed supplied no
    # intersection context at all. If cross streets were supplied but could
    # not be validated, leaving the point unresolved is safer than discarding
    # the most specific location evidence.
    fallback_roads: list[tuple[str, str]] = []
    if street_clean and not cross1:
        fallback_roads.append((street_clean, 'street-only'))
    elif cross1 and not street_clean and not cross2:
        fallback_roads.append((cross1, 'cross-only'))

    for road, quality in fallback_roads:
        result = _find_arcgis_street(
            road, quality, locality_variants, source_name, attempt_state
        )
        if result:
            geocode_cache[cache_key] = result
            log(f"  [ARC] {quality} | {result['query']} -> ({result['lat']:.5f}, {result['lng']:.5f})")
            return result

    for road, quality in fallback_roads:
        for locality_parts in locality_variants:
            query = ", ".join((road, *locality_parts))
            try:
                location = geolocator_osm.geocode(
                    query,
                    country_codes='us',
                    exactly_one=True,
                    addressdetails=True,
                    timeout=3,
                )
                attempt_state["provider_responded"] = True
                if not location:
                    continue
                if not _is_in_source_bounds(location.latitude, location.longitude, source_name):
                    continue
                if not _osm_matches_road(location, road):
                    continue
                result = {
                    'lat': float(location.latitude),
                    'lng': float(location.longitude),
                    'source': 'osm',
                    'quality': quality,
                    'query': query,
                }
                geocode_cache[cache_key] = result
                log(f"  [OSM] {quality} | {query} -> ({result['lat']:.5f}, {result['lng']:.5f})")
                return result
            except Exception:
                continue

    # Never fabricate a point near the city center. An unresolved incident can
    # remain visible in the list without putting a confidently wrong marker on
    # the map.
    unresolved = {
        'lat': None,
        'lng': None,
        'source': 'unresolved',
        'quality': 'unresolved',
        'query': None,
        'provider_responded': attempt_state['provider_responded'],
    }
    if attempt_state['provider_responded']:
        geocode_cache[cache_key] = unresolved
    log(f"  [--] unresolved | {street_clean or '?'} @ {cross1 or '?'} {'& ' + cross2 if cross2 else ''}")
    return unresolved

def hash_incident(incident):
    """Generate unique hash for incident deduplication"""
    source = _normalize_source_name(incident.get('source') if isinstance(incident, dict) else None)
    source_id = _clean_ws(incident.get('source_id') if isinstance(incident, dict) else '')
    if source_id:
        key = f"{source}-{source_id}"
    else:
        key = (
            f"{source}-{incident['agency']}-{incident['time']}-"
            f"{incident['description']}-{incident['street']}-{incident['cross_streets']}"
        )
    return hashlib.md5(key.encode()).hexdigest()

def scrape_caddo_incidents():
    """
    Scrape active incidents from Caddo 911 website.
    """
    try:
        return caddo_source.scrape(user_agent=SCRAPER_USER_AGENT, timeout_seconds=15)
    except Exception as e:
        log(f"Caddo scraping error: {e}")
        import traceback
        traceback.print_exc()
        return [], None


def scrape_lafayette_incidents():
    """Scrape active incidents from Lafayette traffic feed."""
    try:
        return lafayette_source.scrape(user_agent=SCRAPER_USER_AGENT, timeout_seconds=15)
    except Exception as e:
        log(f"Lafayette scraping error: {e}")
        import traceback
        traceback.print_exc()
        return [], None


def scrape_batonrouge_incidents():
    """Scrape active incidents from Baton Rouge traffic feed."""
    try:
        return batonrouge_source.scrape(user_agent=SCRAPER_USER_AGENT, timeout_seconds=15)
    except Exception as e:
        log(f"Baton Rouge scraping error: {e}")
        import traceback
        traceback.print_exc()
        return [], None


def scrape_neworleans_incidents():
    """Import the official delayed NOPD calls-for-service log."""
    try:
        return neworleans_source.scrape(user_agent=SCRAPER_USER_AGENT, timeout_seconds=30)
    except Exception as e:
        log(f"New Orleans scraping error: {e}")
        import traceback
        traceback.print_exc()
        return [], None


# Backwards compatibility for any old call sites.
def scrape_incidents():
    return scrape_caddo_incidents()

# Track last update time
last_update: str | None = None
scrape_interval_seconds: int = SCRAPE_INTERVAL_SECONDS_DEFAULT
source_last_scrape_monotonic: dict[str, float] = {}
SOURCE_MIN_SCRAPE_INTERVAL_SECONDS = {
    # Data.NOLA publishes this dataset in daily batches. Polling it every live
    # feed cycle would create load without making the site any fresher.
    'neworleans': 15 * 60,
}


NOLA_APPROX_LOCATION_RE = re.compile(
    r"^\s*approx(?:imate)?\s+loc(?:ation)?\s*:\s*",
    flags=re.IGNORECASE,
)
NOLA_UNUSABLE_PUBLIC_LOCATIONS = {
    "",
    "UNKNOWN",
    "REDACTED",
    "REDACTED BLOCK",
    "CONFIDENTIAL",
    "WITHHELD",
}


def _expand_public_block_number(value: str) -> str:
    """Turn a public block mask such as 035XX into a block anchor (3500)."""
    text = _clean_ws(value)
    match = re.match(r"^(\d+)(X{2,})(?=\s)", text, flags=re.IGNORECASE)
    if not match:
        return text
    block_number = int(match.group(1)) * (10 ** len(match.group(2)))
    expanded = f"{block_number}{text[match.end():]}"
    return re.sub(r"^(\d+)\s+BLK\s+", r"\1 ", expanded, flags=re.IGNORECASE)


def _normalize_new_orleans_geocode_text(value: str) -> str:
    """Normalize known public-feed notation without changing its display."""
    text = _clean_ws(value)
    text = re.sub(r"\bCHEF\s+MENTUER\b", "Chef Menteur", text, flags=re.IGNORECASE)
    text = re.sub(r"\bUS\s*(\d+)B\b", r"US \1 BUS", text, flags=re.IGNORECASE)
    text = re.sub(r"\bEARHART\s+ONRAMP\b", "Earhart Blvd", text, flags=re.IGNORECASE)
    return text


def _new_orleans_public_location_parts(incident: dict) -> tuple[str | None, str | None, bool]:
    """Prepare only NOPD's published label for approximate geocoding."""
    public_label = _clean_ws(incident.get('street') or '')
    explicitly_approximate = bool(incident.get('location_is_approximate'))
    prefix_match = NOLA_APPROX_LOCATION_RE.match(public_label)
    if prefix_match:
        explicitly_approximate = True
        public_label = public_label[prefix_match.end():].strip()

    if public_label.upper() in NOLA_UNUSABLE_PUBLIC_LOCATIONS:
        return None, None, explicitly_approximate

    public_label = _normalize_new_orleans_geocode_text(public_label)
    public_cross = _clean_ws(incident.get('cross_streets') or '')
    if not public_cross:
        # NOPD commonly stores intersections entirely in block_address.
        parts = re.split(
            r"\s*(?:&|/|@)\s*|\s+\b(?:AND|AT|AFTER|BEFORE|NEAR)\b\s+",
            public_label,
            maxsplit=1,
            flags=re.IGNORECASE,
        )
        if len(parts) == 2 and all(_clean_ws(part) for part in parts):
            public_label, public_cross = (_clean_ws(part) for part in parts)

    # NOPD publishes block masks rather than exact premises. A block anchor is
    # used only to obtain an approximate map point; the stored/displayed label
    # remains the original 035XX-style public value.
    public_label = _expand_public_block_number(public_label)
    public_cross = _normalize_new_orleans_geocode_text(public_cross)
    return public_label or None, public_cross or None, explicitly_approximate


def _incident_geocode_result(incident: dict, source_name: str) -> dict:
    raw_lat = incident.get('latitude')
    raw_lon = incident.get('longitude')
    try:
        latitude = float(raw_lat)
        longitude = float(raw_lon)
    except (TypeError, ValueError):
        latitude = None
        longitude = None

    if (
        latitude is not None
        and longitude is not None
        and math.isfinite(latitude)
        and math.isfinite(longitude)
        and _is_in_source_bounds(latitude, longitude, source_name)
    ):
        is_approximate = bool(
            incident.get('location_is_approximate')
            or (
                source_name == 'neworleans'
                and NOLA_APPROX_LOCATION_RE.match(_clean_ws(incident.get('street') or ''))
            )
        )
        return {
            'lat': latitude,
            'lng': longitude,
            'source': 'source-feed',
            'quality': 'approximate-published' if is_approximate else 'published',
            'query': (
                'Official source coordinates; source labels location approximate'
                if is_approximate
                else 'Official source coordinates'
            ),
            'provider_responded': True,
        }

    if source_name == 'neworleans':
        public_street, public_cross, _ = _new_orleans_public_location_parts(incident)
        if not public_street and not public_cross:
            return {
                'lat': None,
                'lng': None,
                'source': 'source-feed-unmapped',
                'quality': 'location-unavailable',
                'query': None,
                'provider_responded': True,
            }

        result = geocode_address(
            public_street,
            public_cross,
            incident.get('municipality'),
            source=source_name,
        )
        if result.get('lat') is not None and result.get('lng') is not None:
            result = dict(result)
            base_quality = result.get('quality') or 'location'
            result['quality'] = f"approximate-{base_quality}"
            # Do not expose the internal block-anchor query as a more specific
            # address. The user-facing location stays exactly as NOPD supplied.
            result['query'] = 'Approximate from public NOPD block/intersection label'
        return result

    result = geocode_address(
        incident.get('street'),
        incident.get('cross_streets'),
        incident.get('municipality'),
        source=source_name,
    )
    if (
        incident.get('location_is_approximate')
        and result.get('lat') is not None
        and result.get('lng') is not None
    ):
        result = dict(result)
        base_quality = str(result.get('quality') or 'location')
        if not base_quality.startswith('approximate-'):
            result['quality'] = f"approximate-{base_quality}"
    return result

def _new_orleans_processing_priority(incident: dict) -> tuple[int, int]:
    """Put visible NOLA rows with official points ahead of slower fallbacks."""
    active_rank = 0 if incident.get('is_active', True) else 1
    try:
        has_official_point = _is_in_source_bounds(
            float(incident.get('latitude')),
            float(incident.get('longitude')),
            'neworleans',
        )
    except (TypeError, ValueError):
        has_official_point = False
    return active_rank, 0 if has_official_point else 1


def process_incidents(incidents, *, source: str = 'caddo', deactivate_missing: bool = True):
    """Store/update incidents in database"""
    global last_update

    if not incidents:
        return

    source_default = _normalize_source_name(source)
    conn = db_connect()
    cursor = conn.cursor()
    has_source_column = _ensure_incidents_source_column(conn)
    now = datetime.now(timezone.utc).isoformat()
    current_hashes = set()
    ordered_incidents = list(incidents)
    if source_default == 'neworleans':
        # The source publishes many rows at once. Make the current official
        # points queryable immediately, before slower 0,0 location fallbacks.
        ordered_incidents.sort(key=_new_orleans_processing_priority)

    for incident_index, incident in enumerate(ordered_incidents, start=1):
        if source_default == 'neworleans' and incident_index > 1 and (incident_index - 1) % 25 == 0:
            conn.commit()
        incident_source = _normalize_source_name(incident.get('source') if isinstance(incident, dict) else source_default)
        incident['source'] = incident_source
        h = hash_incident(incident)
        current_hashes.add(h)
        desired_active = 1 if incident.get('is_active', True) else 0
        occurred_at = incident.get('occurred_at')
        if not isinstance(occurred_at, str) or not _parse_iso_datetime(occurred_at):
            occurred_at = None

        # Check if exists
        try:
            cursor.execute(
                'SELECT id, latitude, longitude, geocode_source, geocode_quality, geocode_version, street FROM incidents WHERE hash = ?',
                (h,)
            )
            existing = cursor.fetchone()
            existing_cols = "versioned"
        except sqlite3.OperationalError:
            try:
                cursor.execute(
                    'SELECT id, latitude, longitude, geocode_source, geocode_quality, street FROM incidents WHERE hash = ?',
                    (h,)
                )
                existing = cursor.fetchone()
                existing_cols = "new"
            except sqlite3.OperationalError:
                cursor.execute('SELECT id, latitude, longitude FROM incidents WHERE hash = ?', (h,))
                existing = cursor.fetchone()
                existing_cols = "old"
        
        if existing:
            # Unit assignments can change while the incident remains active.
            # Refresh mutable feed fields instead of freezing their first value.
            try:
                cursor.execute(
                    '''UPDATE incidents
                       SET last_seen = ?, is_active = ?, source = ?, agency = ?, time = ?, units = ?,
                           description = ?, street = ?, cross_streets = ?, municipality = ?,
                           first_seen = COALESCE(?, first_seen)
                       WHERE hash = ?''',
                    (
                        now,
                        desired_active,
                        incident_source,
                        incident.get('agency'),
                        incident.get('time'),
                        incident.get('units'),
                        incident.get('description'),
                        incident.get('street'),
                        incident.get('cross_streets'),
                        incident.get('municipality'),
                        occurred_at,
                        h,
                    ),
                )
            except sqlite3.OperationalError:
                cursor.execute(
                    'UPDATE incidents SET last_seen = ?, is_active = ?, units = ? WHERE hash = ?',
                    (now, desired_active, incident.get('units'), h),
                )

            if incident_source == 'neworleans':
                # Re-apply NOPD's current point state on every import. If the
                # official point is 0,0, fall back to the public block or
                # intersection label and mark the result as approximate.
                existing_lat = existing[1]
                existing_lng = existing[2]
                existing_quality = existing[4] if existing_cols in ("new", "versioned") and len(existing) > 4 else None
                existing_version = existing[5] if existing_cols == "versioned" and len(existing) > 5 else None
                existing_street = (
                    existing[6]
                    if existing_cols == "versioned" and len(existing) > 6
                    else existing[5]
                    if existing_cols == "new" and len(existing) > 5
                    else None
                )
                raw_lat = incident.get('latitude')
                raw_lng = incident.get('longitude')
                try:
                    has_official_point = _is_in_source_bounds(
                        float(raw_lat), float(raw_lng), incident_source
                    )
                except (TypeError, ValueError):
                    has_official_point = False

                public_street, public_cross, _ = _new_orleans_public_location_parts(incident)
                has_public_location = bool(public_street or public_cross)
                reusable_approximation = (
                    not has_official_point
                    and has_public_location
                    and existing_lat is not None
                    and existing_lng is not None
                    and str(existing_quality or '').startswith('approximate-')
                    and existing_version == GEOCODER_VERSION
                    and _clean_ws(existing_street or '') == _clean_ws(incident.get('street') or '')
                )
                if reusable_approximation:
                    continue

                current_geo = _incident_geocode_result(incident, incident_source)
                has_current_coords = (
                    current_geo.get('lat') is not None
                    and current_geo.get('lng') is not None
                )
                provider_confirmed = has_current_coords or bool(current_geo.get('provider_responded'))
                if provider_confirmed:
                    cursor.execute(
                        '''UPDATE incidents
                           SET latitude = ?, longitude = ?, geocode_source = ?,
                               geocode_quality = ?, geocode_query = ?, geocoded_at = ?,
                               geocode_version = ?
                           WHERE hash = ?''',
                        (
                            current_geo.get('lat'),
                            current_geo.get('lng'),
                            current_geo.get('source'),
                            current_geo.get('quality'),
                            current_geo.get('query'),
                            now,
                            GEOCODER_VERSION,
                            h,
                        ),
                    )
                continue

            # Some live adapters can publish their own authoritative map point
            # after the text row first appears. Re-apply that source point even
            # when a same-version geocoder fallback already exists.
            raw_lat = incident.get('latitude')
            raw_lng = incident.get('longitude')
            try:
                has_source_point = _is_in_source_bounds(
                    float(raw_lat), float(raw_lng), incident_source
                )
            except (TypeError, ValueError):
                has_source_point = False
            if has_source_point:
                current_geo = _incident_geocode_result(incident, incident_source)
                cursor.execute(
                    '''UPDATE incidents
                       SET latitude = ?, longitude = ?, geocode_source = ?,
                           geocode_quality = ?, geocode_query = ?, geocoded_at = ?,
                           geocode_version = ?
                       WHERE hash = ?''',
                    (
                        current_geo.get('lat'),
                        current_geo.get('lng'),
                        current_geo.get('source'),
                        current_geo.get('quality'),
                        current_geo.get('query'),
                        now,
                        GEOCODER_VERSION,
                        h,
                    ),
                )
                continue

            # Opportunistic re-geocode: if we previously fell back (or have no coords),
            # try again using the improved intersection logic. This keeps your DB, but improves
            # "bad" points over time.
            try:
                existing_lat = existing[1]
                existing_lng = existing[2]
                existing_source = existing[3] if existing_cols in ("new", "versioned") and len(existing) > 3 else None
                existing_quality = existing[4] if existing_cols in ("new", "versioned") and len(existing) > 4 else None
                existing_version = existing[5] if existing_cols == "versioned" and len(existing) > 5 else None

                needs_geo = (existing_lat is None or existing_lng is None)
                low_quality = (existing_source in (None, "fallback", "skipped", "unresolved")) or (existing_quality in (None, "fallback", "city-only", "cross-only", "unknown-location", "unresolved"))
                stale_version = existing_version != GEOCODER_VERSION
                if (needs_geo or low_quality or stale_version) and (incident.get('street') or incident.get('cross_streets')):
                    geo = _incident_geocode_result(incident, incident_source)
                    if geo:
                        new_has_coords = geo.get('lat') is not None and geo.get('lng') is not None
                        provider_confirmed = new_has_coords or bool(geo.get('provider_responded'))
                        should_update = provider_confirmed and (
                            stale_version or (needs_geo and new_has_coords)
                        )
                        if not needs_geo and new_has_coords:
                            try:
                                dist_m = _haversine_m(float(existing_lat), float(existing_lng), float(geo['lat']), float(geo['lng']))
                                # Only overwrite if materially different (avoid churning minor provider jitter)
                                if dist_m > 75:
                                    should_update = True
                            except Exception:
                                # If distance calc fails, be conservative and avoid overwriting
                                should_update = stale_version

                        if should_update:
                            try:
                                cursor.execute(
                                    'UPDATE incidents SET latitude = ?, longitude = ?, geocode_source = ?, geocode_quality = ?, geocode_query = ?, geocoded_at = ?, geocode_version = ? WHERE hash = ?',
                                    (
                                        geo['lat'],
                                        geo['lng'],
                                        geo.get('source'),
                                        geo.get('quality'),
                                        geo.get('query'),
                                        now,
                                        GEOCODER_VERSION,
                                        h,
                                    )
                                )
                                log(f"Re-geocoded: {incident['description']} -> {geo.get('source')} {geo.get('quality')}")
                            except sqlite3.OperationalError:
                                # Older schema without geocode columns: still update lat/lng if missing
                                cursor.execute(
                                    'UPDATE incidents SET latitude = ?, longitude = ? WHERE hash = ?',
                                    (geo['lat'], geo['lng'], h)
                                )
            except Exception:
                pass
        else:
            # New incident - geocode and insert
            geo = _incident_geocode_result(incident, incident_source)
            first_seen = occurred_at or now
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO incidents 
                    (hash, agency, time, units, description, street, cross_streets, municipality, source, is_active,
                     latitude, longitude, first_seen, last_seen,
                     geocode_source, geocode_quality, geocode_query, geocoded_at, geocode_version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    h,
                    incident['agency'],
                    incident['time'],
                    incident['units'],
                    incident['description'],
                    incident['street'],
                    incident['cross_streets'],
                    incident['municipality'],
                    incident_source,
                    desired_active,
                    geo['lat'],
                    geo['lng'],
                    first_seen,
                    now,
                    geo.get('source'),
                    geo.get('quality'),
                    geo.get('query'),
                    now,
                    GEOCODER_VERSION,
                ))
            except sqlite3.OperationalError:
                # Older schema: try insert without geocode metadata columns.
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO incidents 
                        (hash, agency, time, units, description, street, cross_streets, municipality, source, is_active, latitude, longitude, first_seen, last_seen)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        h,
                        incident['agency'],
                        incident['time'],
                        incident['units'],
                        incident['description'],
                        incident['street'],
                        incident['cross_streets'],
                        incident['municipality'],
                        incident_source,
                        desired_active,
                        geo['lat'],
                        geo['lng'],
                        first_seen,
                        now
                    ))
                except sqlite3.OperationalError:
                    cursor.execute('''
                        INSERT OR IGNORE INTO incidents 
                        (hash, agency, time, units, description, street, cross_streets, municipality, latitude, longitude, first_seen, last_seen, is_active)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        h,
                        incident['agency'],
                        incident['time'],
                        incident['units'],
                        incident['description'],
                        incident['street'],
                        incident['cross_streets'],
                        incident['municipality'],
                        geo['lat'],
                        geo['lng'],
                        first_seen,
                        now,
                        desired_active,
                    ))
            if incident_source != 'neworleans':
                log(f"New incident: {incident['description']} at {incident['street'] or incident['cross_streets']}")

    if deactivate_missing:
        # Mark incidents no longer in feed as inactive (source-scoped).
        if not has_source_column:
            if source_default != 'caddo':
                active_hashes = []
            else:
                cursor.execute('SELECT hash FROM incidents WHERE is_active = 1')
                active_hashes = [row[0] for row in cursor.fetchall()]
        elif source_default == 'caddo':
            cursor.execute("SELECT hash FROM incidents WHERE is_active = 1 AND (source = 'caddo' OR source IS NULL OR TRIM(source) = '')")
            active_hashes = [row[0] for row in cursor.fetchall()]
        else:
            cursor.execute('SELECT hash FROM incidents WHERE is_active = 1 AND source = ?', (source_default,))
            active_hashes = [row[0] for row in cursor.fetchall()]

        for h in active_hashes:
            if h not in current_hashes:
                cursor.execute('UPDATE incidents SET is_active = 0 WHERE hash = ?', (h,))

    conn.commit()
    conn.close()
    last_update = datetime.now(timezone.utc).isoformat()
    meta_set('last_update', last_update)


def _store_feed_refresh(source: str, refreshed_at_text: str | None) -> None:
    global feed_refreshed_at
    if not refreshed_at_text:
        return
    source_name = _normalize_source_name(source)
    feed_refreshed_by_source[source_name] = refreshed_at_text
    meta_set(f'feed_refreshed_at_{source_name}', refreshed_at_text)
    if source_name == 'caddo':
        # Keep old status field for backwards compatibility with frontend clients.
        feed_refreshed_at = refreshed_at_text
        meta_set('feed_refreshed_at', refreshed_at_text)


def background_scrape():
    """Background task to scrape incidents periodically"""
    global feed_refreshed_at, last_scrape_started_at, last_scrape_finished_at

    last_scrape_started_at = datetime.now(timezone.utc).isoformat()
    meta_set('last_scrape_started_at', last_scrape_started_at)

    source_jobs = [
        ('caddo', 'Caddo 911', scrape_caddo_incidents),
        ('batonrouge', 'Baton Rouge Traffic', scrape_batonrouge_incidents),
        ('lafayette', 'Lafayette 911', scrape_lafayette_incidents),
        ('neworleans', 'New Orleans daily calls for service', scrape_neworleans_incidents),
    ]
    for source_name, label, scraper in source_jobs:
        min_interval = SOURCE_MIN_SCRAPE_INTERVAL_SECONDS.get(source_name, 0)
        previous_run = source_last_scrape_monotonic.get(source_name)
        monotonic_now = time.monotonic()
        if previous_run is not None and monotonic_now - previous_run < min_interval:
            continue
        source_last_scrape_monotonic[source_name] = monotonic_now
        log(f"[{datetime.now().strftime('%H:%M:%S')}] Scraping {label}...")
        incidents, refreshed_at_text = scraper()
        _store_feed_refresh(source_name, refreshed_at_text)
        if incidents:
            process_incidents(incidents, source=source_name)
            active_count = sum(1 for incident in incidents if incident.get('is_active', True))
            log(
                f"[{datetime.now().strftime('%H:%M:%S')}] Processed {len(incidents)} "
                f"incidents ({active_count} latest) from {source_name}"
            )
        else:
            log(f"[{datetime.now().strftime('%H:%M:%S')}] No incidents found or scraping failed for {source_name}")

    last_scrape_finished_at = datetime.now(timezone.utc).isoformat()
    meta_set('last_scrape_finished_at', last_scrape_finished_at)


VALID_SOURCES = {'caddo', 'lafayette', 'batonrouge', 'neworleans'}


def _normalize_source_filter(value: str | None) -> str:
    s = (value or 'all').strip().lower()
    if s == 'all':
        return 'all'
    return s if s in VALID_SOURCES else 'all'


def _normalize_incident_source_for_read(source: str | None) -> str:
    return _normalize_source_name(source or 'caddo')


def _incidents_table_has_source_column(cursor: sqlite3.Cursor) -> bool:
    try:
        cursor.execute("PRAGMA table_info(incidents)")
        rows = cursor.fetchall() or []
    except sqlite3.Error:
        return False
    for row in rows:
        col_name = row["name"] if isinstance(row, sqlite3.Row) else row[1]
        if str(col_name).strip().lower() == 'source':
            return True
    return False


def _ensure_incidents_source_column(conn: sqlite3.Connection) -> bool:
    cursor = conn.cursor()
    if _incidents_table_has_source_column(cursor):
        return True
    try:
        cursor.execute("ALTER TABLE incidents ADD COLUMN source TEXT DEFAULT 'caddo'")
        cursor.execute("UPDATE incidents SET source = 'caddo' WHERE source IS NULL OR TRIM(source) = ''")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_source ON incidents(source)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_active_first_seen ON incidents(source, is_active, first_seen DESC)")
        conn.commit()
        log("[DB] Added missing incidents.source column at runtime")
        return True
    except sqlite3.OperationalError as e:
        log(f"[DB] Could not add incidents.source column yet: {e}")
        return False


def _incident_matches_source_filter(incident: dict, source_filter: str) -> bool:
    if source_filter == 'all':
        return True
    return _normalize_incident_source_for_read(incident.get('source')) == source_filter


def _query_month_incidents_from_conn(
    conn: sqlite3.Connection,
    month: str,
    source_filter: str,
    *,
    ensure_source_column: bool = False,
) -> list[dict]:
    """Read incidents for a Central month from one SQLite database."""
    bounds = _central_month_bounds_utc(month)
    if not bounds:
        return []

    start_utc, end_utc = bounds
    cursor = conn.cursor()
    has_source_column = _ensure_incidents_source_column(conn) if ensure_source_column else _incidents_table_has_source_column(cursor)

    if source_filter == 'all':
        sql = 'SELECT * FROM incidents WHERE first_seen >= ? AND first_seen < ? ORDER BY first_seen DESC'
        args: tuple = (start_utc, end_utc)
    elif source_filter == 'caddo':
        if has_source_column:
            sql = (
                "SELECT * FROM incidents WHERE first_seen >= ? AND first_seen < ? "
                "AND (source = 'caddo' OR source IS NULL OR TRIM(source) = '') "
                "ORDER BY first_seen DESC"
            )
            args = (start_utc, end_utc)
        else:
            sql = 'SELECT * FROM incidents WHERE first_seen >= ? AND first_seen < ? ORDER BY first_seen DESC'
            args = (start_utc, end_utc)
    elif not has_source_column:
        return []
    else:
        sql = 'SELECT * FROM incidents WHERE first_seen >= ? AND first_seen < ? AND source = ? ORDER BY first_seen DESC'
        args = (start_utc, end_utc, source_filter)

    try:
        cursor.execute(sql, args)
    except sqlite3.OperationalError:
        if source_filter not in ('all', 'caddo'):
            return []
        cursor.execute(
            'SELECT * FROM incidents WHERE first_seen >= ? AND first_seen < ? ORDER BY first_seen DESC',
            (start_utc, end_utc),
        )

    rows = [dict(row) for row in cursor.fetchall()]
    for row in rows:
        row['source'] = _normalize_incident_source_for_read(row.get('source'))
    return rows


def _load_month_incidents(month: str, source_filter: str) -> list[dict]:
    """Load incidents for a month from the main DB plus the month archive DB, if present."""
    incidents_by_hash: dict[str, dict] = {}

    conn = db_connect(row_factory=True)
    try:
        for row in _query_month_incidents_from_conn(conn, month, source_filter, ensure_source_column=True):
            key = str(row.get('hash') or f"main:{row.get('id')}")
            if key not in incidents_by_hash:
                incidents_by_hash[key] = row
    finally:
        conn.close()

    for archive_path in _get_archive_dbs_for_month(month):
        try:
            archive_conn = _archive_db_connect(archive_path, row_factory=True)
            try:
                for row in _query_month_incidents_from_conn(archive_conn, month, source_filter):
                    key = str(row.get('hash') or f"{archive_path}:{row.get('id')}")
                    if key not in incidents_by_hash:
                        incidents_by_hash[key] = row
            finally:
                archive_conn.close()
        except Exception as e:
            log(f"[ARCHIVE] Error reading {archive_path}: {e}")

    return list(incidents_by_hash.values())


def _incident_location_label(incident: dict) -> str | None:
    street = _clean_ws(incident.get('street') or '')
    cross = _clean_ws(incident.get('cross_streets') or '')
    municipality = _clean_ws(incident.get('municipality') or '')

    if street and cross:
        return f"{street} @ {cross}"
    if street:
        return street
    if cross:
        return cross
    if municipality:
        return municipality
    return None


def _incident_coordinates(incident: dict) -> tuple[float, float] | None:
    try:
        lat = float(incident.get('latitude'))
        lon = float(incident.get('longitude'))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(lat) or not math.isfinite(lon):
        return None
    return lat, lon


def _top_counts(values: list[str], *, limit: int = 5, label_key: str = 'label') -> list[dict]:
    counts: dict[str, int] = {}
    for value in values:
        clean = _clean_ws(value)
        if not clean:
            continue
        counts[clean] = counts.get(clean, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [{label_key: label, 'count': count} for label, count in ranked[:limit]]


DEFAULT_MONTHLY_REPORT_EXCLUDED_TYPES = {
    'TAKEN BY OTHER AGENCY',
    'CITIZEN ASSISTANCE',
    'CADDO EMS EVENT',
}


def _normalize_report_excluded_descriptions(raw_values: list[str] | None = None) -> set[str]:
    values = raw_values if raw_values is not None else list(DEFAULT_MONTHLY_REPORT_EXCLUDED_TYPES)
    normalized: set[str] = set()
    for value in values:
        clean = _clean_ws(value or '')
        if clean:
            normalized.add(clean.upper())
    return normalized


def _query_report_rows_from_conn(
    conn: sqlite3.Connection,
    source_filter: str,
    *,
    ensure_source_column: bool = False,
) -> list[dict]:
    """Read lightweight rows needed for report availability checks."""
    cursor = conn.cursor()
    has_source_column = _ensure_incidents_source_column(conn) if ensure_source_column else _incidents_table_has_source_column(cursor)

    if source_filter == 'all':
        sql = 'SELECT first_seen, description, source FROM incidents' if has_source_column else 'SELECT first_seen, description, NULL as source FROM incidents'
        args: tuple = ()
    elif source_filter == 'caddo':
        if has_source_column:
            sql = (
                "SELECT first_seen, description, source FROM incidents "
                "WHERE source = 'caddo' OR source IS NULL OR TRIM(source) = ''"
            )
            args = ()
        else:
            sql = 'SELECT first_seen, description, NULL as source FROM incidents'
            args = ()
    elif not has_source_column:
        return []
    else:
        sql = 'SELECT first_seen, description, source FROM incidents WHERE source = ?'
        args = (source_filter,)

    try:
        cursor.execute(sql, args)
    except sqlite3.OperationalError:
        if source_filter not in ('all', 'caddo'):
            return []
        cursor.execute('SELECT first_seen, description, NULL as source FROM incidents')

    rows = [dict(row) for row in cursor.fetchall()]
    for row in rows:
        row['source'] = _normalize_incident_source_for_read(row.get('source'))
    return rows


def _available_report_months(
    source_filter: str,
    *,
    excluded_descriptions: set[str] | None = None,
) -> list[str]:
    excluded = excluded_descriptions if excluded_descriptions is not None else _normalize_report_excluded_descriptions()
    cache_key = (source_filter, tuple(sorted(excluded)))
    now = time.monotonic()
    cached = _available_report_months_cache.get(cache_key)
    if cached and now - cached[0] <= REPORT_MAP_PERIOD_CACHE_TTL_SECONDS:
        return list(cached[1])

    months: set[str] = set()

    conn = db_connect(row_factory=True)
    try:
        rows = _query_report_rows_from_conn(conn, source_filter, ensure_source_column=True)
    finally:
        conn.close()

    for row in rows:
        description = _clean_ws(row.get('description') or '')
        if description.upper() in excluded:
            continue
        month_key = _central_month_key(row.get('first_seen'))
        if month_key:
            months.add(month_key)

    for archive_path in _list_archive_dbs():
        try:
            archive_conn = _archive_db_connect(archive_path, row_factory=True)
            try:
                rows = _query_report_rows_from_conn(archive_conn, source_filter)
            finally:
                archive_conn.close()
        except Exception as e:
            log(f"[ARCHIVE] Error reading {archive_path}: {e}")
            continue

        for row in rows:
            description = _clean_ws(row.get('description') or '')
            if description.upper() in excluded:
                continue
            month_key = _central_month_key(row.get('first_seen'))
            if month_key:
                months.add(month_key)

    result = sorted(months, reverse=True)
    _available_report_months_cache[cache_key] = (now, result)
    if len(_available_report_months_cache) > 8:
        oldest_key = min(_available_report_months_cache, key=lambda key: _available_report_months_cache[key][0])
        _available_report_months_cache.pop(oldest_key, None)
    return list(result)


def _is_past_month(month: str) -> bool:
    current_month = datetime.now(CENTRAL_TZ).strftime('%Y-%m')
    return month < current_month


def _monthly_report_cache_path(month: str, source_filter: str, radius_miles: float) -> str:
    safe_source = re.sub(r'[^a-z0-9_-]+', '_', (source_filter or 'all').lower()).strip('_') or 'all'
    safe_radius = re.sub(r'[^0-9a-z_-]+', '_', f"{float(radius_miles):g}mi")
    filename = f"monthly_report_v{REPORT_CACHE_VERSION}_{safe_source}_{month}_{safe_radius}.json"
    return os.path.join(_get_report_cache_dir(), filename)


def _load_cached_monthly_report(month: str, source_filter: str, radius_miles: float) -> dict | None:
    path = _monthly_report_cache_path(month, source_filter, radius_miles)
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
        if int(data.get('cacheVersion') or 0) != REPORT_CACHE_VERSION:
            return None
        if abs(float(data.get('radiusMiles') or 0) - float(radius_miles)) > 1e-9:
            return None
        return data
    except Exception:
        return None


def _save_cached_monthly_report(report: dict) -> None:
    month = str(report.get('month') or '')
    source_filter = str(report.get('source') or 'all')
    radius_miles = float(report.get('radiusMiles') or 0)
    if not month:
        return
    cache_dir = _get_report_cache_dir()
    os.makedirs(cache_dir, exist_ok=True)
    path = _monthly_report_cache_path(month, source_filter, radius_miles)
    payload = dict(report)
    payload['cacheVersion'] = REPORT_CACHE_VERSION
    payload['isStatic'] = True
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(payload, fh, ensure_ascii=True, separators=(',', ':'))


def _build_hotspot_summary(incidents: list[dict], radius_miles: float) -> dict | None:
    points: list[tuple[dict, float, float]] = []
    for incident in incidents:
        coords = _incident_coordinates(incident)
        if coords is None:
            continue
        lat, lon = coords
        points.append((incident, lat, lon))

    if not points:
        return None

    radius_m = max(float(radius_miles), 0.1) * 1609.344
    best_cluster: list[tuple[dict, float, float, float]] = []
    best_avg_distance: float | None = None

    for anchor_incident, anchor_lat, anchor_lon in points:
        cluster: list[tuple[dict, float, float, float]] = []
        total_distance = 0.0
        for incident, lat, lon in points:
            dist_m = _haversine_m(anchor_lat, anchor_lon, lat, lon)
            if dist_m <= radius_m:
                cluster.append((incident, lat, lon, dist_m))
                total_distance += dist_m

        if not cluster:
            continue

        avg_distance = total_distance / len(cluster)
        is_better = False
        if len(cluster) > len(best_cluster):
            is_better = True
        elif len(cluster) == len(best_cluster):
            if best_avg_distance is None or avg_distance < best_avg_distance:
                is_better = True

        if is_better:
            best_cluster = cluster
            best_avg_distance = avg_distance

    if not best_cluster:
        return None

    center_lat = sum(lat for _, lat, _, _ in best_cluster) / len(best_cluster)
    center_lon = sum(lon for _, _, lon, _ in best_cluster) / len(best_cluster)

    top_municipalities = _top_counts(
        [incident.get('municipality') or '' for incident, _, _, _ in best_cluster],
        label_key='name',
    )
    top_locations = _top_counts(
        [_incident_location_label(incident) or '' for incident, _, _, _ in best_cluster],
    )

    anchor_incident, anchor_lat, anchor_lon, _ = min(best_cluster, key=lambda item: item[3])

    return {
        'incidentCount': len(best_cluster),
        'radiusMiles': radius_miles,
        'center': {
            'latitude': round(center_lat, 6),
            'longitude': round(center_lon, 6),
        },
        'anchor': {
            'latitude': round(anchor_lat, 6),
            'longitude': round(anchor_lon, 6),
            'label': _incident_location_label(anchor_incident),
            'municipality': anchor_incident.get('municipality'),
        },
        'topMunicipalities': top_municipalities,
        'topLocations': top_locations,
    }


def _build_monthly_report(
    month: str,
    source_filter: str,
    radius_miles: float,
    *,
    excluded_descriptions: set[str] | None = None,
) -> dict:
    incidents = _load_month_incidents(month, source_filter)
    incidents.sort(key=lambda row: row.get('first_seen') or '')
    excluded = excluded_descriptions if excluded_descriptions is not None else _normalize_report_excluded_descriptions()

    included_incidents: list[dict] = []
    excluded_count = 0
    for incident in incidents:
        description = _clean_ws(incident.get('description') or '')
        if description.upper() in excluded:
            excluded_count += 1
            continue
        included_incidents.append(incident)

    by_type: dict[str, int] = {}
    for incident in included_incidents:
        description = _clean_ws(incident.get('description') or '')
        if not description:
            continue
        by_type[description] = by_type.get(description, 0) + 1

    top_types = sorted(by_type.items(), key=lambda item: (-item[1], item[0]))
    top_incident_type = None
    hotspot = None

    if top_types:
        top_description, top_count = top_types[0]
        top_incidents = [
            incident for incident in included_incidents
            if _clean_ws(incident.get('description') or '') == top_description
        ]
        hotspot = _build_hotspot_summary(top_incidents, radius_miles)
        top_incident_type = {
            'description': top_description,
            'count': top_count,
            'shareOfMonth': round(top_count / len(included_incidents), 4) if included_incidents else 0.0,
            'geocodedCount': sum(1 for incident in top_incidents if _incident_coordinates(incident) is not None),
            'hotspot': hotspot,
        }
        if hotspot:
            top_incident_type['hotspot']['shareOfType'] = round(hotspot['incidentCount'] / top_count, 4) if top_count else 0.0

    summary = None
    if top_incident_type:
        summary = (
            f"{top_incident_type['description']} was the most common incident type "
            f"with {top_incident_type['count']} incidents."
        )
        if hotspot:
            anchor_label = hotspot['anchor'].get('label') or hotspot['anchor'].get('municipality') or 'the mapped area'
            summary += (
                f" The densest {radius_miles:g}-mile hotspot had "
                f"{hotspot['incidentCount']} of them near {anchor_label}."
            )

    return {
        'month': month,
        'source': source_filter,
        'radiusMiles': radius_miles,
        'generatedAt': datetime.now(timezone.utc).isoformat(),
        'isStatic': False,
        'totalIncidents': len(included_incidents),
        'rawIncidentCount': len(incidents),
        'excludedIncidentCount': excluded_count,
        'excludedDescriptions': sorted(excluded),
        'topIncidentType': top_incident_type,
        'topTypes': [
            {
                'description': description,
                'count': count,
                'shareOfMonth': round(count / len(included_incidents), 4) if included_incidents else 0.0,
            }
            for description, count in top_types[:10]
        ],
        'summary': summary,
    }


REPORT_MAP_COLOR_ORDER = ['red', 'orange', 'blue', 'medical']
REPORT_MAP_ADDRESS_FORMAT_MESSAGE = 'Use format: 1234 Market St, Shreveport, LA 71101.'
REPORT_MAP_ADDRESS_RE = re.compile(
    r"^\s*\d{1,6}[A-Za-z]?\s+[A-Za-z0-9 .#'/-]{2,},?\s+[A-Za-z .'-]{2,},?\s+(?:LA|Louisiana)\s+\d{5}(?:-\d{4})?\s*$",
    re.IGNORECASE,
)
REPORT_MAP_EXACT_SCORE_LIMIT = int(_env_setting('LOUISIANA911_REPORT_EXACT_SCORE_LIMIT', 'CADDO911_REPORT_EXACT_SCORE_LIMIT', '12000'))
REPORT_MAP_PERIOD_CACHE_TTL_SECONDS = int(_env_setting('LOUISIANA911_REPORT_PERIOD_CACHE_TTL_SECONDS', 'CADDO911_REPORT_PERIOD_CACHE_TTL_SECONDS', '120'))
_report_geocode_cache: dict[tuple[str, str], dict | None] = {}
_report_period_cache: dict[tuple[str, str, tuple[str, ...]], tuple[float, dict]] = {}
_available_report_months_cache: dict[tuple[str, tuple[str, ...]], tuple[float, list[str]]] = {}

REPORT_MAP_COLOR_META = {
    'red': {
        'label': 'Red',
        'description': 'Violence, weapons, major fire, and urgent life-safety calls',
        'color': '#ff3b3b',
    },
    'orange': {
        'label': 'Orange',
        'description': 'Property crime, suspicious activity, hazards, and crashes',
        'color': '#ffb830',
    },
    'blue': {
        'label': 'Blue',
        'description': 'Lower-risk assistance, welfare, traffic, and service calls',
        'color': '#3b8bff',
    },
    'medical': {
        'label': 'Medical',
        'description': 'EMS and medical-emergency calls',
        'color': '#ff3b6b',
    },
}

REPORT_MAP_COLOR_TERMS = {
    'medical': [
        'medical emergency', 'caddo ems event', 'ems', 'unconscious', 'not breathing',
        'difficulty breathing', 'choking', 'overdose', 'cardiac', 'heart', 'stroke',
        'seizure', 'prisoner medical security',
    ],
    'red': [
        'shots fired', 'shooting', 'shot fired', 'gun', 'armed', 'weapon',
        'stabbing', 'stab', 'knife', 'assault', 'battery', 'domestic', 'fight',
        'robbery', 'home invasion', 'kidnap', 'hostage', 'homicide', 'murder',
        'rape', 'sexual', 'missing person', 'structure fire', 'house fire',
        'apartment fire', 'building fire', 'fire emergency', 'explosion',
        'major accident', 'injury accident', 'accident with injuries', 'fatal',
        'entrap', 'rollover',
    ],
    'orange': [
        'theft', 'burglary', 'stolen', 'shoplift', 'vandal', 'fraud',
        'accident', 'crash', 'wreck', 'collision', 'mvc', 'mva',
        'hit and run', 'hit run', 'traffic hazard', 'road hazard', 'debris',
        'disabled vehicle', 'gas leak', 'smoke', 'fire alarm', 'alarm',
        'loose livestock', 'livestock', 'loose animal', 'animal in roadway',
        'disturbance', 'dispute', 'disorderly', 'suspicious', 'prowler',
        'trespass', 'harassment', 'juvenile complaint',
    ],
    'blue': [
        'assist', 'assist motorist', 'deliver message', 'periodic check',
        'taken by other agency', 'follow up', 'followup', 'follow',
        'investigation', 'report', 'information', 'citizen assist',
        'citizen assistance', 'civil', 'welfare concern', 'welfare check',
        'wellness check', 'property check', 'extra patrol', 'directed patrol',
        'noise', 'complaint', 'parking', 'traffic control', 'traffic stop',
        'traffic violation', 'minor accident', 'minor traffic accident',
        'minor hit', 'lost property', 'found property', 'public service',
        'special event stand by', 'transport', 'caddo fire district',
    ],
}


def _normalize_report_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (value or "").lower())).strip()


def _report_text_includes_any(text: str, terms: list[str]) -> bool:
    return any(term in text for term in terms)


def _incident_report_color(incident: dict) -> str:
    """Map an incident onto the user-facing report colors."""
    description = _normalize_report_text(incident.get('description'))
    agency = str(incident.get('agency') or '').upper()

    if agency == 'EMS' or 'EMS' in agency or _report_text_includes_any(description, REPORT_MAP_COLOR_TERMS['medical']):
        return 'medical'
    if _report_text_includes_any(description, REPORT_MAP_COLOR_TERMS['red']):
        return 'red'
    if _report_text_includes_any(description, REPORT_MAP_COLOR_TERMS['blue']):
        return 'blue'
    if _report_text_includes_any(description, REPORT_MAP_COLOR_TERMS['orange']):
        return 'orange'
    return 'orange'


def _parse_report_colors(raw_value: str | None) -> list[str]:
    colors: list[str] = []
    for part in (raw_value or '').split(','):
        color = part.strip().lower()
        if color in REPORT_MAP_COLOR_META and color not in colors:
            colors.append(color)
    return colors


def _normalize_report_map_month(value: str | None) -> str | None:
    month = (value or 'all').strip().lower()
    if month in ('', 'all', '12mo', 'last12'):
        return 'all'
    if month in ('this_year', 'year', 'current_year'):
        return 'this_year'
    if len(month) == 7 and _central_month_bounds_utc(month) is not None:
        return month
    return None


def _report_period_months(month: str, source_filter: str, excluded_descriptions: set[str]) -> list[str]:
    available_months = _available_report_months(source_filter, excluded_descriptions=excluded_descriptions)
    if month == 'all':
        return available_months
    if month == 'this_year':
        year_prefix = f"{datetime.now(CENTRAL_TZ).year:04d}-"
        return [month_key for month_key in available_months if month_key.startswith(year_prefix)]
    return [month] if month in available_months else []


def _load_report_period_incidents(
    month: str,
    source_filter: str,
    *,
    excluded_descriptions: set[str] | None = None,
) -> list[dict]:
    excluded = excluded_descriptions or set()
    months = _report_period_months(month, source_filter, excluded)

    incidents_by_hash: dict[str, dict] = {}
    for month_key in months:
        for incident in _load_month_incidents(month_key, source_filter):
            description = _clean_ws(incident.get('description') or '')
            if description.upper() in excluded:
                continue
            key = str(incident.get('hash') or f"{month_key}:{incident.get('source')}:{incident.get('id')}")
            if key not in incidents_by_hash:
                incidents_by_hash[key] = incident

    incidents = list(incidents_by_hash.values())
    incidents.sort(key=lambda row: row.get('first_seen') or '')
    return incidents


def _load_report_period_dataset(
    month: str,
    source_filter: str,
    *,
    excluded_descriptions: set[str] | None = None,
) -> dict:
    excluded = excluded_descriptions or set()
    cache_key = (month, source_filter, tuple(sorted(excluded)))
    now = time.monotonic()
    cached = _report_period_cache.get(cache_key)
    if cached and now - cached[0] <= REPORT_MAP_PERIOD_CACHE_TTL_SECONDS:
        return cached[1]

    incidents = _load_report_period_incidents(
        month,
        source_filter,
        excluded_descriptions=excluded,
    )
    mappable = [
        decorated for incident in incidents
        if (decorated := _decorate_report_map_incident(incident)) is not None
    ]

    color_counts = {color: 0 for color in REPORT_MAP_COLOR_ORDER}
    for incident in mappable:
        color = incident.get('_reportColor') or _incident_report_color(incident)
        if color in color_counts:
            color_counts[color] += 1

    dataset = {
        'incidents': incidents,
        'mappable': mappable,
        'colorCounts': color_counts,
        'createdAt': datetime.now(timezone.utc).isoformat(),
    }
    _report_period_cache[cache_key] = (now, dataset)
    if len(_report_period_cache) > 18:
        oldest_key = min(_report_period_cache, key=lambda key: _report_period_cache[key][0])
        _report_period_cache.pop(oldest_key, None)
    return dataset


def _report_period_label(month: str) -> str:
    if month == 'all':
        return 'All Data'
    if month == 'this_year':
        return f"This Year ({datetime.now(CENTRAL_TZ).year})"
    try:
        year_s, month_s = month.split('-')
        dt = datetime(int(year_s), int(month_s), 1)
        return dt.strftime('%B %Y')
    except Exception:
        return month


def _validate_report_address(address: str) -> str | None:
    address = _clean_ws(address)
    if not address:
        return 'Address is required. ' + REPORT_MAP_ADDRESS_FORMAT_MESSAGE
    if len(address) > 140:
        return 'Address is too long. ' + REPORT_MAP_ADDRESS_FORMAT_MESSAGE
    if not REPORT_MAP_ADDRESS_RE.match(address):
        return 'Invalid address. ' + REPORT_MAP_ADDRESS_FORMAT_MESSAGE
    return None


def _source_area_sq_miles(source_filter: str) -> float:
    if source_filter == 'all':
        return sum(float(profile.get('area_sq_miles') or 0) for profile in SOURCE_GEO_PROFILES.values())
    profile = SOURCE_GEO_PROFILES.get(source_filter) or SOURCE_GEO_PROFILES['caddo']
    return float(profile.get('area_sq_miles') or SOURCE_GEO_PROFILES['caddo']['area_sq_miles'])


def _point_in_report_source_scope(lat: float, lon: float, source_filter: str) -> bool:
    if source_filter == 'all':
        return any(_is_in_source_bounds(lat, lon, source_name) for source_name in SOURCE_GEO_PROFILES)
    return _is_in_source_bounds(lat, lon, source_filter)


def _geocode_report_address(address: str, source_filter: str) -> dict | None:
    address = _clean_ws(address)
    if _validate_report_address(address):
        return None

    cache_key = (source_filter, address.lower())
    if cache_key in _report_geocode_cache:
        return _report_geocode_cache[cache_key]
    if len(_report_geocode_cache) > 512:
        _report_geocode_cache.pop(next(iter(_report_geocode_cache)), None)

    source_names = list(SOURCE_GEO_PROFILES.keys()) if source_filter == 'all' else [source_filter]
    queries: list[str] = []
    for source_name in source_names:
        profile = SOURCE_GEO_PROFILES.get(source_name) or SOURCE_GEO_PROFILES['caddo']
        default_city = _clean_ws(profile.get('default_city') or '')
        for query in (
            address,
            f"{address}, {default_city}, LA" if default_city else '',
        ):
            query = _clean_ws(query)
            if query and query.lower() not in {q.lower() for q in queries}:
                queries.append(query)

    best_out_of_scope: dict | None = None
    for query in queries[:4]:
        try:
            location = geolocator_arcgis.geocode(query, timeout=5)
        except Exception:
            location = None
        provider = 'arcgis'
        if not location:
            try:
                location = geolocator_osm.geocode(query, country_codes='us', exactly_one=True, timeout=5)
                provider = 'osm'
            except Exception:
                continue
        if not location:
            continue

        lat = float(location.latitude)
        lon = float(location.longitude)
        result = {
            'latitude': round(lat, 6),
            'longitude': round(lon, 6),
            'label': getattr(location, 'address', None) or query,
            'query': query,
            'geocodeSource': provider,
            'inSourceBounds': _point_in_report_source_scope(lat, lon, source_filter),
        }
        if result['inSourceBounds']:
            _report_geocode_cache[cache_key] = result
            return result
        if best_out_of_scope is None:
            best_out_of_scope = result

    _report_geocode_cache[cache_key] = best_out_of_scope
    return best_out_of_scope


def _decorate_report_map_incident(incident: dict) -> dict | None:
    coords = _incident_coordinates(incident)
    if coords is None:
        return None
    if _is_unknown_location(incident.get('street'), incident.get('cross_streets')):
        return None

    lat, lon = coords
    color = _incident_report_color(incident)
    row = dict(incident)
    row['_reportColor'] = color
    row['_reportColorLabel'] = REPORT_MAP_COLOR_META[color]['label']
    row['_lat'] = lat
    row['_lon'] = lon
    return row


def _report_incident_type_counts(incidents: list[dict], colors: list[str]) -> list[dict]:
    color_set = set(colors)
    counts: dict[str, dict] = {}
    for incident in incidents:
        color = incident.get('_reportColor') or _incident_report_color(incident)
        if color not in color_set:
            continue
        description = _clean_ws(incident.get('description') or '')
        if not description:
            continue
        bucket = counts.setdefault(description, {'description': description, 'count': 0, 'color': color})
        bucket['count'] += 1
    return sorted(counts.values(), key=lambda row: (-row['count'], row['description']))


def _incident_matches_report_filters(incident: dict, colors: list[str], incident_type: str | None) -> bool:
    if (incident.get('_reportColor') or _incident_report_color(incident)) not in set(colors):
        return False
    if incident_type:
        return _clean_ws(incident.get('description') or '').upper() == incident_type.upper()
    return True


def _distance_miles_to_incident(target_lat: float, target_lon: float, incident: dict) -> float:
    return _haversine_m(target_lat, target_lon, float(incident['_lat']), float(incident['_lon'])) / 1609.344


def _sample_anchor_points(incidents: list[dict], *, limit: int | None = None) -> tuple[list[dict], bool]:
    effective_limit = max(1, int(limit if limit is not None else REPORT_MAP_EXACT_SCORE_LIMIT))
    if len(incidents) <= effective_limit:
        return incidents, False
    if effective_limit <= 1:
        return incidents[:1], True
    # Extreme data sizes are geographically sampled. Normal selected-color
    # periods use every selected incident as a peer anchor.
    geo_sorted = sorted(
        incidents,
        key=lambda incident: (
            round(float(incident['_lat']), 3),
            round(float(incident['_lon']), 3),
            str(incident.get('first_seen') or ''),
        ),
    )
    step = (len(geo_sorted) - 1) / (effective_limit - 1)
    return [geo_sorted[int(round(idx * step))] for idx in range(effective_limit)], True


def _report_metric_empty(label: str, count: int, total: int, expected_by_area: float, ratio_to_area: float | None) -> dict:
    ratio_to_peer = None if count == 0 else float(count)
    percentile = None
    return {
        'label': label,
        'count': count,
        'totalInPeriod': total,
        'expectedByArea': round(expected_by_area, 2),
        'peerAverage': 0.0,
        'aboveAveragePoints': float(count),
        'ratioToArea': round(ratio_to_area, 2) if ratio_to_area is not None else None,
        'ratioToPeer': ratio_to_peer,
        'percentile': percentile,
        'peerSampleSize': 0,
        'peerPopulationSize': 0,
        'peerSampled': False,
        'score': _map_score_value(count, ratio_to_peer, percentile),
        'verdict': _map_score_label(count, ratio_to_peer, percentile),
    }


def _report_spatial_cell_size(radius_miles: float) -> float:
    # Roughly half the radius in latitude degrees, bounded so tiny radii still
    # keep cell counts low and larger custom radii do not create huge neighbor scans.
    return max(0.003, min(0.03, float(radius_miles) / 138.0))


def _build_report_spatial_index(incidents: list[dict], cell_size: float) -> dict[tuple[int, int], list[dict]]:
    index: dict[tuple[int, int], list[dict]] = {}
    for incident in incidents:
        cell = (
            math.floor(float(incident['_lat']) / cell_size),
            math.floor(float(incident['_lon']) / cell_size),
        )
        index.setdefault(cell, []).append(incident)
    return index


def _candidate_incidents_near(
    lat: float,
    lon: float,
    incidents: list[dict],
    radius_m: float,
    *,
    spatial_index: dict[tuple[int, int], list[dict]] | None = None,
    cell_size: float | None = None,
) -> list[dict]:
    if spatial_index is None or not cell_size:
        return incidents

    radius_miles = radius_m / 1609.344
    lat_range = max(1, math.ceil((radius_miles / 69.0) / cell_size) + 1)
    cos_lat = max(0.2, abs(math.cos(math.radians(lat))))
    lon_range = max(1, math.ceil((radius_miles / (69.0 * cos_lat)) / cell_size) + 1)
    base_lat_cell = math.floor(lat / cell_size)
    base_lon_cell = math.floor(lon / cell_size)
    nearby: list[dict] = []
    for lat_cell in range(base_lat_cell - lat_range, base_lat_cell + lat_range + 1):
        for lon_cell in range(base_lon_cell - lon_range, base_lon_cell + lon_range + 1):
            nearby.extend(spatial_index.get((lat_cell, lon_cell), []))
    return nearby


def _count_incidents_near(
    lat: float,
    lon: float,
    incidents: list[dict],
    radius_m: float,
    *,
    spatial_index: dict[tuple[int, int], list[dict]] | None = None,
    cell_size: float | None = None,
) -> int:
    candidates = _candidate_incidents_near(
        lat,
        lon,
        incidents,
        radius_m,
        spatial_index=spatial_index,
        cell_size=cell_size,
    )

    count = 0
    for incident in candidates:
        if _haversine_m(lat, lon, float(incident['_lat']), float(incident['_lon'])) <= radius_m:
            count += 1
    return count


def _map_score_label(count: int, ratio_to_peer: float | None, percentile: float | None) -> str:
    ratio = ratio_to_peer if ratio_to_peer is not None else 0.0
    pct = percentile if percentile is not None else 0.0
    if count <= 0:
        return 'No mapped cases'
    if ratio >= 2.0 or pct >= 0.9:
        return 'Well above average'
    if ratio >= 1.25 or pct >= 0.75:
        return 'Above average'
    if ratio <= 0.65 and pct <= 0.35:
        return 'Below average'
    return 'Near average'


def _map_score_value(count: int, ratio_to_peer: float | None, percentile: float | None) -> int:
    if count <= 0:
        return 0
    ratio = max(float(ratio_to_peer or 1.0), 0.05)
    pct = float(percentile if percentile is not None else 0.5)
    score = 50 + (22 * math.log(ratio, 2)) + (18 * (pct - 0.5))
    return max(0, min(100, int(round(score))))


def _build_report_map_metric(
    label: str,
    period_incidents: list[dict],
    target_incidents: list[dict],
    anchor_incidents: list[dict],
    *,
    radius_miles: float,
    source_area_sq_miles: float,
) -> dict:
    radius_m = radius_miles * 1609.344
    circle_area = math.pi * (radius_miles ** 2)
    total = len(period_incidents)
    count = len(target_incidents)
    expected_by_area = total * min(circle_area / max(source_area_sq_miles, 1.0), 1.0)
    ratio_to_area = (count / expected_by_area) if expected_by_area > 0 else None

    sampled_anchors, peer_sampled = _sample_anchor_points(anchor_incidents)
    if not sampled_anchors:
        return _report_metric_empty(label, count, total, expected_by_area, ratio_to_area)

    cell_size = _report_spatial_cell_size(radius_miles)
    spatial_index = _build_report_spatial_index(period_incidents, cell_size)
    peer_counts = [
        _count_incidents_near(
            float(anchor['_lat']),
            float(anchor['_lon']),
            period_incidents,
            radius_m,
            spatial_index=spatial_index,
            cell_size=cell_size,
        )
        for anchor in sampled_anchors
    ]
    peer_average = (sum(peer_counts) / len(peer_counts)) if peer_counts else 0.0
    ratio_to_peer = (count / peer_average) if peer_average > 0 else (None if count == 0 else count)
    percentile = None
    if peer_counts:
        percentile = sum(1 for peer_count in peer_counts if peer_count <= count) / len(peer_counts)

    score = _map_score_value(count, ratio_to_peer, percentile)
    verdict = _map_score_label(count, ratio_to_peer, percentile)
    above_average_points = count - peer_average

    return {
        'label': label,
        'count': count,
        'totalInPeriod': total,
        'expectedByArea': round(expected_by_area, 2),
        'peerAverage': round(peer_average, 2),
        'aboveAveragePoints': round(above_average_points, 2),
        'ratioToArea': round(ratio_to_area, 2) if ratio_to_area is not None else None,
        'ratioToPeer': round(ratio_to_peer, 2) if ratio_to_peer is not None else None,
        'percentile': round(percentile, 4) if percentile is not None else None,
        'peerSampleSize': len(sampled_anchors),
        'peerPopulationSize': len(anchor_incidents),
        'peerSampled': peer_sampled,
        'score': score,
        'verdict': verdict,
    }


def _serialize_map_report_incident(incident: dict) -> dict:
    return {
        'id': incident.get('id'),
        'description': incident.get('description'),
        'agency': incident.get('agency'),
        'time': incident.get('time'),
        'street': incident.get('street'),
        'crossStreets': incident.get('cross_streets'),
        'municipality': incident.get('municipality'),
        'source': incident.get('source'),
        'firstSeen': incident.get('first_seen'),
        'latitude': round(float(incident['_lat']), 6),
        'longitude': round(float(incident['_lon']), 6),
        'distanceMiles': round(float(incident.get('_distanceMiles') or 0), 2),
        'color': incident.get('_reportColor'),
        'colorLabel': incident.get('_reportColorLabel'),
        'locationLabel': _incident_location_label(incident),
    }


# API Routes
@app.route('/')
def index():
    return send_from_directory('public', 'index.html')

@app.route('/about/')
def about():
    return send_from_directory('public', 'about.html')

@app.route('/about')
def about_redirect():
    return redirect('/about/', code=301)


@app.route('/caddo911/')
def caddo911_landing():
    return send_from_directory('public', 'caddo911.html')


@app.route('/caddo911')
def caddo911_landing_redirect():
    return redirect('/caddo911/', code=301)


@app.route('/coverage/')
def coverage():
    return send_from_directory('public', 'coverage.html')


@app.route('/coverage')
def coverage_redirect():
    return redirect('/coverage/', code=301)


@app.route('/coverage/baton-rouge/')
def baton_rouge_coverage():
    return send_from_directory('public', 'coverage-baton-rouge.html')


@app.route('/coverage/baton-rouge')
def baton_rouge_coverage_redirect():
    return redirect('/coverage/baton-rouge/', code=301)


@app.route('/coverage/lafayette/')
def lafayette_coverage():
    return send_from_directory('public', 'coverage-lafayette.html')


@app.route('/coverage/lafayette')
def lafayette_coverage_redirect():
    return redirect('/coverage/lafayette/', code=301)


@app.route('/coverage/new-orleans/')
def new_orleans_coverage():
    return send_from_directory('public', 'coverage-new-orleans.html')


@app.route('/coverage/new-orleans')
def new_orleans_coverage_redirect():
    return redirect('/coverage/new-orleans/', code=301)


@app.route('/reports/')
def reports():
    return send_from_directory('public', 'reports.html')

@app.route('/reports')
def reports_redirect():
    return redirect('/reports/', code=301)


@app.route('/reports/monthly/')
def monthly_reports():
    return send_from_directory('public', 'monthly-reports.html')


@app.route('/reports/monthly')
def monthly_reports_redirect():
    return redirect('/reports/monthly/', code=301)

@app.route('/healthz')
def healthz():
    return jsonify({'ok': True})

@app.route('/api/incidents/active')
def get_active_incidents():
    source_filter = _normalize_source_filter(request.args.get('source'))
    conn = db_connect(row_factory=True)
    cursor = conn.cursor()
    has_source_column = _ensure_incidents_source_column(conn)
    try:
        if source_filter == 'all' or not has_source_column:
            cursor.execute('SELECT * FROM incidents WHERE is_active = 1 ORDER BY time DESC')
        elif source_filter == 'caddo':
            cursor.execute("SELECT * FROM incidents WHERE is_active = 1 AND (source = 'caddo' OR source IS NULL OR TRIM(source) = '') ORDER BY time DESC")
        else:
            cursor.execute('SELECT * FROM incidents WHERE is_active = 1 AND source = ? ORDER BY time DESC', (source_filter,))
        incidents = [dict(row) for row in cursor.fetchall()]
    except sqlite3.OperationalError:
        cursor.execute('SELECT * FROM incidents WHERE is_active = 1 ORDER BY time DESC')
        incidents = [dict(row) for row in cursor.fetchall()]
    for incident in incidents:
        incident['source'] = _normalize_incident_source_for_read(incident.get('source'))
    if source_filter != 'all':
        incidents = [incident for incident in incidents if _incident_matches_source_filter(incident, source_filter)]
    conn.close()
    return jsonify(incidents)

@app.route('/api/incidents/history')
def get_history():
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    date = request.args.get('date')  # YYYY-MM-DD format
    source_filter = _normalize_source_filter(request.args.get('source'))

    all_incidents: list[dict] = []
    total = 0

    # Query main database
    conn = db_connect(row_factory=True)
    cursor = conn.cursor()
    has_source_column = _ensure_incidents_source_column(conn)

    if date:
        bounds = _central_date_bounds_utc(date)
        if bounds:
            start_utc, end_utc = bounds
            if source_filter == 'all':
                cursor.execute(
                    'SELECT * FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ? ORDER BY first_seen DESC',
                    (start_utc, end_utc)
                )
            elif source_filter == 'caddo' and has_source_column:
                cursor.execute(
                    "SELECT * FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ? AND (source = 'caddo' OR source IS NULL OR TRIM(source) = '') ORDER BY first_seen DESC",
                    (start_utc, end_utc)
                )
            elif source_filter == 'caddo':
                cursor.execute(
                    'SELECT * FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ? ORDER BY first_seen DESC',
                    (start_utc, end_utc)
                )
            elif not has_source_column:
                cursor.execute('SELECT * FROM incidents WHERE 1 = 0')
            else:
                cursor.execute(
                    'SELECT * FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ? AND source = ? ORDER BY first_seen DESC',
                    (start_utc, end_utc, source_filter)
                )
            all_incidents.extend([dict(row) for row in cursor.fetchall()])
            if source_filter == 'all':
                cursor.execute(
                    'SELECT COUNT(*) as count FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ?',
                    (start_utc, end_utc)
                )
            elif source_filter == 'caddo' and has_source_column:
                cursor.execute(
                    "SELECT COUNT(*) as count FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ? AND (source = 'caddo' OR source IS NULL OR TRIM(source) = '')",
                    (start_utc, end_utc)
                )
            elif source_filter == 'caddo':
                cursor.execute(
                    'SELECT COUNT(*) as count FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ?',
                    (start_utc, end_utc)
                )
            elif not has_source_column:
                cursor.execute('SELECT 0 as count')
            else:
                cursor.execute(
                    'SELECT COUNT(*) as count FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ? AND source = ?',
                    (start_utc, end_utc, source_filter)
                )
            total += cursor.fetchone()['count']
    else:
        if source_filter == 'all':
            cursor.execute('SELECT * FROM incidents WHERE is_active = 0 ORDER BY first_seen DESC')
        elif source_filter == 'caddo' and has_source_column:
            cursor.execute("SELECT * FROM incidents WHERE is_active = 0 AND (source = 'caddo' OR source IS NULL OR TRIM(source) = '') ORDER BY first_seen DESC")
        elif source_filter == 'caddo':
            cursor.execute('SELECT * FROM incidents WHERE is_active = 0 ORDER BY first_seen DESC')
        elif not has_source_column:
            cursor.execute('SELECT * FROM incidents WHERE 1 = 0')
        else:
            cursor.execute('SELECT * FROM incidents WHERE is_active = 0 AND source = ? ORDER BY first_seen DESC', (source_filter,))
        all_incidents.extend([dict(row) for row in cursor.fetchall()])
        if source_filter == 'all':
            cursor.execute('SELECT COUNT(*) as count FROM incidents WHERE is_active = 0')
        elif source_filter == 'caddo' and has_source_column:
            cursor.execute("SELECT COUNT(*) as count FROM incidents WHERE is_active = 0 AND (source = 'caddo' OR source IS NULL OR TRIM(source) = '')")
        elif source_filter == 'caddo':
            cursor.execute('SELECT COUNT(*) as count FROM incidents WHERE is_active = 0')
        elif not has_source_column:
            cursor.execute('SELECT 0 as count')
        else:
            cursor.execute('SELECT COUNT(*) as count FROM incidents WHERE is_active = 0 AND source = ?', (source_filter,))
        total += cursor.fetchone()['count']
    conn.close()

    # Also query archive database(s) if date is specified and archive exists
    if date:
        archive_dbs = _get_archive_dbs_for_date(date)
        for archive_path in archive_dbs:
            try:
                archive_conn = _archive_db_connect(archive_path, row_factory=True)
                archive_cursor = archive_conn.cursor()
                bounds = _central_date_bounds_utc(date)
                if bounds:
                    start_utc, end_utc = bounds
                    if source_filter == 'all':
                        archive_cursor.execute(
                            'SELECT * FROM incidents WHERE first_seen >= ? AND first_seen < ? ORDER BY first_seen DESC',
                            (start_utc, end_utc)
                        )
                        rows = [dict(row) for row in archive_cursor.fetchall()]
                        archive_cursor.execute(
                            'SELECT COUNT(*) as count FROM incidents WHERE first_seen >= ? AND first_seen < ?',
                            (start_utc, end_utc)
                        )
                        count_row = archive_cursor.fetchone()
                        total += int(count_row['count']) if count_row else 0
                    else:
                        source_sql = "source = ?" if source_filter != 'caddo' else "(source = 'caddo' OR source IS NULL OR TRIM(source) = '')"
                        source_args = (source_filter,) if source_filter != 'caddo' else tuple()
                        try:
                            archive_cursor.execute(
                                f'SELECT * FROM incidents WHERE first_seen >= ? AND first_seen < ? AND {source_sql} ORDER BY first_seen DESC',
                                (start_utc, end_utc, *source_args)
                            )
                            rows = [dict(row) for row in archive_cursor.fetchall()]
                            archive_cursor.execute(
                                f'SELECT COUNT(*) as count FROM incidents WHERE first_seen >= ? AND first_seen < ? AND {source_sql}',
                                (start_utc, end_utc, *source_args)
                            )
                            count_row = archive_cursor.fetchone()
                            total += int(count_row['count']) if count_row else 0
                        except sqlite3.OperationalError:
                            # Legacy archives without source column are Caddo-only.
                            if source_filter != 'caddo':
                                rows = []
                            else:
                                archive_cursor.execute(
                                    'SELECT * FROM incidents WHERE first_seen >= ? AND first_seen < ? ORDER BY first_seen DESC',
                                    (start_utc, end_utc)
                                )
                                rows = [dict(row) for row in archive_cursor.fetchall()]
                                archive_cursor.execute(
                                    'SELECT COUNT(*) as count FROM incidents WHERE first_seen >= ? AND first_seen < ?',
                                    (start_utc, end_utc)
                                )
                                count_row = archive_cursor.fetchone()
                                total += int(count_row['count']) if count_row else 0
                    all_incidents.extend(rows)
                archive_conn.close()
            except Exception as e:
                log(f"[ARCHIVE] Error reading {archive_path}: {e}")

    for incident in all_incidents:
        incident['source'] = _normalize_incident_source_for_read(incident.get('source'))

    # Sort all incidents by first_seen descending, then apply pagination
    all_incidents.sort(key=lambda x: x.get('first_seen') or '', reverse=True)
    paginated = all_incidents[offset:offset + limit]

    return jsonify({'incidents': paginated, 'total': total})

@app.route('/api/incidents/history_counts')
def get_history_counts():
    month = request.args.get('month')  # YYYY-MM format
    source_filter = _normalize_source_filter(request.args.get('source'))

    if not month or len(month) != 7:
        return jsonify({'error': 'month is required (YYYY-MM)'}), 400

    counts: dict[str, int] = {}
    bounds = _central_month_bounds_utc(month)
    
    # Query main database
    conn = db_connect(row_factory=True)
    cursor = conn.cursor()
    has_source_column = _ensure_incidents_source_column(conn)
    if bounds:
        start_utc, end_utc = bounds
        if source_filter == 'all':
            cursor.execute(
                'SELECT first_seen FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ?',
                (start_utc, end_utc)
            )
        elif source_filter == 'caddo' and has_source_column:
            cursor.execute(
                "SELECT first_seen FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ? AND (source = 'caddo' OR source IS NULL OR TRIM(source) = '')",
                (start_utc, end_utc)
            )
        elif source_filter == 'caddo':
            cursor.execute(
                'SELECT first_seen FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ?',
                (start_utc, end_utc)
            )
        elif not has_source_column:
            cursor.execute('SELECT first_seen FROM incidents WHERE 1 = 0')
        else:
            cursor.execute(
                'SELECT first_seen FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ? AND source = ?',
                (start_utc, end_utc, source_filter)
            )
        for row in cursor.fetchall():
            day = _central_date_key(row['first_seen'])
            if day:
                counts[day] = counts.get(day, 0) + 1
    conn.close()
    
    # Also query archive database if it exists for this month
    archive_dbs = _get_archive_dbs_for_month(month)
    for archive_path in archive_dbs:
        try:
            archive_conn = _archive_db_connect(archive_path, row_factory=True)
            archive_cursor = archive_conn.cursor()
            if bounds:
                start_utc, end_utc = bounds
                try:
                    if source_filter == 'all':
                        archive_cursor.execute(
                            'SELECT first_seen FROM incidents WHERE first_seen >= ? AND first_seen < ?',
                            (start_utc, end_utc)
                        )
                    elif source_filter == 'caddo':
                        archive_cursor.execute(
                            "SELECT first_seen FROM incidents WHERE first_seen >= ? AND first_seen < ? AND (source = 'caddo' OR source IS NULL OR TRIM(source) = '')",
                            (start_utc, end_utc)
                        )
                    else:
                        archive_cursor.execute(
                            'SELECT first_seen FROM incidents WHERE first_seen >= ? AND first_seen < ? AND source = ?',
                            (start_utc, end_utc, source_filter)
                        )
                except sqlite3.OperationalError:
                    # Legacy archive DBs are Caddo-only and have no source column.
                    if source_filter != 'caddo':
                        archive_conn.close()
                        continue
                    archive_cursor.execute(
                        'SELECT first_seen FROM incidents WHERE first_seen >= ? AND first_seen < ?',
                        (start_utc, end_utc)
                    )
                for row in archive_cursor.fetchall():
                    day = _central_date_key(row['first_seen'])
                    if day:
                        counts[day] = counts.get(day, 0) + 1
            archive_conn.close()
        except Exception as e:
            log(f"[ARCHIVE] Error reading {archive_path}: {e}")

    return jsonify({'counts': counts})

@app.route('/api/reports/monthly')
def get_monthly_report():
    month = (request.args.get('month') or datetime.now(CENTRAL_TZ).strftime('%Y-%m')).strip()
    source_filter = _normalize_source_filter(request.args.get('source'))
    radius_miles = request.args.get('radius_miles', default=3.0, type=float)

    if len(month) != 7 or _central_month_bounds_utc(month) is None:
        return jsonify({'error': 'month must be in YYYY-MM format'}), 400

    if radius_miles is None or not math.isfinite(radius_miles) or radius_miles <= 0 or radius_miles > 50:
        return jsonify({'error': 'radius_miles must be a number between 0 and 50'}), 400

    excluded = _normalize_report_excluded_descriptions()
    available_months = _available_report_months(source_filter, excluded_descriptions=excluded)
    if month not in available_months:
        return jsonify({'error': 'no report data available for that month and source'}), 404

    if _is_past_month(month):
        cached = _load_cached_monthly_report(month, source_filter, radius_miles)
        if cached:
            return jsonify(cached)

    report = _build_monthly_report(
        month,
        source_filter,
        radius_miles,
        excluded_descriptions=excluded,
    )
    report['cacheVersion'] = REPORT_CACHE_VERSION
    if _is_past_month(month):
        _save_cached_monthly_report(report)
        report['isStatic'] = True

    return jsonify(report)


@app.route('/api/reports/available_months')
def get_available_report_months():
    source_filter = _normalize_source_filter(request.args.get('source'))
    excluded = _normalize_report_excluded_descriptions()
    months = _available_report_months(source_filter, excluded_descriptions=excluded)
    return jsonify({
        'source': source_filter,
        'months': months,
        'latest': months[0] if months else None,
        'excludedDescriptions': sorted(excluded),
    })


def get_map_report_options():
    source_filter = _normalize_source_filter(request.args.get('source'))
    month = _normalize_report_map_month(request.args.get('month'))
    if month is None:
        return jsonify({'error': 'month must be all, this_year, or YYYY-MM'}), 400

    colors = _parse_report_colors(request.args.get('colors'))
    if not colors:
        colors = list(REPORT_MAP_COLOR_ORDER)
    excluded = _normalize_report_excluded_descriptions()
    months = _available_report_months(source_filter, excluded_descriptions=excluded)
    dataset = _load_report_period_dataset(
        month,
        source_filter,
        excluded_descriptions=excluded,
    )
    incidents = dataset['incidents']
    decorated = dataset['mappable']
    color_counts = dataset['colorCounts']

    return jsonify({
        'source': source_filter,
        'month': month,
        'periodLabel': _report_period_label(month),
        'months': months,
        'latest': months[0] if months else None,
        'colors': [
            {
                'key': color,
                **REPORT_MAP_COLOR_META[color],
                'count': color_counts.get(color, 0),
            }
            for color in REPORT_MAP_COLOR_ORDER
        ],
        'incidentTypes': _report_incident_type_counts(decorated, colors)[:250],
        'totalIncidents': len(incidents),
        'mappableIncidents': len(decorated),
        'excludedDescriptions': sorted(excluded),
    })


def get_map_report():
    report_started_at = time.perf_counter()
    source_filter = _normalize_source_filter(request.args.get('source'))
    month = _normalize_report_map_month(request.args.get('month'))
    if month is None:
        return jsonify({'error': 'month must be all, this_year, or YYYY-MM'}), 400

    radius_miles = request.args.get('radius_miles', default=2.0, type=float)
    if radius_miles is None or not math.isfinite(radius_miles) or radius_miles < 0.25 or radius_miles > 25:
        return jsonify({'error': 'radius_miles must be a number between 0.25 and 25'}), 400

    colors = _parse_report_colors(request.args.get('colors'))
    if not colors:
        return jsonify({'error': 'Select at least one report color before running the map report.'}), 400
    incident_type_raw = _clean_ws(request.args.get('incident_type') or '')
    incident_type = incident_type_raw if incident_type_raw and incident_type_raw.lower() != 'all' else None

    target_lat = request.args.get('lat', type=float)
    target_lon = request.args.get('lng', type=float)
    address = _clean_ws(request.args.get('address') or '')
    target = None
    if target_lat is not None and target_lon is not None and math.isfinite(target_lat) and math.isfinite(target_lon):
        target = {
            'latitude': round(float(target_lat), 6),
            'longitude': round(float(target_lon), 6),
            'label': address or 'Selected point',
            'query': None,
            'geocodeSource': 'coordinates',
            'inSourceBounds': _point_in_report_source_scope(float(target_lat), float(target_lon), source_filter),
        }
    elif address:
        address_error = _validate_report_address(address)
        if address_error:
            return jsonify({'error': address_error}), 400
        target = _geocode_report_address(address, source_filter)
    else:
        return jsonify({'error': 'Address is required. ' + REPORT_MAP_ADDRESS_FORMAT_MESSAGE}), 400

    if not target:
        return jsonify({'error': 'Address could not be verified. ' + REPORT_MAP_ADDRESS_FORMAT_MESSAGE}), 404
    if not target.get('inSourceBounds'):
        return jsonify({'error': 'Address is outside the selected source area. Check Source or enter a local Louisiana address.'}), 400

    target_lat = float(target['latitude'])
    target_lon = float(target['longitude'])
    radius_m = radius_miles * 1609.344
    excluded = _normalize_report_excluded_descriptions()
    dataset = _load_report_period_dataset(
        month,
        source_filter,
        excluded_descriptions=excluded,
    )
    incidents = dataset['incidents']
    mappable = [dict(incident) for incident in dataset['mappable']]

    cell_size = _report_spatial_cell_size(radius_miles)
    spatial_index = _build_report_spatial_index(mappable, cell_size)
    radius_candidates = _candidate_incidents_near(
        target_lat,
        target_lon,
        mappable,
        radius_m,
        spatial_index=spatial_index,
        cell_size=cell_size,
    )
    for incident in radius_candidates:
        incident['_distanceMiles'] = _distance_miles_to_incident(target_lat, target_lon, incident)

    in_radius = [
        incident for incident in radius_candidates
        if float(incident.get('_distanceMiles') or 0) * 1609.344 <= radius_m
    ]
    selected_period = [
        incident for incident in mappable
        if _incident_matches_report_filters(incident, colors, incident_type)
    ]
    selected_in_radius = [
        incident for incident in in_radius
        if _incident_matches_report_filters(incident, colors, incident_type)
    ]

    source_area = _source_area_sq_miles(source_filter)
    overall_metric = _build_report_map_metric(
        incident_type or 'Selected colors',
        selected_period,
        selected_in_radius,
        selected_period,
        radius_miles=radius_miles,
        source_area_sq_miles=source_area,
    )

    color_metrics = []
    for color in REPORT_MAP_COLOR_ORDER:
        period_rows = [incident for incident in mappable if incident.get('_reportColor') == color]
        radius_rows = [incident for incident in in_radius if incident.get('_reportColor') == color]
        metric = _build_report_map_metric(
            REPORT_MAP_COLOR_META[color]['label'],
            period_rows,
            radius_rows,
            period_rows,
            radius_miles=radius_miles,
            source_area_sq_miles=source_area,
        )
        color_metrics.append({
            'key': color,
            **REPORT_MAP_COLOR_META[color],
            'enabled': color in set(colors),
            **metric,
        })

    top_types = _report_incident_type_counts(selected_in_radius, REPORT_MAP_COLOR_ORDER)[:12]
    selected_in_radius.sort(key=lambda incident: (float(incident.get('_distanceMiles') or 0), incident.get('first_seen') or ''))

    summary = (
        f"{overall_metric['count']} matching incident"
        f"{'' if overall_metric['count'] == 1 else 's'} found within {radius_miles:g} miles. "
        f"That is {overall_metric['verdict'].lower()} for {_report_period_label(month)}."
    )

    return jsonify({
        'address': address,
        'target': target,
        'source': source_filter,
        'month': month,
        'periodLabel': _report_period_label(month),
        'radiusMiles': radius_miles,
        'colors': colors,
        'incidentType': incident_type,
        'totalIncidents': len(incidents),
        'mappableIncidents': len(mappable),
        'incidentsInRadius': len(in_radius),
        'matchingIncidentsInRadius': len(selected_in_radius),
        'sourceAreaSqMiles': round(source_area, 2),
        'score': overall_metric,
        'summary': summary,
        'colorMetrics': color_metrics,
        'topIncidentTypes': top_types,
        'incidents': [_serialize_map_report_incident(incident) for incident in selected_in_radius[:200]],
        'processing': {
            'elapsedMs': int(round((time.perf_counter() - report_started_at) * 1000)),
            'periodCacheSeconds': REPORT_MAP_PERIOD_CACHE_TTL_SECONDS,
            'peerSampleLimit': REPORT_MAP_EXACT_SCORE_LIMIT,
            'radiusCandidateCount': len(radius_candidates),
            'returnedIncidentLimit': 200,
        },
        'excludedDescriptions': sorted(excluded),
    })

@app.route('/api/stats')
def get_stats():
    conn = db_connect(row_factory=True)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) as count FROM incidents WHERE is_active = 1')
    active = cursor.fetchone()['count']
    
    today = 0
    today_bounds = _central_date_bounds_utc(datetime.now(CENTRAL_TZ).date().isoformat())
    if today_bounds:
        start_utc, end_utc = today_bounds
        cursor.execute(
            "SELECT COUNT(*) as count FROM incidents WHERE first_seen >= ? AND first_seen < ?",
            (start_utc, end_utc)
        )
        today = cursor.fetchone()['count']
    
    cursor.execute('SELECT COUNT(*) as count FROM incidents')
    total = cursor.fetchone()['count']
    
    cursor.execute('SELECT agency, COUNT(*) as count FROM incidents WHERE is_active = 1 GROUP BY agency')
    by_agency = [dict(row) for row in cursor.fetchall()]
    
    cursor.execute('''
        SELECT description, COUNT(*) as count FROM incidents 
        WHERE is_active = 1 GROUP BY description ORDER BY count DESC LIMIT 10
    ''')
    by_type = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    return jsonify({
        'active': active,
        'today': today,
        'total': total,
        'byAgency': by_agency,
        'byType': by_type
    })

@app.route('/api/status')
def get_status():
    meta = meta_get_many([
        'last_update',
        'feed_refreshed_at',
        'feed_refreshed_at_caddo',
        'feed_refreshed_at_lafayette',
        'feed_refreshed_at_batonrouge',
        'feed_refreshed_at_neworleans',
        'last_scrape_started_at',
        'last_scrape_finished_at',
        'scrape_interval_seconds',
    ])

    interval_seconds = int(meta.get('scrape_interval_seconds') or scrape_interval_seconds)
    now_central = datetime.now(CENTRAL_TZ)

    last_update_dt = _parse_iso_datetime(meta.get('last_update') or last_update)
    last_scrape_finished_dt = _parse_iso_datetime(meta.get('last_scrape_finished_at') or last_scrape_finished_at)
    # Prefer scrape-finished time for "last update" display; otherwise fall back to last_update.
    display_base = last_scrape_finished_dt or last_update_dt

    refreshed_by_source = {
        'caddo': meta.get('feed_refreshed_at_caddo') or feed_refreshed_by_source.get('caddo'),
        'lafayette': meta.get('feed_refreshed_at_lafayette') or feed_refreshed_by_source.get('lafayette'),
        'batonrouge': meta.get('feed_refreshed_at_batonrouge') or feed_refreshed_by_source.get('batonrouge'),
        'neworleans': meta.get('feed_refreshed_at_neworleans') or feed_refreshed_by_source.get('neworleans'),
    }

    return jsonify({
        # lastUpdate: UTC ISO timestamp of when we last processed/saved a scrape
        'lastUpdate': meta.get('last_update') or last_update,
        # feedRefreshedAt: backwards-compatible Caddo source refresh text.
        'feedRefreshedAt': meta.get('feed_refreshed_at') or feed_refreshed_at,
        'feedRefreshedBySource': refreshed_by_source,
        'lastScrapeStartedAt': meta.get('last_scrape_started_at') or last_scrape_started_at,
        'lastScrapeFinishedAt': meta.get('last_scrape_finished_at') or last_scrape_finished_at,
        'serverNow': datetime.now(timezone.utc).isoformat(),
        # Central-time helpers for the UI (so clients always see Louisiana time, not browser locale)
        'centralTzAbbr': ("CST" if CENTRAL_TZ_IS_FALLBACK else now_central.strftime("%Z")),
        'centralDate': now_central.date().isoformat(),
        'lastUpdateDisplay': _format_central_hms(display_base),
        'lastUpdateTooltip': _format_central_tooltip(display_base),
        # milliseconds (frontend expects ms)
        'scrapeInterval': interval_seconds * 1000,
    })

@app.route('/api/refresh', methods=['POST'])
def force_refresh():
    try:
        # Disabled by default for safety; enable explicitly if you really need it.
        enabled = _env_setting('LOUISIANA911_ENABLE_REFRESH_ENDPOINT', 'CADDO911_ENABLE_REFRESH_ENDPOINT', '0').strip().lower() in ('1', 'true', 'yes')
        if not enabled:
            return jsonify({'error': 'refresh endpoint disabled'}), 403

        global feed_refreshed_at, last_scrape_started_at, last_scrape_finished_at
        last_scrape_started_at = datetime.now(timezone.utc).isoformat()
        meta_set('last_scrape_started_at', last_scrape_started_at)
        total_count = 0
        for source_name, scraper in (
            ('caddo', scrape_caddo_incidents),
            ('batonrouge', scrape_batonrouge_incidents),
            ('lafayette', scrape_lafayette_incidents),
        ):
            incidents, refreshed_at_text = scraper()
            _store_feed_refresh(source_name, refreshed_at_text)
            process_incidents(incidents, source=source_name)
            total_count += len(incidents)
        last_scrape_finished_at = datetime.now(timezone.utc).isoformat()
        meta_set('last_scrape_finished_at', last_scrape_finished_at)
        return jsonify({
            'success': True,
            'count': total_count,
            'feedRefreshedAt': feed_refreshed_at,
            'feedRefreshedBySource': {
                'caddo': feed_refreshed_by_source.get('caddo'),
                'lafayette': feed_refreshed_by_source.get('lafayette'),
                'batonrouge': feed_refreshed_by_source.get('batonrouge'),
            },
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def _background_archive():
    """Background task to archive old incidents (runs daily)."""
    try:
        log(f"[{datetime.now().strftime('%H:%M:%S')}] Running daily archive check...")
        result = archive_old_incidents(dry_run=False)
        if result['archived'] > 0:
            log(f"[{datetime.now().strftime('%H:%M:%S')}] Archived {result['archived']} incidents to {len(result['files'])} file(s)")
        else:
            log(f"[{datetime.now().strftime('%H:%M:%S')}] No incidents to archive")
    except Exception as e:
        log(f"[{datetime.now().strftime('%H:%M:%S')}] Archive error: {e}")

def _background_weekly_backup():
    """Background task to create weekly SQLite backup snapshots."""
    try:
        log(f"[{datetime.now().strftime('%H:%M:%S')}] Running weekly backup snapshot...")
        result = create_backup_snapshot(include_archives=True)
        created = len(result["created"])
        removed = len(result["removed"])
        log(
            f"[{datetime.now().strftime('%H:%M:%S')}] Backup complete: "
            f"{created} file(s) created in {result['backup_dir']}"
            + (f", {removed} old backup(s) pruned" if removed else "")
        )
    except Exception as e:
        log(f"[{datetime.now().strftime('%H:%M:%S')}] Weekly backup error: {e}")


def start_collector(
    *,
    interval_seconds: int = SCRAPE_INTERVAL_SECONDS_DEFAULT,
    initial_scrape: bool = True,
    initial_scrape_async: bool = False,
    enable_archive: bool = True,
    enable_weekly_backup: bool = True,
) -> BackgroundScheduler:
    """Start background scraping + DB persistence, return the scheduler."""
    global scrape_interval_seconds
    scrape_interval_seconds = int(interval_seconds)
    meta_set('scrape_interval_seconds', str(scrape_interval_seconds))
    init_db()
    
    if initial_scrape:
        background_scrape()

    scheduler = BackgroundScheduler(timezone=CENTRAL_TZ)
    
    # In web-serving mode, schedule the initial collection immediately in the
    # scheduler thread so Flask can accept requests without waiting on feeds or
    # fallback geocoding. Collector-only modes retain their synchronous start.
    if initial_scrape:
        first_run_time = datetime.now(timezone.utc) + timedelta(seconds=int(scrape_interval_seconds))
    elif initial_scrape_async:
        first_run_time = datetime.now(timezone.utc)
    else:
        first_run_time = datetime.now(timezone.utc) + timedelta(seconds=int(scrape_interval_seconds))
    
    # Scrape job - runs every N seconds (normal mode with geocoding)
    scheduler.add_job(
        background_scrape,
        'interval',
        seconds=int(scrape_interval_seconds),
        next_run_time=first_run_time,
        max_instances=1,
        coalesce=True,
        id='scrape_job',
    )
    
    # Archive job - runs daily at 3:00 AM Central time
    if enable_archive:
        scheduler.add_job(
            _background_archive,
            'cron',
            hour=3,
            minute=0,
            max_instances=1,
            coalesce=True,
            id='archive_job',
        )
        log(f"[LOUISIANA 911] Daily archive scheduled for 3:00 AM Central")

    # Weekly snapshot backup - Sunday night (Central)
    if enable_weekly_backup:
        scheduler.add_job(
            _background_weekly_backup,
            'cron',
            day_of_week='sun',
            hour=23,
            minute=30,
            max_instances=1,
            coalesce=True,
            id='weekly_backup_job',
        )
        log("[LOUISIANA 911] Weekly backup scheduled for Sundays at 11:30 PM Central")
    
    scheduler.start()
    return scheduler

def run_webserver(*, host: str = '0.0.0.0', port: int = 3911) -> None:
    """Run the Flask web UI server (blocking)."""
    log(f"[LOUISIANA 911] Web UI running at http://localhost:{port}")
    log("            Press Ctrl+C to stop")
    app.run(host=host, port=port, debug=False, use_reloader=False)

def _read_key_nonblocking() -> str | None:
    """
    Non-blocking key read.
    - Windows: reads single key presses via msvcrt (no Enter needed)
    - Other: falls back to reading stdin when available (may require Enter)
    """
    if os.name == 'nt':
        try:
            import msvcrt  # type: ignore
            if msvcrt.kbhit():
                return msvcrt.getwch()
        except Exception:
            return None
        return None

    try:
        import select
        r, _, _ = select.select([sys.stdin], [], [], 0)
        if r:
            return sys.stdin.read(1)
    except Exception:
        return None
    return None

def run_interactive_mode(
    *,
    host: str = '0.0.0.0',
    port: int = 3911,
    scrape_interval_seconds: int = 60,
    enable_archive: bool = True,
    enable_weekly_backup: bool = True,
) -> None:
    """
    Start in 'event gather' mode (collector only), and allow starting the web UI on demand.
    Press '2' to start the web UI. Press 'q' to quit.
    """
    scheduler = start_collector(
        interval_seconds=scrape_interval_seconds,
        initial_scrape=True,
        enable_archive=enable_archive,
        enable_weekly_backup=enable_weekly_backup,
    )
    web_thread: Thread | None = None

    log("[LOUISIANA 911] Event gather mode is running (collector only).")
    log("          Press '2' to start the web UI, 'q' to quit.")

    try:
        while True:
            key = _read_key_nonblocking()
            if key:
                key = key.strip().lower()
                if key == '2':
                    if web_thread is None or not web_thread.is_alive():
                        log("[LOUISIANA 911] Starting web UI...")
                        web_thread = Thread(target=run_webserver, kwargs={'host': host, 'port': port}, daemon=True)
                        web_thread.start()
                    else:
                        log("[LOUISIANA 911] Web UI already running.")
                elif key in ('q',):
                    break
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass
        log("[LOUISIANA 911] Collector stopped.")

def run_gather_mode(
    *,
    scrape_interval_seconds: int = 60,
    enable_archive: bool = True,
    enable_weekly_backup: bool = True,
) -> None:
    """Collector-only mode. No web server, just keeps filling the DB."""
    scheduler = start_collector(
        interval_seconds=scrape_interval_seconds,
        initial_scrape=True,
        enable_archive=enable_archive,
        enable_weekly_backup=enable_weekly_backup,
    )
    log("[LOUISIANA 911] Collector-only mode running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass
        log("[LOUISIANA 911] Collector stopped.")

def run_regeocode(*, dry_run: bool = False, limit: int | None = None) -> None:
    """
    Re-geocode all incidents in the database using the improved geocoding logic.
    Useful for fixing historical bad geocodes (e.g., Bossier Parish false positives).
    """
    init_db()
    conn = db_connect(row_factory=True)
    cursor = conn.cursor()
    
    # Get all incidents (or limit if specified)
    if limit:
        cursor.execute('SELECT * FROM incidents ORDER BY first_seen DESC LIMIT ?', (limit,))
    else:
        cursor.execute('SELECT * FROM incidents ORDER BY first_seen DESC')
    
    rows = cursor.fetchall()
    total = len(rows)
    log(f"[REGEOCODE] Found {total} incidents to process{' (dry run)' if dry_run else ''}")
    
    updated = 0
    skipped = 0
    failed = 0
    
    # Clear the geocode cache to force fresh lookups
    geocode_cache.clear()
    geocode_intersection_cache.clear()
    
    for i, row in enumerate(rows, 1):
        # Convert Row to dict for easier access (handles missing columns gracefully)
        row_dict = dict(row)
        incident_id = row_dict['id']
        street = row_dict.get('street')
        cross_streets = row_dict.get('cross_streets')
        municipality = row_dict.get('municipality')
        old_lat = row_dict.get('latitude')
        old_lng = row_dict.get('longitude')
        old_source = row_dict.get('geocode_source')
        old_quality = row_dict.get('geocode_quality')
        old_version = row_dict.get('geocode_version')
        desc = row_dict.get('description') or 'Unknown'
        
        # Skip if no address info at all
        if not street and not cross_streets:
            skipped += 1
            continue
        
        try:
            # Get new geocode. New Orleans needs the same public block-mask,
            # intersection, and Approx Loc handling used during normal imports.
            row_source = _normalize_source_name(row_dict.get('source') or 'caddo')
            if row_source == 'neworleans':
                nola_probe = dict(row_dict)
                nola_probe['latitude'] = None
                nola_probe['longitude'] = None
                geo = _incident_geocode_result(nola_probe, row_source)
            else:
                geo = geocode_address(
                    street,
                    cross_streets,
                    municipality,
                    source=row_source,
                )
            new_lat = geo.get('lat')
            new_lng = geo.get('lng')
            new_source = geo.get('source')
            new_quality = geo.get('quality')
            
            # Check coordinate presence/distance plus metadata algorithm version.
            old_has_coords = old_lat is not None and old_lng is not None
            new_has_coords = new_lat is not None and new_lng is not None
            provider_confirmed = new_has_coords or bool(geo.get('provider_responded'))
            coordinates_changed = provider_confirmed and old_has_coords != new_has_coords
            if old_has_coords and new_has_coords:
                dist = _haversine_m(float(old_lat), float(old_lng), float(new_lat), float(new_lng))
                coordinates_changed = dist > 50
            changed = provider_confirmed and (
                coordinates_changed or any((
                    old_source != new_source,
                    old_quality != new_quality,
                    old_version != GEOCODER_VERSION,
                ))
            )
            
            # Log progress
            status = "CHANGED" if changed else "same"
            if changed and not dry_run:
                now = datetime.now(timezone.utc).isoformat()
                try:
                    cursor.execute('''
                        UPDATE incidents 
                        SET latitude = ?, longitude = ?, geocode_source = ?, geocode_quality = ?, 
                            geocode_query = ?, geocoded_at = ?, geocode_version = ?
                        WHERE id = ?
                    ''', (new_lat, new_lng, new_source, new_quality, geo.get('query'), now, GEOCODER_VERSION, incident_id))
                    updated += 1
                except sqlite3.OperationalError:
                    # Older schema
                    cursor.execute('UPDATE incidents SET latitude = ?, longitude = ? WHERE id = ?',
                                   (new_lat, new_lng, incident_id))
                    updated += 1
            elif changed:
                updated += 1  # Count as "would update" in dry run
            
            # Progress every 25 incidents
            if i % 25 == 0 or i == total:
                log(f"[REGEOCODE] Progress: {i}/{total} ({updated} updated, {skipped} skipped)")
                
        except Exception as e:
            failed += 1
            log(f"[REGEOCODE] Error on incident {incident_id}: {e}")
        
        # Small delay to avoid hammering geocoding APIs
        time.sleep(0.05)
    
    if not dry_run:
        conn.commit()
    conn.close()
    
    log(f"[REGEOCODE] Complete! Updated: {updated}, Skipped: {skipped}, Failed: {failed}")
    if dry_run:
        log(f"[REGEOCODE] (Dry run - no changes were saved)")


def run_archive(*, dry_run: bool = False) -> None:
    """Run the archive process to move old incidents to monthly archive DBs."""
    init_db()
    result = archive_old_incidents(dry_run=dry_run)
    if result['archived'] == 0:
        log("[ARCHIVE] Nothing to archive.")
    else:
        log(f"[ARCHIVE] Done. Archived {result['archived']} incidents.")

def run_backup(*, include_archives: bool = True) -> None:
    """Run a one-time SQLite backup snapshot."""
    init_db()
    result = create_backup_snapshot(include_archives=include_archives)
    created = result['created']
    skipped = result['skipped']
    removed = result['removed']
    if created:
        log(f"[BACKUP] Created {len(created)} backup file(s) in {result['backup_dir']}")
    else:
        log(f"[BACKUP] No backup files created (check paths/permissions).")
    if skipped:
        log(f"[BACKUP] Skipped missing DBs: {len(skipped)}")
    if removed:
        log(f"[BACKUP] Pruned {len(removed)} old backup file(s)")


def run_new_orleans_month_backfill(month: str) -> None:
    """Import a NOLA calendar month without deleting or replacing prior rows."""
    init_db()
    log(f"[NOLA] Fetching official citizen-initiated calls for {month}...")
    incidents, coverage_label = neworleans_source.scrape_month(
        month,
        user_agent=SCRAPER_USER_AGENT,
        timeout_seconds=60,
    )
    if not incidents:
        log(f"[NOLA] No published rows found for {month}.")
        return

    # Backfills are additive: rows outside the requested month must never be
    # deactivated or removed. A normal two-day refresh below restores the
    # exact current Latest slice after the historical import.
    process_incidents(incidents, source='neworleans', deactivate_missing=False)
    latest_incidents, refreshed_at_text = scrape_neworleans_incidents()
    if latest_incidents:
        process_incidents(latest_incidents, source='neworleans')
        _store_feed_refresh('neworleans', refreshed_at_text)

    active_count = sum(1 for incident in incidents if incident.get('is_active', True))
    log(
        f"[NOLA] Imported {len(incidents)} preserved rows for {month} "
        f"({active_count} in the current Latest slice). {coverage_label or ''}".strip()
    )


def run_new_orleans_year_mirror(year: int) -> dict:
    """Preserve every official NOLA source row and every observed revision."""
    db_path = _get_neworleans_raw_db_path(year)
    log(f"[NOLA RAW] Mirroring all official {year} rows to {db_path}...")
    result = neworleans_archive_source.mirror_year(
        year,
        db_path=db_path,
        user_agent=SCRAPER_USER_AGENT,
        timeout_seconds=120,
        progress=log,
    )
    log(
        f"[NOLA RAW] {result['status']}: {result['current_calls']:,} current calls, "
        f"{result['call_versions']:,} preserved versions, "
        f"quick_check={result['quick_check']}"
    )
    if result['status'] != 'complete' or result['quick_check'] != 'ok':
        raise RuntimeError(
            f"NOLA raw mirror verification failed: status={result['status']} "
            f"quick_check={result['quick_check']}"
        )
    return result


def run_new_orleans_year_backfill(year: int) -> None:
    """Add every retained NOLA call for a year to statewide History."""
    init_db()
    log(f"[NOLA] Fetching all retained citizen-initiated calls for {year}...")
    incidents, coverage_label = neworleans_source.scrape_year(
        year,
        user_agent=SCRAPER_USER_AGENT,
        timeout_seconds=120,
    )
    if not incidents:
        log(f"[NOLA] No published retained rows found for {year}.")
        return

    process_incidents(incidents, source='neworleans', deactivate_missing=False)
    latest_incidents, refreshed_at_text = scrape_neworleans_incidents()
    if latest_incidents:
        process_incidents(latest_incidents, source='neworleans')
        _store_feed_refresh('neworleans', refreshed_at_text)

    active_count = sum(1 for incident in incidents if incident.get('is_active', True))
    log(
        f"[NOLA] Imported {len(incidents):,} preserved display rows for {year} "
        f"({active_count} in the current Latest slice). {coverage_label or ''}".strip()
    )


def run_new_orleans_year_prepare(year: int) -> None:
    """Build the raw annual mirror first, then the curated statewide history."""
    run_new_orleans_year_mirror(year)
    run_new_orleans_year_backfill(year)


def main() -> None:
    parser = argparse.ArgumentParser(description="Louisiana 911 public incident monitor")
    parser.add_argument("--mode", choices=["serve", "gather", "interactive"], default="serve",
                        help="serve: collector + web UI (default); gather: collector only; interactive: collector only, press 2 to start UI")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=3911)
    parser.add_argument("--interval", type=int, default=60, help="scrape interval in seconds")
    parser.add_argument("--quiet", action="store_true", help="reduce console output (recommended for gather mode)")
    parser.add_argument("--regeocode", action="store_true", help="re-geocode all incidents in database using improved logic, then exit")
    parser.add_argument("--regeocode-dry-run", action="store_true", help="like --regeocode but don't save changes (preview only)")
    parser.add_argument("--regeocode-limit", type=int, default=None, help="limit re-geocoding to N most recent incidents")
    parser.add_argument("--archive", action="store_true", help=f"archive incidents older than {ARCHIVE_AFTER_DAYS} days to monthly DBs, then exit")
    parser.add_argument("--archive-dry-run", action="store_true", help="like --archive but don't move anything (preview only)")
    parser.add_argument("--no-auto-archive", action="store_true", help="disable daily automatic archiving (3 AM Central)")
    parser.add_argument("--backup", action="store_true", help="create a one-time SQLite backup snapshot, then exit")
    parser.add_argument("--backup-main-only", action="store_true", help="when used with --backup, only back up the main DB (skip archive DBs)")
    parser.add_argument("--no-auto-backup", action="store_true", help="disable weekly automatic backup snapshots (Sunday 11:30 PM Central)")
    parser.add_argument(
        "--backfill-neworleans-month",
        metavar="YYYY-MM",
        help="additively import one full official New Orleans calendar month, then exit",
    )
    parser.add_argument(
        "--mirror-neworleans-year",
        type=int,
        metavar="YYYY",
        help="append every official NOLA source row/version for a year to its raw mirror DB, then exit",
    )
    parser.add_argument(
        "--backfill-neworleans-year",
        type=int,
        metavar="YYYY",
        help="additively import every retained NOLA call for a year into statewide History, then exit",
    )
    parser.add_argument(
        "--prepare-neworleans-year",
        type=int,
        metavar="YYYY",
        help="mirror all raw NOLA rows, then add every retained call to statewide History",
    )
    args = parser.parse_args()

    global QUIET
    QUIET = bool(args.quiet or args.mode == "gather")

    # Handle archive modes (one-time, then exit)
    if args.archive or args.archive_dry_run:
        run_archive(dry_run=args.archive_dry_run)
        return

    # Handle re-geocode modes (one-time, then exit)
    if args.regeocode or args.regeocode_dry_run:
        run_regeocode(dry_run=args.regeocode_dry_run, limit=args.regeocode_limit)
        return

    # Handle one-time backup mode
    if args.backup:
        run_backup(include_archives=not args.backup_main_only)
        return

    if args.backfill_neworleans_month:
        run_new_orleans_month_backfill(args.backfill_neworleans_month)
        return

    if args.mirror_neworleans_year:
        run_new_orleans_year_mirror(args.mirror_neworleans_year)
        return

    if args.backfill_neworleans_year:
        run_new_orleans_year_backfill(args.backfill_neworleans_year)
        return

    if args.prepare_neworleans_year:
        run_new_orleans_year_prepare(args.prepare_neworleans_year)
        return

    # Store archive preference for start_collector
    enable_archive = not args.no_auto_archive
    enable_weekly_backup = not args.no_auto_backup

    if args.mode == "gather":
        run_gather_mode(
            scrape_interval_seconds=args.interval,
            enable_archive=enable_archive,
            enable_weekly_backup=enable_weekly_backup,
        )
        return

    if args.mode == "interactive":
        run_interactive_mode(
            host=args.host,
            port=args.port,
            scrape_interval_seconds=args.interval,
            enable_archive=enable_archive,
            enable_weekly_backup=enable_weekly_backup,
        )
        return

    # serve mode: collector + web UI immediately
    scheduler = start_collector(
        interval_seconds=args.interval,
        initial_scrape=False,
        initial_scrape_async=True,
        enable_archive=enable_archive,
        enable_weekly_backup=enable_weekly_backup,
    )
    try:
        run_webserver(host=args.host, port=args.port)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass
        log("[LOUISIANA 911] Shutting down.")

if __name__ == '__main__':
    main()
