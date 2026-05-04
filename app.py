#!/usr/bin/env python3
"""
CADDO 911 Live Feed - Real-time emergency incident tracker
Scrapes Caddo Parish 911 dispatch data and displays on interactive map
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
from datetime import datetime, timezone, timedelta
from threading import Lock, Thread
from flask import Flask, jsonify, request, send_from_directory
from geopy.geocoders import Nominatim
from apscheduler.schedulers.background import BackgroundScheduler
from zoneinfo import ZoneInfo
from sources import caddo as caddo_source
from sources import lafayette as lafayette_source
from sources import batonrouge as batonrouge_source

app = Flask(__name__, static_folder='public', static_url_path='')

# Database setup
def _resolve_db_path() -> str:
    """
    Pick the live SQLite database path.

    Production installs commonly keep the git checkout in ./repo and the
    persistent SQLite files in a sibling ./data directory. Keep
    CADDO911_DB_PATH as the strongest override, then auto-detect that layout so
    report pages read the same DBs as the scraper.
    """
    configured_path = os.environ.get('CADDO911_DB_PATH', '').strip()
    if configured_path:
        return os.path.expanduser(configured_path)

    app_dir = os.path.dirname(os.path.abspath(__file__))
    cwd = os.getcwd()
    configured_data_dir = os.environ.get('CADDO911_DATA_DIR', '').strip()
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
ARCHIVE_AFTER_DAYS = int(os.environ.get('CADDO911_ARCHIVE_DAYS', '30'))
BACKUP_RETENTION_WEEKS = int(os.environ.get('CADDO911_BACKUP_RETENTION_WEEKS', '5'))
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

def _get_backup_dir() -> str:
    """Directory where periodic backup snapshots are written."""
    configured = os.environ.get('CADDO911_BACKUP_DIR', '').strip()
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
    - Optionally includes archive DBs.
    """
    backup_dir = _get_backup_dir()
    os.makedirs(backup_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    targets = [DB_PATH]
    if include_archives:
        targets.extend(_list_archive_dbs())
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
            geocoded_at DATETIME
        )
    ''')
    try:
        cursor.execute("ALTER TABLE incidents ADD COLUMN source TEXT DEFAULT 'caddo'")
    except sqlite3.OperationalError:
        pass
    cursor.execute("UPDATE incidents SET source = 'caddo' WHERE source IS NULL OR TRIM(source) = ''")
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash ON incidents(hash)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_first_seen ON incidents(first_seen)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_source ON incidents(source)')
    conn.commit()
    conn.close()

# Scraper/status metadata
SCRAPE_INTERVAL_SECONDS_DEFAULT = 60
feed_refreshed_at: str | None = None  # backwards-compatible: Caddo refresh text
feed_refreshed_by_source: dict[str, str | None] = {"caddo": None, "lafayette": None, "batonrouge": None}
last_scrape_started_at: str | None = None  # ISO UTC
last_scrape_finished_at: str | None = None  # ISO UTC

# Identify the scraper politely in upstream logs.
SCRAPER_USER_AGENT = "Friendly - Caddo911.vincentlarkin.com (+https://caddo911.vincentlarkin.com)"

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
        ("geocode_source", "TEXT"),     # 'arcgis' | 'osm' | 'fallback'
        ("geocode_quality", "TEXT"),    # 'intersection-2' | 'street+cross' | 'street-only' | 'cross-only' | 'fallback'
        ("geocode_query", "TEXT"),      # the query string we sent to the provider
        ("geocoded_at", "DATETIME"),    # UTC ISO timestamp
    ):
        try:
            cursor.execute(f"ALTER TABLE incidents ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass

    # Backfill legacy rows that predate multi-source support.
    cursor.execute("UPDATE incidents SET source = 'caddo' WHERE source IS NULL OR TRIM(source) = ''")
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_source ON incidents(source)')

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
                         geocode_source, geocode_quality, geocode_query, geocoded_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
AUTH_TOKEN = os.environ.get('CADDO911_AUTH_TOKEN')
AUTH_USER = os.environ.get('CADDO911_AUTH_USER')
AUTH_PASS = os.environ.get('CADDO911_AUTH_PASS')

# Simple in-process report rate limiting. This is intentionally lightweight:
# enough to slow accidental refresh loops and casual abuse without adding
# infrastructure or dependencies.
REPORT_PAGE_RATE_LIMIT = int(os.environ.get('CADDO911_REPORT_PAGE_RATE_LIMIT', '120'))
REPORT_API_RATE_LIMIT = int(os.environ.get('CADDO911_REPORT_API_RATE_LIMIT', '90'))
REPORT_MAP_RATE_LIMIT = int(os.environ.get('CADDO911_REPORT_MAP_RATE_LIMIT', '25'))
REPORT_RATE_WINDOW_SECONDS = int(os.environ.get('CADDO911_REPORT_RATE_WINDOW_SECONDS', '60'))
_report_rate_lock = Lock()
_report_rate_hits: dict[tuple[str, str], list[float]] = {}

def _unauthorized():
    resp = jsonify({'error': 'unauthorized'})
    resp.status_code = 401
    if AUTH_USER and AUTH_PASS:
        resp.headers['WWW-Authenticate'] = 'Basic realm="caddo911-live"'
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
    if clean_path == '/api/reports/map':
        return 'report-map', REPORT_MAP_RATE_LIMIT
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
    return resp

# Geocoder setup with caching - try ArcGIS first (better US coverage), fallback to Nominatim
from geopy.geocoders import ArcGIS
geolocator_arcgis = ArcGIS(timeout=5)
geolocator_osm = Nominatim(user_agent=SCRAPER_USER_AGENT, timeout=5)
geocode_cache = {}

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
        "lat_max": 32.90,
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
        p = _clean_ws(p)
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
    street_s = _clean_ws(street or "")
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

def geocode_address(street, cross_streets, municipality, source: str = 'caddo'):
    """
    Convert address to lat/lng coordinates.
    Fast, best-effort geocoding with source-aware bounds.
    """
    import random

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
    cross1 = crosses[0] if len(crosses) > 0 else None
    cross2 = crosses[1] if len(crosses) > 1 else None

    # Cache key
    cache_key = f"{source_name}|{street_clean or ''}|{cross1 or ''}|{cross2 or ''}|{cache_locality}"
    if cache_key in geocode_cache:
        return geocode_cache[cache_key]

    # Build queries in priority order - best first
    queries: list[tuple[str, str]] = []
    seen_queries: set[str] = set()

    def add_query(location_part: str | None, quality: str) -> None:
        location_part = _clean_ws(location_part or "")
        if not location_part:
            return
        for locality_parts in locality_variants:
            query = ", ".join((location_part, *locality_parts))
            query_key = query.lower()
            if query_key in seen_queries:
                continue
            seen_queries.add(query_key)
            queries.append((query, quality))

    # Best: intersection of two cross streets
    if cross1 and cross2:
        add_query(f"{cross1} & {cross2}", "intersection-2")
    # Good: street + cross street
    if street_clean and cross1:
        add_query(f"{street_clean} & {cross1}", "street+cross")
    if street_clean and cross2:
        add_query(f"{street_clean} & {cross2}", "street+cross")
    # OK: just the street
    if street_clean:
        add_query(street_clean, "street-only")
    # Fallback: just a cross street
    if cross1:
        add_query(cross1, "cross-only")
    
    # Quality levels - only return early on GOOD matches
    good_qualities = {'intersection-2', 'street+cross'}
    
    # Collect all valid results, return early only on high-quality matches
    valid_results: list[dict] = []
    
    # Try ArcGIS
    for query, quality in queries:
        try:
            location = geolocator_arcgis.geocode(query, timeout=3)
            if location and _is_in_source_bounds(location.latitude, location.longitude, source_name):
                result = {
                    'lat': location.latitude,
                    'lng': location.longitude,
                    'source': 'arcgis',
                    'quality': quality,
                    'query': query,
                }
                # Only return early if it's a HIGH-QUALITY match
                if quality in good_qualities:
                    geocode_cache[cache_key] = result
                    log(f"  [ARC] {quality} | {query} -> ({result['lat']:.5f}, {result['lng']:.5f})")
                    return result
                # Otherwise save it and keep trying for something better
                valid_results.append(result)
        except Exception:
            pass
    
    # Try OSM if we don't have a good result yet
    for query, quality in queries[:8]:
        try:
            location = geolocator_osm.geocode(query, country_codes='us', exactly_one=True, timeout=3)
            if location and _is_in_source_bounds(location.latitude, location.longitude, source_name):
                result = {
                    'lat': location.latitude,
                    'lng': location.longitude,
                    'source': 'osm',
                    'quality': quality,
                    'query': query,
                }
                if quality in good_qualities:
                    geocode_cache[cache_key] = result
                    log(f"  [OSM] {quality} | {query} -> ({result['lat']:.5f}, {result['lng']:.5f})")
                    return result
                valid_results.append(result)
        except Exception:
            pass
    
    # Pick best from what we collected
    if valid_results:
        # Sort by quality: intersection-2 > street+cross > street-only > cross-only
        quality_rank = {'intersection-2': 4, 'street+cross': 3, 'street-only': 2, 'cross-only': 1}
        county = (geo_profile.get("county") or "").lower()
        valid_results.sort(
            key=lambda r: (
                quality_rank.get(r['quality'], 0),
                1 if county and county in (r.get('query') or '').lower() else 0,
                -_haversine_m(
                    float(r['lat']),
                    float(r['lng']),
                    float(geo_profile['center_lat']),
                    float(geo_profile['center_lon']),
                ),
            ),
            reverse=True,
        )
        result = valid_results[0]
        geocode_cache[cache_key] = result
        log(f"  [{result['source'].upper()[:3]}] {result['quality']} | {result['query']} -> ({result['lat']:.5f}, {result['lng']:.5f})")
        return result
    
    # Nothing worked - fallback
    log(f"  [--] fallback | {street_clean or '?'} @ {cross1 or '?'} {'& ' + cross2 if cross2 else ''}")
    
    # Fallback to source region center with slight jitter.
    offset = lambda: (random.random() - 0.5) * 0.04
    default = {
        'lat': geo_profile['center_lat'] + offset(),
        'lng': geo_profile['center_lon'] + offset(),
        'source': 'fallback',
        'quality': 'fallback',
        'query': None,
    }
    geocode_cache[cache_key] = default
    return default

def hash_incident(incident):
    """Generate unique hash for incident deduplication"""
    source = _normalize_source_name(incident.get('source') if isinstance(incident, dict) else None)
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


# Backwards compatibility for any old call sites.
def scrape_incidents():
    return scrape_caddo_incidents()

# Track last update time
last_update: str | None = None
scrape_interval_seconds: int = SCRAPE_INTERVAL_SECONDS_DEFAULT

def process_incidents(incidents, *, source: str = 'caddo'):
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

    for incident in incidents:
        incident_source = _normalize_source_name(incident.get('source') if isinstance(incident, dict) else source_default)
        incident['source'] = incident_source
        h = hash_incident(incident)
        current_hashes.add(h)

        # Check if exists
        try:
            cursor.execute(
                'SELECT id, latitude, longitude, geocode_source, geocode_quality FROM incidents WHERE hash = ?',
                (h,)
            )
            existing = cursor.fetchone()
            existing_cols = "new"
        except sqlite3.OperationalError:
            cursor.execute('SELECT id, latitude, longitude FROM incidents WHERE hash = ?', (h,))
            existing = cursor.fetchone()
            existing_cols = "old"
        
        if existing:
            # Update last_seen
            try:
                cursor.execute('UPDATE incidents SET last_seen = ?, is_active = 1, source = ? WHERE hash = ?', (now, incident_source, h))
            except sqlite3.OperationalError:
                cursor.execute('UPDATE incidents SET last_seen = ?, is_active = 1 WHERE hash = ?', (now, h))

            # Opportunistic re-geocode: if we previously fell back (or have no coords),
            # try again using the improved intersection logic. This keeps your DB, but improves
            # "bad" points over time.
            try:
                existing_lat = existing[1]
                existing_lng = existing[2]
                existing_source = existing[3] if existing_cols == "new" and len(existing) > 3 else None
                existing_quality = existing[4] if existing_cols == "new" and len(existing) > 4 else None

                needs_geo = (existing_lat is None or existing_lng is None)
                low_quality = (existing_source in (None, "fallback", "skipped")) or (existing_quality in (None, "fallback", "city-only", "cross-only", "unknown-location"))
                if (needs_geo or low_quality) and (incident.get('street') or incident.get('cross_streets')):
                    geo = geocode_address(
                        incident.get('street'),
                        incident.get('cross_streets'),
                        incident.get('municipality'),
                        source=incident_source,
                    )
                    if geo and geo.get('lat') is not None and geo.get('lng') is not None:
                        should_update = False
                        if needs_geo:
                            should_update = True
                        else:
                            try:
                                dist_m = _haversine_m(float(existing_lat), float(existing_lng), float(geo['lat']), float(geo['lng']))
                                # Only overwrite if materially different (avoid churning minor provider jitter)
                                if dist_m > 75:
                                    should_update = True
                            except Exception:
                                # If distance calc fails, be conservative and avoid overwriting
                                should_update = False

                        if should_update:
                            try:
                                cursor.execute(
                                    'UPDATE incidents SET latitude = ?, longitude = ?, geocode_source = ?, geocode_quality = ?, geocode_query = ?, geocoded_at = ? WHERE hash = ?',
                                    (
                                        geo['lat'],
                                        geo['lng'],
                                        geo.get('source'),
                                        geo.get('quality'),
                                        geo.get('query'),
                                        now,
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
            geo = geocode_address(
                incident['street'],
                incident['cross_streets'],
                incident['municipality'],
                source=incident_source,
            )
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO incidents 
                    (hash, agency, time, units, description, street, cross_streets, municipality, source,
                     latitude, longitude, first_seen, last_seen,
                     geocode_source, geocode_quality, geocode_query, geocoded_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    geo['lat'],
                    geo['lng'],
                    now,
                    now,
                    geo.get('source'),
                    geo.get('quality'),
                    geo.get('query'),
                    now,
                ))
            except sqlite3.OperationalError:
                # Older schema: try insert without geocode metadata columns.
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO incidents 
                        (hash, agency, time, units, description, street, cross_streets, municipality, source, latitude, longitude, first_seen, last_seen)
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
                        incident_source,
                        geo['lat'],
                        geo['lng'],
                        now,
                        now
                    ))
                except sqlite3.OperationalError:
                    cursor.execute('''
                        INSERT OR IGNORE INTO incidents 
                        (hash, agency, time, units, description, street, cross_streets, municipality, latitude, longitude, first_seen, last_seen)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        now,
                        now
                    ))
            log(f"New incident: {incident['description']} at {incident['street'] or incident['cross_streets']}")

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
        ('lafayette', 'Lafayette 911 (beta)', scrape_lafayette_incidents),
    ]
    for source_name, label, scraper in source_jobs:
        log(f"[{datetime.now().strftime('%H:%M:%S')}] Scraping {label}...")
        incidents, refreshed_at_text = scraper()
        _store_feed_refresh(source_name, refreshed_at_text)
        if incidents:
            process_incidents(incidents, source=source_name)
            log(f"[{datetime.now().strftime('%H:%M:%S')}] Processed {len(incidents)} active incidents from {source_name}")
        else:
            log(f"[{datetime.now().strftime('%H:%M:%S')}] No incidents found or scraping failed for {source_name}")

    last_scrape_finished_at = datetime.now(timezone.utc).isoformat()
    meta_set('last_scrape_finished_at', last_scrape_finished_at)


VALID_SOURCES = {'caddo', 'lafayette', 'batonrouge'}


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

    return sorted(months, reverse=True)


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
    if not raw_value:
        return list(REPORT_MAP_COLOR_ORDER)
    colors: list[str] = []
    for part in raw_value.split(','):
        color = part.strip().lower()
        if color in REPORT_MAP_COLOR_META and color not in colors:
            colors.append(color)
    return colors or list(REPORT_MAP_COLOR_ORDER)


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
    if not address:
        return None

    source_names = list(SOURCE_GEO_PROFILES.keys()) if source_filter == 'all' else [source_filter]
    queries: list[str] = []
    for source_name in source_names:
        profile = SOURCE_GEO_PROFILES.get(source_name) or SOURCE_GEO_PROFILES['caddo']
        default_city = _clean_ws(profile.get('default_city') or '')
        county = _clean_ws(profile.get('county') or '')
        for query in (
            address,
            f"{address}, {default_city}, LA" if default_city else '',
            f"{address}, {county}, LA" if county else '',
            f"{address}, Louisiana",
        ):
            query = _clean_ws(query)
            if query and query.lower() not in {q.lower() for q in queries}:
                queries.append(query)

    best_out_of_scope: dict | None = None
    for provider_name, geocoder in (('arcgis', geolocator_arcgis), ('osm', geolocator_osm)):
        for query in queries:
            try:
                if provider_name == 'osm':
                    location = geocoder.geocode(query, country_codes='us', exactly_one=True, timeout=4)
                else:
                    location = geocoder.geocode(query, timeout=4)
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
                'geocodeSource': provider_name,
                'inSourceBounds': _point_in_report_source_scope(lat, lon, source_filter),
            }
            if result['inSourceBounds']:
                return result
            if best_out_of_scope is None:
                best_out_of_scope = result

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


def _sample_anchor_points(incidents: list[dict], *, limit: int = 450) -> list[dict]:
    if len(incidents) <= limit:
        return incidents
    if limit <= 1:
        return incidents[:1]
    step = (len(incidents) - 1) / (limit - 1)
    return [incidents[int(round(idx * step))] for idx in range(limit)]


def _count_incidents_near(lat: float, lon: float, incidents: list[dict], radius_m: float) -> int:
    count = 0
    for incident in incidents:
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

    sampled_anchors = _sample_anchor_points(anchor_incidents)
    peer_counts = [
        _count_incidents_near(float(anchor['_lat']), float(anchor['_lon']), period_incidents, radius_m)
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

@app.route('/reports')
@app.route('/reports/')
def reports():
    return send_from_directory('public', 'reports.html')

@app.route('/reports/monthly')
@app.route('/reports/monthly/')
def monthly_reports():
    return send_from_directory('public', 'monthly-reports.html')

@app.route('/reports/map')
@app.route('/reports/map/')
def map_reports():
    return send_from_directory('public', 'map-report.html')

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


@app.route('/api/reports/map/options')
def get_map_report_options():
    source_filter = _normalize_source_filter(request.args.get('source'))
    month = _normalize_report_map_month(request.args.get('month'))
    if month is None:
        return jsonify({'error': 'month must be all, this_year, or YYYY-MM'}), 400

    colors = _parse_report_colors(request.args.get('colors'))
    excluded = _normalize_report_excluded_descriptions()
    months = _available_report_months(source_filter, excluded_descriptions=excluded)
    incidents = _load_report_period_incidents(
        month,
        source_filter,
        excluded_descriptions=excluded,
    )
    decorated = [
        decorated for incident in incidents
        if (decorated := _decorate_report_map_incident(incident)) is not None
    ]

    color_counts = {color: 0 for color in REPORT_MAP_COLOR_ORDER}
    for incident in decorated:
        color = incident.get('_reportColor') or _incident_report_color(incident)
        if color in color_counts:
            color_counts[color] += 1

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


@app.route('/api/reports/map')
def get_map_report():
    source_filter = _normalize_source_filter(request.args.get('source'))
    month = _normalize_report_map_month(request.args.get('month'))
    if month is None:
        return jsonify({'error': 'month must be all, this_year, or YYYY-MM'}), 400

    radius_miles = request.args.get('radius_miles', default=2.0, type=float)
    if radius_miles is None or not math.isfinite(radius_miles) or radius_miles < 0.25 or radius_miles > 25:
        return jsonify({'error': 'radius_miles must be a number between 0.25 and 25'}), 400

    colors = _parse_report_colors(request.args.get('colors'))
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
        target = _geocode_report_address(address, source_filter)
    else:
        return jsonify({'error': 'address or lat/lng is required'}), 400

    if not target:
        return jsonify({'error': 'address could not be geocoded'}), 404

    target_lat = float(target['latitude'])
    target_lon = float(target['longitude'])
    radius_m = radius_miles * 1609.344
    excluded = _normalize_report_excluded_descriptions()
    incidents = _load_report_period_incidents(
        month,
        source_filter,
        excluded_descriptions=excluded,
    )
    mappable = [
        decorated for incident in incidents
        if (decorated := _decorate_report_map_incident(incident)) is not None
    ]

    for incident in mappable:
        incident['_distanceMiles'] = _distance_miles_to_incident(target_lat, target_lon, incident)

    in_radius = [
        incident for incident in mappable
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
        mappable,
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
            mappable,
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
    available_types = _report_incident_type_counts(mappable, colors)[:250]
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
        'incidentTypes': available_types,
        'incidents': [_serialize_map_report_incident(incident) for incident in selected_in_radius[:500]],
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
    }

    return jsonify({
        # lastUpdate: UTC ISO timestamp of when we last processed/saved a scrape
        'lastUpdate': meta.get('last_update') or last_update,
        # feedRefreshedAt: the website's own "Refreshed at: ..." text (local to Caddo911)
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
        enabled = os.environ.get('CADDO911_ENABLE_REFRESH_ENDPOINT', '0').strip().lower() in ('1', 'true', 'yes')
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
    
    # Calculate when the next scrape should run
    # If we just did an initial scrape, wait the full interval before the next one
    first_run_time = datetime.now(timezone.utc) + timedelta(seconds=int(scrape_interval_seconds)) if initial_scrape else None
    
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
        log(f"[CADDO 911] Daily archive scheduled for 3:00 AM Central")

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
        log("[CADDO 911] Weekly backup scheduled for Sundays at 11:30 PM Central")
    
    scheduler.start()
    return scheduler

def run_webserver(*, host: str = '0.0.0.0', port: int = 3911) -> None:
    """Run the Flask web UI server (blocking)."""
    log(f"[CADDO 911] Web UI running at http://localhost:{port}")
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

    log("[CADDO 911] Event gather mode is running (collector only).")
    log("          Press '2' to start the web UI, 'q' to quit.")

    try:
        while True:
            key = _read_key_nonblocking()
            if key:
                key = key.strip().lower()
                if key == '2':
                    if web_thread is None or not web_thread.is_alive():
                        log("[CADDO 911] Starting web UI...")
                        web_thread = Thread(target=run_webserver, kwargs={'host': host, 'port': port}, daemon=True)
                        web_thread.start()
                    else:
                        log("[CADDO 911] Web UI already running.")
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
        log("[CADDO 911] Collector stopped.")

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
    log("[CADDO 911] Collector-only mode running. Press Ctrl+C to stop.")
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
        log("[CADDO 911] Collector stopped.")

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
        desc = row_dict.get('description') or 'Unknown'
        
        # Skip if no address info at all
        if not street and not cross_streets:
            skipped += 1
            continue
        
        try:
            # Get new geocode
            geo = geocode_address(
                street,
                cross_streets,
                municipality,
                source=row_dict.get('source') or 'caddo',
            )
            new_lat = geo.get('lat')
            new_lng = geo.get('lng')
            new_source = geo.get('source')
            new_quality = geo.get('quality')
            
            # Check if coordinates changed significantly (>50m)
            changed = False
            if old_lat is None or old_lng is None:
                changed = True
            elif new_lat is not None and new_lng is not None:
                dist = _haversine_m(float(old_lat), float(old_lng), float(new_lat), float(new_lng))
                changed = dist > 50
            
            # Log progress
            status = "CHANGED" if changed else "same"
            if changed and not dry_run:
                now = datetime.now(timezone.utc).isoformat()
                try:
                    cursor.execute('''
                        UPDATE incidents 
                        SET latitude = ?, longitude = ?, geocode_source = ?, geocode_quality = ?, 
                            geocode_query = ?, geocoded_at = ?
                        WHERE id = ?
                    ''', (new_lat, new_lng, new_source, new_quality, geo.get('query'), now, incident_id))
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Caddo 911 Live Feed")
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
        initial_scrape=True,
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
        log("[CADDO 911] Shutting down.")

if __name__ == '__main__':
    main()
