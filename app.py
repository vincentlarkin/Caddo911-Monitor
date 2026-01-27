#!/usr/bin/env python3
"""
CADDO 911 Live Feed - Real-time emergency incident tracker
Scrapes Caddo Parish 911 dispatch data and displays on interactive map
"""

import sqlite3
import hashlib
import json
import time
import argparse
import os
import sys
import math
import re
from datetime import datetime, timezone, timedelta
from threading import Thread
from flask import Flask, jsonify, request, send_from_directory
import requests
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from apscheduler.schedulers.background import BackgroundScheduler
from zoneinfo import ZoneInfo

app = Flask(__name__, static_folder='public', static_url_path='')

# Database setup
DB_PATH = os.environ.get('CADDO911_DB_PATH', 'caddo911.db')

# Archive settings: incidents older than this many days get moved to monthly archive DBs
ARCHIVE_AFTER_DAYS = int(os.environ.get('CADDO911_ARCHIVE_DAYS', '30'))

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
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash ON incidents(hash)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_first_seen ON incidents(first_seen)')
    conn.commit()
    conn.close()

# Scraper/status metadata
SCRAPE_INTERVAL_SECONDS_DEFAULT = 60
feed_refreshed_at: str | None = None  # e.g. "January 13 09:56"
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
        ("geocode_source", "TEXT"),     # 'arcgis' | 'osm' | 'fallback'
        ("geocode_quality", "TEXT"),    # 'intersection-2' | 'street+cross' | 'street-only' | 'cross-only' | 'fallback'
        ("geocode_query", "TEXT"),      # the query string we sent to the provider
        ("geocoded_at", "DATETIME"),    # UTC ISO timestamp
    ):
        try:
            cursor.execute(f"ALTER TABLE incidents ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass

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
                        (hash, agency, time, units, description, street, cross_streets, municipality,
                         latitude, longitude, first_seen, last_seen, is_active,
                         geocode_source, geocode_quality, geocode_query, geocoded_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        inc.get('hash'),
                        inc.get('agency'),
                        inc.get('time'),
                        inc.get('units'),
                        inc.get('description'),
                        inc.get('street'),
                        inc.get('cross_streets'),
                        inc.get('municipality'),
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

@app.before_request
def _auth_middleware():
    # Allow container health checks without auth
    if request.path == '/healthz':
        return None
    if not _check_auth():
        return _unauthorized()
    return None

@app.after_request
def _security_headers(resp):
    # Minimal hardening headers (avoid breaking external map tiles/scripts)
    resp.headers.setdefault('X-Content-Type-Options', 'nosniff')
    resp.headers.setdefault('X-Frame-Options', 'DENY')
    resp.headers.setdefault('Referrer-Policy', 'same-origin')
    resp.headers.setdefault('Permissions-Policy', 'geolocation=(), microphone=(), camera=()')
    return resp

# Geocoder setup with caching - try ArcGIS first (better US coverage), fallback to Nominatim
from geopy.geocoders import ArcGIS
geolocator_arcgis = ArcGIS(timeout=5)
geolocator_osm = Nominatim(user_agent=SCRAPER_USER_AGENT, timeout=5)
geocode_cache = {}

# Caddo Parish bounding box - exclude Bossier Parish (east of Red River ~-93.65)
# Caddo Parish is roughly: lat 32.15-32.85, lon -94.04 to -93.56
# But to avoid Bossier false positives, we use a tighter western bound
CADDO_LAT_MIN = 32.10
CADDO_LAT_MAX = 32.90
CADDO_LON_MIN = -94.10  # western edge of Caddo Parish
CADDO_LON_MAX = -93.62  # just west of Red River (excludes most of Bossier)

# Shreveport city center (for distance-based fallback ranking)
SHREVEPORT_CENTER_LAT = 32.5252
SHREVEPORT_CENTER_LON = -93.7502

def _is_in_caddo_bounds(lat: float, lon: float) -> bool:
    """Check if coordinates are within Caddo Parish (west of Red River)."""
    return (CADDO_LAT_MIN < lat < CADDO_LAT_MAX and 
            CADDO_LON_MIN < lon < CADDO_LON_MAX)

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
        up = p.upper()
        if "DEAD END" in up or up in ("DEADEND",):
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

def geocode_address(street, cross_streets, municipality):
    """
    Convert address to lat/lng coordinates.
    Fast, best-effort geocoding - tries ArcGIS first, returns as soon as we get 
    a valid Caddo Parish result.
    """
    import random
    
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
    
    city = _clean_ws(municipality or "") or 'Shreveport'
    state = 'LA'

    street_clean, crosses = _extract_street_and_crosses(street, cross_streets)
    cross1 = crosses[0] if len(crosses) > 0 else None
    cross2 = crosses[1] if len(crosses) > 1 else None

    # Cache key
    cache_key = f"{street_clean or ''}|{cross1 or ''}|{cross2 or ''}|{city}"
    if cache_key in geocode_cache:
        return geocode_cache[cache_key]

    # Build queries in priority order - best first
    queries: list[tuple[str, str]] = []
    
    # Best: intersection of two cross streets
    if cross1 and cross2:
        queries.append((f"{cross1} & {cross2}, {city}, {state}", "intersection-2"))
    # Good: street + cross street  
    if street_clean and cross1:
        queries.append((f"{street_clean} & {cross1}, {city}, {state}", "street+cross"))
    if street_clean and cross2:
        queries.append((f"{street_clean} & {cross2}, {city}, {state}", "street+cross"))
    # OK: just the street
    if street_clean:
        queries.append((f"{street_clean}, {city}, {state}", "street-only"))
    # Fallback: just a cross street
    if cross1:
        queries.append((f"{cross1}, {city}, {state}", "cross-only"))
    
    # Quality levels - only return early on GOOD matches
    good_qualities = {'intersection-2', 'street+cross'}
    
    # Collect all valid results, return early only on high-quality matches
    valid_results: list[dict] = []
    
    # Try ArcGIS
    for query, quality in queries:
        try:
            location = geolocator_arcgis.geocode(query, timeout=3)
            if location and _is_in_caddo_bounds(location.latitude, location.longitude):
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
    for query, quality in queries[:3]:
        try:
            location = geolocator_osm.geocode(query, country_codes='us', exactly_one=True, timeout=3)
            if location and _is_in_caddo_bounds(location.latitude, location.longitude):
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
        valid_results.sort(key=lambda r: quality_rank.get(r['quality'], 0), reverse=True)
        result = valid_results[0]
        geocode_cache[cache_key] = result
        log(f"  [{result['source'].upper()[:3]}] {result['quality']} | {result['query']} -> ({result['lat']:.5f}, {result['lng']:.5f})")
        return result
    
    # Nothing worked - fallback
    log(f"  [--] fallback | {street_clean or '?'} @ {cross1 or '?'} {'& ' + cross2 if cross2 else ''}")
    
    # Fallback to Shreveport area (randomized within western Caddo Parish)
    offset = lambda: (random.random() - 0.5) * 0.04
    default = {
        'lat': 32.47 + offset(),
        'lng': -93.79 + offset(),
        'source': 'fallback',
        'quality': 'fallback',
        'query': None,
    }
    geocode_cache[cache_key] = default
    return default

def hash_incident(incident):
    """Generate unique hash for incident deduplication"""
    key = f"{incident['agency']}-{incident['time']}-{incident['description']}-{incident['street']}-{incident['cross_streets']}"
    return hashlib.md5(key.encode()).hexdigest()

def scrape_incidents():
    """
    Scrape active incidents from Caddo 911 website (ASP.NET site with cookie requirement).
    Returns a tuple: (incidents, refreshed_at_text)
      - refreshed_at_text example: "January 13 09:56"
    """
    base_url = 'https://ias.ecc.caddo911.com/All_ActiveEvents.aspx'
    
    try:
        # Create session to handle cookies (ASP.NET requires this)
        session = requests.Session()
        headers = {
            'User-Agent': SCRAPER_USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
        session.headers.update(headers)
        
        # First request establishes session cookie
        response = session.get(base_url, timeout=15, allow_redirects=True)
        
        # If redirected to cookie check URL, follow it
        if 'AspxAutoDetectCookieSupport' in response.url or response.status_code == 302:
            response = session.get(f"{base_url}?AspxAutoDetectCookieSupport=1", timeout=15)
        
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        incidents = []
        refreshed_at_text = None

        # Extract the page's own refresh banner timestamp (matches: "Refreshed at: ...")
        for s in soup.stripped_strings:
            if 'Refreshed at:' in s:
                refreshed_at_text = s.split('Refreshed at:', 1)[1].strip()
                break
        
        # Find the data table - look for table with incident data
        tables = soup.find_all('table')
        
        for table in tables:
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) >= 6:
                    agency = cells[0].get_text(strip=True)
                    time_val = cells[1].get_text(strip=True)
                    units = cells[2].get_text(strip=True)
                    description = cells[3].get_text(strip=True)
                    street = cells[4].get_text(strip=True)
                    cross_streets = cells[5].get_text(strip=True)
                    municipality = cells[6].get_text(strip=True) if len(cells) > 6 else ''
                    
                    # Validate: agency should be short code, time should be 3-4 digits
                    if (agency and len(agency) <= 10 and 
                        time_val and time_val.isdigit() and len(time_val) <= 4 and
                        description):
                        incidents.append({
                            'agency': agency,
                            'time': time_val,
                            'units': int(units) if units.isdigit() else 1,
                            'description': description,
                            'street': street,
                            'cross_streets': cross_streets,
                            'municipality': municipality
                        })
        
        return incidents, refreshed_at_text
    
    except Exception as e:
        log(f"Scraping error: {e}")
        import traceback
        traceback.print_exc()
        return [], None

# Track last update time
last_update: str | None = None
scrape_interval_seconds: int = SCRAPE_INTERVAL_SECONDS_DEFAULT

def process_incidents(incidents):
    """Store/update incidents in database"""
    global last_update
    
    if not incidents:
        return
    
    conn = db_connect()
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    current_hashes = set()
    
    for incident in incidents:
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
                    geo = geocode_address(incident.get('street'), incident.get('cross_streets'), incident.get('municipality'))
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
            geo = geocode_address(incident['street'], incident['cross_streets'], incident['municipality'])
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO incidents 
                    (hash, agency, time, units, description, street, cross_streets, municipality,
                     latitude, longitude, first_seen, last_seen,
                     geocode_source, geocode_quality, geocode_query, geocoded_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    now,
                    geo.get('source'),
                    geo.get('quality'),
                    geo.get('query'),
                    now,
                ))
            except sqlite3.OperationalError:
                # Older schema: insert without geocode metadata columns
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
    
    # Mark incidents no longer in feed as inactive
    cursor.execute('SELECT hash FROM incidents WHERE is_active = 1')
    active_hashes = [row[0] for row in cursor.fetchall()]
    
    for h in active_hashes:
        if h not in current_hashes:
            cursor.execute('UPDATE incidents SET is_active = 0 WHERE hash = ?', (h,))
    
    conn.commit()
    conn.close()
    last_update = datetime.now(timezone.utc).isoformat()
    meta_set('last_update', last_update)

def background_scrape():
    """Background task to scrape incidents periodically"""
    global feed_refreshed_at, last_scrape_started_at, last_scrape_finished_at

    last_scrape_started_at = datetime.now(timezone.utc).isoformat()
    meta_set('last_scrape_started_at', last_scrape_started_at)
    log(f"[{datetime.now().strftime('%H:%M:%S')}] Scraping Caddo 911...")
    incidents, refreshed_at_text = scrape_incidents()
    if refreshed_at_text:
        feed_refreshed_at = refreshed_at_text
        meta_set('feed_refreshed_at', feed_refreshed_at)
    if incidents:
        process_incidents(incidents)
        log(f"[{datetime.now().strftime('%H:%M:%S')}] Processed {len(incidents)} active incidents")
    else:
        log(f"[{datetime.now().strftime('%H:%M:%S')}] No incidents found or scraping failed")

    last_scrape_finished_at = datetime.now(timezone.utc).isoformat()
    meta_set('last_scrape_finished_at', last_scrape_finished_at)

# API Routes
@app.route('/')
def index():
    return send_from_directory('public', 'index.html')

@app.route('/healthz')
def healthz():
    return jsonify({'ok': True})

@app.route('/api/incidents/active')
def get_active_incidents():
    conn = db_connect(row_factory=True)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM incidents WHERE is_active = 1 ORDER BY time DESC')
    incidents = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(incidents)

@app.route('/api/incidents/history')
def get_history():
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    date = request.args.get('date')  # YYYY-MM-DD format
    
    all_incidents: list[dict] = []
    total = 0
    
    # Query main database
    conn = db_connect(row_factory=True)
    cursor = conn.cursor()
    
    if date:
        bounds = _central_date_bounds_utc(date)
        if bounds:
            start_utc, end_utc = bounds
            cursor.execute(
                'SELECT * FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ? ORDER BY first_seen DESC',
                (start_utc, end_utc)
            )
            all_incidents.extend([dict(row) for row in cursor.fetchall()])
            cursor.execute(
                'SELECT COUNT(*) as count FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ?',
                (start_utc, end_utc)
            )
            total += cursor.fetchone()['count']
    else:
        cursor.execute('SELECT * FROM incidents WHERE is_active = 0 ORDER BY first_seen DESC')
        all_incidents.extend([dict(row) for row in cursor.fetchall()])
        cursor.execute('SELECT COUNT(*) as count FROM incidents WHERE is_active = 0')
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
                    archive_cursor.execute(
                        'SELECT * FROM incidents WHERE first_seen >= ? AND first_seen < ? ORDER BY first_seen DESC',
                        (start_utc, end_utc)
                    )
                    all_incidents.extend([dict(row) for row in archive_cursor.fetchall()])
                    archive_cursor.execute(
                        'SELECT COUNT(*) as count FROM incidents WHERE first_seen >= ? AND first_seen < ?',
                        (start_utc, end_utc)
                    )
                    total += archive_cursor.fetchone()['count']
                archive_conn.close()
            except Exception as e:
                log(f"[ARCHIVE] Error reading {archive_path}: {e}")
    
    # Sort all incidents by first_seen descending, then apply pagination
    all_incidents.sort(key=lambda x: x.get('first_seen') or '', reverse=True)
    paginated = all_incidents[offset:offset + limit]
    
    return jsonify({'incidents': paginated, 'total': total})

@app.route('/api/incidents/history_counts')
def get_history_counts():
    month = request.args.get('month')  # YYYY-MM format

    if not month or len(month) != 7:
        return jsonify({'error': 'month is required (YYYY-MM)'}), 400

    counts: dict[str, int] = {}
    bounds = _central_month_bounds_utc(month)
    
    # Query main database
    conn = db_connect(row_factory=True)
    cursor = conn.cursor()
    if bounds:
        start_utc, end_utc = bounds
        cursor.execute(
            'SELECT first_seen FROM incidents WHERE is_active = 0 AND first_seen >= ? AND first_seen < ?',
            (start_utc, end_utc)
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

    return jsonify({
        # lastUpdate: UTC ISO timestamp of when we last processed/saved a scrape
        'lastUpdate': meta.get('last_update') or last_update,
        # feedRefreshedAt: the website's own "Refreshed at: ..." text (local to Caddo911)
        'feedRefreshedAt': meta.get('feed_refreshed_at') or feed_refreshed_at,
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
        incidents, refreshed_at_text = scrape_incidents()
        if refreshed_at_text:
            feed_refreshed_at = refreshed_at_text
            meta_set('feed_refreshed_at', feed_refreshed_at)
        process_incidents(incidents)
        last_scrape_finished_at = datetime.now(timezone.utc).isoformat()
        meta_set('last_scrape_finished_at', last_scrape_finished_at)
        return jsonify({'success': True, 'count': len(incidents), 'feedRefreshedAt': feed_refreshed_at})
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


def start_collector(*, interval_seconds: int = SCRAPE_INTERVAL_SECONDS_DEFAULT, initial_scrape: bool = True, enable_archive: bool = True) -> BackgroundScheduler:
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

def run_interactive_mode(*, host: str = '0.0.0.0', port: int = 3911, scrape_interval_seconds: int = 60, enable_archive: bool = True) -> None:
    """
    Start in 'event gather' mode (collector only), and allow starting the web UI on demand.
    Press '2' to start the web UI. Press 'q' to quit.
    """
    scheduler = start_collector(interval_seconds=scrape_interval_seconds, initial_scrape=True, enable_archive=enable_archive)
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

def run_gather_mode(*, scrape_interval_seconds: int = 60, enable_archive: bool = True) -> None:
    """Collector-only mode. No web server, just keeps filling the DB."""
    scheduler = start_collector(interval_seconds=scrape_interval_seconds, initial_scrape=True, enable_archive=enable_archive)
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
            geo = geocode_address(street, cross_streets, municipality)
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

    # Store archive preference for start_collector
    enable_archive = not args.no_auto_archive

    if args.mode == "gather":
        run_gather_mode(scrape_interval_seconds=args.interval, enable_archive=enable_archive)
        return

    if args.mode == "interactive":
        run_interactive_mode(host=args.host, port=args.port, scrape_interval_seconds=args.interval, enable_archive=enable_archive)
        return

    # serve mode: collector + web UI immediately
    scheduler = start_collector(interval_seconds=args.interval, initial_scrape=True, enable_archive=enable_archive)
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
