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

# Scraper/status metadata
SCRAPE_INTERVAL_SECONDS_DEFAULT = 60
feed_refreshed_at: str | None = None  # e.g. "January 13 09:56"
last_scrape_started_at: str | None = None  # ISO UTC
last_scrape_finished_at: str | None = None  # ISO UTC

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

def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        # We store timezone-aware ISO timestamps (UTC) via datetime.now(timezone.utc).isoformat()
        return datetime.fromisoformat(value)
    except Exception:
        return None

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
geolocator_osm = Nominatim(user_agent="caddo911_tracker", timeout=5)
geocode_cache = {}

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
    Uses up to TWO cross streets (when available) and handles '@' formatting in the street field.
    Tries ArcGIS first (better US coverage), then Nominatim.
    """
    import random
    
    city = _clean_ws(municipality or "") or 'Shreveport'
    state = 'LA'

    street_clean, crosses = _extract_street_and_crosses(street, cross_streets)
    cross1 = crosses[0] if len(crosses) > 0 else None
    cross2 = crosses[1] if len(crosses) > 1 else None

    # Cache key (include both crosses + city so we don't collide on common names like "Dee St")
    cache_key = f"{street_clean or ''}|{cross1 or ''}|{cross2 or ''}|{city}"
    if cache_key in geocode_cache:
        return geocode_cache[cache_key]

    # Build queries optimized for intersection lookup.
    # Priority:
    # - cross1 & cross2 (best when there is no main street, or when street field contains multiple streets)
    # - street & cross1 / street & cross2
    # - street only / cross only
    candidates: list[tuple[str, str]] = []
    if cross1 and cross2:
        candidates.append((f"{cross1} & {cross2}, {city}, {state}", "intersection-2"))
        candidates.append((f"{cross1} and {cross2}, {city}, {state}", "intersection-2"))
    if street_clean and cross1:
        candidates.append((f"{street_clean} & {cross1}, {city}, {state}", "street+cross"))
        candidates.append((f"{street_clean} and {cross1}, {city}, {state}", "street+cross"))
    if street_clean and cross2:
        candidates.append((f"{street_clean} & {cross2}, {city}, {state}", "street+cross"))
    if street_clean:
        candidates.append((f"{street_clean}, {city}, {state}", "street-only"))
    if cross1:
        candidates.append((f"{cross1}, {city}, {state}", "cross-only"))
    candidates.append((f"{city}, {state}", "city-only"))

    # De-dupe queries while preserving quality tag for each query string
    deduped: list[tuple[str, str]] = []
    seen_q: set[str] = set()
    for q, quality in candidates:
        qn = _clean_ws(q)
        if not qn or qn.lower() in seen_q:
            continue
        seen_q.add(qn.lower())
        deduped.append((qn, quality))
    
    # Try ArcGIS first (better for US addresses)
    for attempt_query, quality in deduped[:4]:
        try:
            time.sleep(0.1)
            location = geolocator_arcgis.geocode(attempt_query, timeout=4)
            if location and 32.0 < location.latitude < 33.2 and -94.2 < location.longitude < -93.3:
                result = {
                    'lat': location.latitude,
                    'lng': location.longitude,
                    'source': 'arcgis',
                    'quality': quality,
                    'query': attempt_query,
                }
                geocode_cache[cache_key] = result
                log(f"  [ARC] {quality} | {attempt_query} -> ({location.latitude:.5f}, {location.longitude:.5f})")
                return result
        except Exception:
            pass
    
    # Fallback to Nominatim/OSM
    for attempt_query, quality in deduped[:4]:
        try:
            time.sleep(0.1)
            location = geolocator_osm.geocode(attempt_query, country_codes='us', exactly_one=True, timeout=3)
            if location and 32.0 < location.latitude < 33.2 and -94.2 < location.longitude < -93.3:
                result = {
                    'lat': location.latitude,
                    'lng': location.longitude,
                    'source': 'osm',
                    'quality': quality,
                    'query': attempt_query,
                }
                geocode_cache[cache_key] = result
                log(f"  [OSM] {quality} | {attempt_query} -> ({location.latitude:.5f}, {location.longitude:.5f})")
                return result
        except Exception:
            pass
    
    log(f"  [--] fallback | {street_clean or '?'} @ {cross1 or '?'} {'& ' + cross2 if cross2 else ''}")
    
    # Fallback to Shreveport area
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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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
                low_quality = (existing_source in (None, "fallback")) or (existing_quality in (None, "fallback", "city-only", "cross-only"))
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
    
    conn = db_connect(row_factory=True)
    cursor = conn.cursor()
    
    # History should not include incidents that are currently active on Caddo911.
    # Only return cleared/inactive incidents.
    if date:
        cursor.execute(
            'SELECT * FROM incidents WHERE is_active = 0 AND DATE(first_seen) = ? ORDER BY first_seen DESC LIMIT ? OFFSET ?',
            (date, limit, offset)
        )
    else:
        cursor.execute(
            'SELECT * FROM incidents WHERE is_active = 0 ORDER BY first_seen DESC LIMIT ? OFFSET ?',
            (limit, offset)
        )
    
    incidents = [dict(row) for row in cursor.fetchall()]
    
    if date:
        cursor.execute('SELECT COUNT(*) as count FROM incidents WHERE is_active = 0 AND DATE(first_seen) = ?', (date,))
    else:
        cursor.execute('SELECT COUNT(*) as count FROM incidents WHERE is_active = 0')
    total = cursor.fetchone()['count']
    
    conn.close()
    return jsonify({'incidents': incidents, 'total': total})

@app.route('/api/incidents/history_counts')
def get_history_counts():
    month = request.args.get('month')  # YYYY-MM format

    if not month or len(month) != 7:
        return jsonify({'error': 'month is required (YYYY-MM)'}), 400

    conn = db_connect(row_factory=True)
    cursor = conn.cursor()
    cursor.execute(
        '''
        SELECT DATE(first_seen) as day, COUNT(*) as count
        FROM incidents
        WHERE is_active = 0 AND strftime('%Y-%m', first_seen) = ?
        GROUP BY day
        ''',
        (month,)
    )
    rows = cursor.fetchall()
    conn.close()

    counts = {row['day']: row['count'] for row in rows}
    return jsonify({'counts': counts})

@app.route('/api/stats')
def get_stats():
    conn = db_connect(row_factory=True)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) as count FROM incidents WHERE is_active = 1')
    active = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(*) as count FROM incidents WHERE DATE(first_seen) = DATE('now')")
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

def start_collector(*, interval_seconds: int = SCRAPE_INTERVAL_SECONDS_DEFAULT, initial_scrape: bool = True) -> BackgroundScheduler:
    """Start background scraping + DB persistence, return the scheduler."""
    global scrape_interval_seconds
    scrape_interval_seconds = int(interval_seconds)
    meta_set('scrape_interval_seconds', str(scrape_interval_seconds))
    init_db()
    if initial_scrape:
        background_scrape()

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        background_scrape,
        'interval',
        seconds=int(scrape_interval_seconds),
        max_instances=1,
        coalesce=True,
    )
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

def run_interactive_mode(*, host: str = '0.0.0.0', port: int = 3911, scrape_interval_seconds: int = 60) -> None:
    """
    Start in 'event gather' mode (collector only), and allow starting the web UI on demand.
    Press '2' to start the web UI. Press 'q' to quit.
    """
    scheduler = start_collector(interval_seconds=scrape_interval_seconds, initial_scrape=True)
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

def run_gather_mode(*, scrape_interval_seconds: int = 60) -> None:
    """Collector-only mode. No web server, just keeps filling the DB."""
    scheduler = start_collector(interval_seconds=scrape_interval_seconds, initial_scrape=True)
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

def main() -> None:
    parser = argparse.ArgumentParser(description="Caddo 911 Live Feed")
    parser.add_argument("--mode", choices=["serve", "gather", "interactive"], default="serve",
                        help="serve: collector + web UI (default); gather: collector only; interactive: collector only, press 2 to start UI")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=3911)
    parser.add_argument("--interval", type=int, default=60, help="scrape interval in seconds")
    parser.add_argument("--quiet", action="store_true", help="reduce console output (recommended for gather mode)")
    args = parser.parse_args()

    global QUIET
    QUIET = bool(args.quiet or args.mode == "gather")

    if args.mode == "gather":
        run_gather_mode(scrape_interval_seconds=args.interval)
        return

    if args.mode == "interactive":
        run_interactive_mode(host=args.host, port=args.port, scrape_interval_seconds=args.interval)
        return

    # serve mode: collector + web UI immediately
    scheduler = start_collector(interval_seconds=args.interval, initial_scrape=True)
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
