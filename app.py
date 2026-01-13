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
from datetime import datetime, timezone
from threading import Thread
from flask import Flask, jsonify, request, send_from_directory
import requests
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__, static_folder='public', static_url_path='')

# Database setup
DB_PATH = 'caddo911.db'

QUIET = False

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
    conn.commit()
    conn.close()

# Geocoder setup with caching - try ArcGIS first (better US coverage), fallback to Nominatim
from geopy.geocoders import ArcGIS
geolocator_arcgis = ArcGIS(timeout=5)
geolocator_osm = Nominatim(user_agent="caddo911_tracker", timeout=5)
geocode_cache = {}

def geocode_address(street, cross_streets, municipality):
    """
    Convert address to lat/lng coordinates.
    Uses street + first cross street as intersection.
    Tries ArcGIS first (better US coverage), then Nominatim.
    """
    import random
    
    city = 'Shreveport'
    state = 'LA'
    
    # Parse cross streets - get FIRST real one
    first_cross = None
    if cross_streets and cross_streets.strip():
        parts = [p.strip() for p in cross_streets.split('&')]
        for part in parts:
            if part and part.upper() not in ['DEAD END', '']:
                first_cross = part
                break
    
    street_clean = street.strip() if street else None
    
    # Cache key
    cache_key = f"{street_clean}|{first_cross}"
    if cache_key in geocode_cache:
        return geocode_cache[cache_key]
    
    # Build queries optimized for intersection lookup
    queries = []
    if street_clean and first_cross:
        # ArcGIS format: "Street1 & Street2, City, State"
        queries.append(f"{street_clean} & {first_cross}, {city}, {state}")
        queries.append(f"{street_clean} St & {first_cross}, {city}, {state}")
        queries.append(f"{street_clean} and {first_cross}, {city}, {state}")
    elif first_cross:
        queries.append(f"{first_cross}, {city}, {state}")
    elif street_clean:
        queries.append(f"{street_clean}, {city}, {state}")
        queries.append(f"{street_clean} St, {city}, {state}")
    
    if not queries:
        queries.append(f"{city}, {state}")
    
    # Try ArcGIS first (better for US addresses)
    for attempt_query in queries[:2]:
        try:
            time.sleep(0.1)
            location = geolocator_arcgis.geocode(attempt_query, timeout=4)
            if location and 32.0 < location.latitude < 33.2 and -94.2 < location.longitude < -93.3:
                result = {'lat': location.latitude, 'lng': location.longitude}
                geocode_cache[cache_key] = result
                log(f"  [ARC] {attempt_query} -> ({location.latitude:.5f}, {location.longitude:.5f})")
                return result
        except Exception:
            pass
    
    # Fallback to Nominatim/OSM
    for attempt_query in queries[:2]:
        try:
            time.sleep(0.1)
            location = geolocator_osm.geocode(attempt_query, country_codes='us', exactly_one=True, timeout=3)
            if location and 32.0 < location.latitude < 33.2 and -94.2 < location.longitude < -93.3:
                result = {'lat': location.latitude, 'lng': location.longitude}
                geocode_cache[cache_key] = result
                log(f"  [OSM] {attempt_query} -> ({location.latitude:.5f}, {location.longitude:.5f})")
                return result
        except Exception:
            pass
    
    log(f"  [--] {street_clean or '?'} @ {first_cross or '?'}")
    
    # Fallback to Shreveport area
    offset = lambda: (random.random() - 0.5) * 0.04
    default = {'lat': 32.47 + offset(), 'lng': -93.79 + offset()}
    geocode_cache[cache_key] = default
    return default

def hash_incident(incident):
    """Generate unique hash for incident deduplication"""
    key = f"{incident['agency']}-{incident['time']}-{incident['description']}-{incident['street']}-{incident['cross_streets']}"
    return hashlib.md5(key.encode()).hexdigest()

def scrape_incidents():
    """Scrape active incidents from Caddo 911 website (ASP.NET site with cookie requirement)"""
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
        
        return incidents
    
    except Exception as e:
        print(f"Scraping error: {e}")
        import traceback
        traceback.print_exc()
        return []

# Track last update time
last_update = None

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
        cursor.execute('SELECT id, latitude, longitude FROM incidents WHERE hash = ?', (h,))
        existing = cursor.fetchone()
        
        if existing:
            # Update last_seen
            cursor.execute('UPDATE incidents SET last_seen = ?, is_active = 1 WHERE hash = ?', (now, h))
        else:
            # New incident - geocode and insert
            coords = geocode_address(incident['street'], incident['cross_streets'], incident['municipality'])
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
                coords['lat'],
                coords['lng'],
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

def background_scrape():
    """Background task to scrape incidents periodically"""
    log(f"[{datetime.now().strftime('%H:%M:%S')}] Scraping Caddo 911...")
    incidents = scrape_incidents()
    if incidents:
        process_incidents(incidents)
        log(f"[{datetime.now().strftime('%H:%M:%S')}] Processed {len(incidents)} active incidents")
    else:
        log(f"[{datetime.now().strftime('%H:%M:%S')}] No incidents found or scraping failed")

# API Routes
@app.route('/')
def index():
    return send_from_directory('public', 'index.html')

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
    return jsonify({
        'lastUpdate': last_update,
        'scrapeInterval': 60000
    })

@app.route('/api/refresh', methods=['POST'])
def force_refresh():
    try:
        incidents = scrape_incidents()
        process_incidents(incidents)
        return jsonify({'success': True, 'count': len(incidents)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def start_collector(*, scrape_interval_seconds: int = 60, initial_scrape: bool = True) -> BackgroundScheduler:
    """Start background scraping + DB persistence, return the scheduler."""
    init_db()
    if initial_scrape:
        background_scrape()

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        background_scrape,
        'interval',
        seconds=scrape_interval_seconds,
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
    scheduler = start_collector(scrape_interval_seconds=scrape_interval_seconds, initial_scrape=True)
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
    scheduler = start_collector(scrape_interval_seconds=scrape_interval_seconds, initial_scrape=True)
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
    scheduler = start_collector(scrape_interval_seconds=args.interval, initial_scrape=True)
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
