# Caddo 911 Live Feed (with Lafayette Beta)

Real-time incident tracker for Caddo Parish and Lafayette traffic incidents with an interactive map and live/history views.

![Dashboard](https://img.shields.io/badge/Status-Live-red) ![Python](https://img.shields.io/badge/Python-3.10+-blue) ![License](https://img.shields.io/badge/License-Proprietary-orange)

**Created by [Vincent Larkin](https://vincentlarkin.com)** | [LinkedIn](https://linkedin.com/in/vincentwlarkin) | [GitHub](https://github.com/vincentlarkin)

## What It Does

- **Scrapes** the official [Caddo 911 Active Events](https://ias.ecc.caddo911.com/All_ActiveEvents.aspx) feed and Lafayette's traffic feed every cycle
- **Displays** incidents on an interactive dark-themed map with color-coded markers
- **Supports source tabs**: `All`, `Caddo`, and `Lafayette (Beta)` in both Live and History views
- **Groups incidents by source** in `All` mode (not interleaved)
- **Filters** by agency and urgency/severity
- **Caches** incidents to SQLite for live + historical views
- **Archives** older, inactive incidents to monthly archive databases
- **Geocodes** addresses using source-aware bounds for better placement
- **Serves** a single-page frontend from `public/` (Leaflet map + filters)

## Requirements

- Python 3.10+
- Internet connection

## Installation

```bash
pip install -r requirements.txt
```

## Running

```bash
python app.py
```

Then open **http://localhost:3911** in your browser.

### Run Modes

This app supports multiple modes depending on what you want to do.

#### Web dashboard (collector + web UI)

```bash
python app.py
# or explicitly:
python app.py --mode serve
```

#### Event gather mode (collector only, no web UI)

```bash
python app.py --mode gather
```

#### Interactive gather mode (press `2` to start the web UI)

```bash
python app.py --mode interactive
```

While running interactive mode:
- Press `2` to start the web UI
- Press `q` to quit

#### Options

```bash
# scrape interval (seconds)
python app.py --mode gather --interval 60

# quiet console output (recommended for gather mode)
python app.py --mode gather --quiet
```

#### Maintenance commands

```bash
# re-geocode all incidents (improves old coordinates)
python app.py --regeocode

# archive old incidents to monthly DBs
python app.py --archive

# create a one-time DB backup snapshot (main + archive DBs)
python app.py --backup

# backup only the main DB
python app.py --backup --backup-main-only
```

### Environment (optional)

- `CADDO911_DB_PATH` (default: `caddo911.db`)
- `CADDO911_ARCHIVE_DAYS` (default: `30`)
- `CADDO911_BACKUP_DIR` (default: `<db dir>/backups`)
- `CADDO911_BACKUP_RETENTION_WEEKS` (default: `5`, keep the most recent 5 weekly snapshots per DB)
- `CADDO911_AUTH_TOKEN` or `CADDO911_AUTH_USER` + `CADDO911_AUTH_PASS`
- `CADDO911_ENABLE_REFRESH_ENDPOINT` (set to `1` to enable `/api/refresh`)

Automatic schedules:
- Daily archive: `3:00 AM` Central (`--no-auto-archive` to disable)
- Weekly backup snapshot: `Sunday 11:30 PM` Central (`--no-auto-backup` to disable)

### Self-hosting (NAS / Docker)

See the GitHub wiki page: [Self-hosting](https://github.com/vincentlarkin/Caddo911-Monitor/wiki/Self-hosting)

## Wiki (in-repo)

This repo also includes wiki pages in `wiki/`:

- [Home](wiki/Home.md)
- [Behavior](wiki/Behavior.md)
- [Scraping](wiki/Scraping.md)

## How It Works

1. **Scraping**: Uses source adapters in `sources/` (`caddo` + `lafayette`) to fetch and normalize incidents into one shared data shape.
2. **Deduplication**: Each incident gets a source-aware hash based on source, agency, time, description, and location.
3. **Geocoding**: Cross streets are prioritized over street names for more accurate intersection placement. Uses ArcGIS first and falls back to OpenStreetMap's Nominatim.
4. **Storage**: Incidents stored in `caddo911.db` (SQLite) with source, timestamps, and active/inactive status.
5. **Archiving**: Inactive incidents older than `CADDO911_ARCHIVE_DAYS` move to `caddo911_archive_YYYY_MM.db`.
6. **Backup snapshots**: Weekly SQLite-consistent snapshots are written to `backups/` (configurable).
7. **Frontend**: Single-page app with Leaflet.js map, source tabs, and shared filters for Live + History views.

## Agency Labels

| Code | Agency |
|------|--------|
| CFD1-9 | Caddo Fire Districts |
| SFD | Shreveport Fire Department |
| SPD | Shreveport Police Department |
| CSO | Caddo Sheriff's Office |
| POLICE | Lafayette Police label |
| SHERIFF | Lafayette Sheriff label |
| FIRE | Lafayette Fire label |

## Data Sources

This app currently ingests from:

- Caddo Parish 911 Communications District public feed:  
  `https://ias.ecc.caddo911.com/All_ActiveEvents.aspx`
- Lafayette Parish traffic feed endpoint (beta integration):  
  `https://lafayette911.org/wp-json/traffic-feed/v1/data`

## Files

| File | Purpose |
|------|---------|
| `app.py` | Flask server, scraper, scheduler |
| `sources/caddo.py` | Caddo source adapter |
| `sources/lafayette.py` | Lafayette source adapter |
| `public/index.html` | Dashboard UI with map + filters |
| `public/styles.css` | Frontend styling |
| `public/images/` | Logos and agency icons |
| `caddo911.db` | SQLite database (auto-created) |
| `caddo911_archive_YYYY_MM.db` | Monthly archive databases (auto-created) |
| `backups/*.db` | Weekly backup snapshots (auto-created) |
| `requirements.txt` | Python dependencies |

## Tips

- **Geocoding improves over time**: The app stores geocode metadata and can re-geocode low-quality points automatically as new scrapes arrive (no DB wipe required).
- **Choose source scope**: Use `All`, `Caddo`, or `Lafayette (Beta)` to control which feed is visible.
- **Filter incidents**: Use filter buttons to focus on agency types and urgency/severity.
- **Historical view**: Switch to "History" tab and select a date to browse past incidents

## License

This project is **proprietary software** owned by Vincent Larkin.

- ✅ Personal and educational use permitted
- ❌ Commercial use prohibited
- ❌ Government use prohibited without authorization
- ⚠️ Attribution required: "Created by Vincent Larkin" with a link to `vincentlarkin.com` or `github.com/vincentlarkin`

See [LICENSE](LICENSE) for full terms.
