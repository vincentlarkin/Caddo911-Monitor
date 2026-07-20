# Louisiana911 Public Incident Monitor

Citizen-built Louisiana emergency incident monitor combining official live feeds and clearly labeled delayed public datasets in one interactive map.

![Dashboard](https://img.shields.io/badge/Status-Live-red) ![Python](https://img.shields.io/badge/Python-3.10+-blue) ![License](https://img.shields.io/badge/License-Proprietary-orange)

**Created by [Vincent Larkin](https://vincentlarkin.com)** | [LinkedIn](https://linkedin.com/in/vincentwlarkin) | [GitHub](https://github.com/vincentlarkin)

## What It Does

- **Collects** official public feeds from Caddo, Baton Rouge, Lafayette, and New Orleans
- **Displays** incidents on an interactive dark-themed map with color-coded markers
- **Supports source tabs**: `All`, `Caddo`, `Baton Rouge`, `Lafayette`, and `New Orleans (Daily)` in Latest and History views
- **Documents each source** with lightweight, crawlable coverage pages for Caddo Parish, Baton Rouge, Lafayette Parish, and New Orleans
- **Groups incidents by source** in `All` mode (not interleaved)
- **Filters** by agency and urgency/severity
- **Caches** incidents to SQLite for live + historical views
- **Archives** older, inactive incidents to monthly archive databases
- **Geocodes** addresses using source-aware bounds for better placement
- **Serves** a single-page frontend from `public/` (Leaflet map + filters)

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
# re-geocode all incidents with the current validation algorithm
python app.py --regeocode

# archive old incidents to monthly DBs
python app.py --archive

# create a one-time DB backup snapshot (main + archive DBs)
python app.py --backup

# backup only the main DB
python app.py --backup --backup-main-only
```

### Environment (optional)

- `LOUISIANA911_DB_PATH` (default: auto-detect `data/caddo911.db`, sibling `../data/caddo911.db`, `/data/caddo911.db`, then `caddo911.db`)
- `LOUISIANA911_DATA_DIR` (optional data directory used during DB auto-detection)
- `LOUISIANA911_ARCHIVE_DAYS` (default: `30`)
- `LOUISIANA911_BACKUP_DIR` (default: `<db dir>/backups`)
- `LOUISIANA911_BACKUP_RETENTION_WEEKS` (default: `5`, keep the most recent 5 weekly snapshots per DB)
- `LOUISIANA911_NOLA_DATASET_ID` (default: `es9j-6y5d`; update when the City publishes a new annual Calls for Service dataset)
- `LOUISIANA911_NOLA_RAW_DB_PATH` (optional annual raw-mirror path; supports `{year}`, default: `<db dir>/neworleans_calls_YYYY.db`)
- `LOUISIANA911_AUTH_TOKEN` or `LOUISIANA911_AUTH_USER` + `LOUISIANA911_AUTH_PASS`
- `LOUISIANA911_ENABLE_REFRESH_ENDPOINT` (set to `1` to enable `/api/refresh`)

Every `LOUISIANA911_*` setting also accepts its previous `CADDO911_*` name so existing deployments do not need an abrupt migration.

Automatic schedules:
- Daily archive: `3:00 AM` Central (`--no-auto-archive` to disable)
- Weekly backup snapshot: `Sunday 11:30 PM` Central (`--no-auto-backup` to disable)

### Self-hosting (NAS / Docker)

See the GitHub wiki page: [Self-hosting](https://github.com/vincentlarkin/Louisiana911/wiki/Self-hosting)

## Wiki (in-repo)

This repo also includes wiki pages in `wiki/`:

- [Home](wiki/Home.md)
- [Behavior](wiki/Behavior.md)
- [Scraping](wiki/Scraping.md)

## How It Works

1. **Collection**: Uses source adapters in `sources/` (`caddo` + `batonrouge` + `lafayette` + `neworleans`) to fetch and normalize incidents into one shared data shape. New Orleans is an official delayed daily dataset and is labeled accordingly.
2. **Deduplication**: Each incident gets a source-aware hash based on source, agency, time, description, and location.
3. **Geocoding**: Validates actual road intersections from ArcGIS provider metadata. Two cross streets are treated as endpoints bracketing the named street, and their midpoint is used when both endpoints validate. Baton Rouge rows are enriched with the City-Parish traffic map's own approximate geometry when available, with validated address-range and corridor fallbacks when that map layer lags. For New Orleans records with `POINT(0 0)`, the importer can map the published block/intersection text as an explicitly approximate location while preserving the original public label. Unverified provider guesses are not mapped.
4. **Storage**: Incidents stored in `caddo911.db` (SQLite) with source, timestamps, and active/inactive status.
5. **Archiving**: Inactive incidents older than `LOUISIANA911_ARCHIVE_DAYS` move to the existing compatible `caddo911_archive_YYYY_MM.db` files.
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
| NOPD | New Orleans Police Department calls for service |

## Data Sources

This app currently ingests from:

- Caddo Parish 911 Communications District public feed:  
  `https://ias.ecc.caddo911.com/All_ActiveEvents.aspx`
- City of Baton Rouge traffic incidents page:  
  `https://city.brla.gov/traffic/incidents.asp`
- City-Parish EBRGIS traffic-incident map layer (approximate published points): `https://maps.brla.gov/gis/rest/services/Transportation/Traffic_Incident/MapServer/0`
- Lafayette Parish traffic feed endpoint: `https://lafayette911.org/wp-json/traffic-feed/v1/data`
- City of New Orleans / NOPD Calls for Service dataset supplied by OPCD (daily):
  `https://data.nola.gov/resource/es9j-6y5d.json`

### New Orleans inclusion filter

The City dataset is much broader than a literal list of emergency calls. The regular collector imports the newest two published dates and excludes all rows marked `selfinitiated = Y` (officer-initiated activity). Preserved calendar-month history can be imported additively with `python app.py --backfill-neworleans-month YYYY-MM`; this command does not delete or replace rows outside that month.

Before a statewide launch, a complete annual source mirror plus curated History can be prepared with:

```bash
python app.py --prepare-neworleans-year 2026
```

This intentionally creates two layers:

- `neworleans_calls_2026.db` is the append-only raw source mirror. It stores every official row, including officer-initiated and display-filtered records. If Data.NOLA changes a row, the new payload becomes another preserved version; the previous payload is never replaced or deleted.
- `caddo911.db` and its existing monthly archives remain the statewide serving layer. They receive only the citizen-initiated, display-retained NOLA records so `All`, Latest, History, and reports stay simple and fast.

Use `--mirror-neworleans-year YYYY` to refresh only the raw mirror or `--backfill-neworleans-year YYYY` to rebuild only the curated History layer. Weekly backups include annual `neworleans_calls_YYYY.db` mirrors along with the main and monthly archive databases.

The following final call types are also excluded as routine/generic activity:

- `AREA CHECK`
- `BUSINESS CHECK`
- `COMPLAINT OTHER`
- `DISTURBANCE (OTHER)`
- `FUGITIVE ATTACHMENT`
- `INCIDENT REQUESTED BY ANOTHER AGENCY`
- `MEDICAL`
- `MENTAL PATIENT`
- `RETURN FOR ADDITIONAL INFO`
- `TOW IMPOUNDED VEHICLE (PRIVATE)`

Crime-preservation rule: if one of those generic final types has a non-generic `initialtypetext`, the call remains in Louisiana911 under `<initial type> (initial classification)`. This prevents a reported theft, battery, burglary, robbery, protection-order violation, or other potentially material call from disappearing merely because NOPD later moved it into a generic final category. Filtering controls what Louisiana911 imports; it does not alter the official Data.NOLA dataset.

## Files

| File | Purpose |
|------|---------|
| `app.py` | Flask server, scraper, scheduler |
| `sources/caddo.py` | Caddo source adapter |
| `sources/batonrouge.py` | Baton Rouge source adapter |
| `sources/lafayette.py` | Lafayette source adapter |
| `sources/neworleans.py` | New Orleans daily calls-for-service adapter |
| `sources/neworleans_archive.py` | Append-only annual NOLA raw mirror with payload version preservation |
| `public/index.html` | Dashboard UI with map + filters |
| `public/coverage.html` | Browseable city/parish coverage hub |
| `public/coverage-*.html` | Source-specific timing, location, and limitation pages |
| `public/styles.css` | Frontend styling |
| `public/images/` | Logos and agency icons |
| `caddo911.db` | SQLite database (auto-created) |
| `caddo911_archive_YYYY_MM.db` | Monthly archive databases (auto-created) |
| `neworleans_calls_YYYY.db` | Annual append-only raw NOLA source mirror (auto-created on demand) |
| `backups/*.db` | Weekly backup snapshots (auto-created) |
| `requirements.txt` | Python dependencies |

## Tips

- **Geocoding improves over time**: The app stores a geocoder version with each result. Active rows made by an older algorithm are re-checked automatically (no DB wipe required); use `python app.py --regeocode` to refresh inactive rows in the main database.
- **Choose source scope**: Use `All`, `Caddo`, `Baton Rouge`, `Lafayette`, or `New Orleans (Daily)` to control which feed is visible.
- **Filter incidents**: Use filter buttons to focus on agency types and urgency/severity.
- **Historical view**: Switch to "History" tab and select a date to browse past incidents

## Monthly Reporting API

The app now exposes a monthly report endpoint that summarizes:

- the most common incident type for a month
- the densest hotspot for that type within a configurable radius (default `5` miles)

Example:

```bash
curl "http://localhost:3911/api/reports/monthly?month=2026-02&radius_miles=5&source=all"
```

Example with your hosted domain:

```bash
curl "https://your-domain.example/api/reports/monthly?month=2026-02&radius_miles=5&source=caddo"
```

Response highlights:

- `totalIncidents`: all incidents seen in that month
- `topIncidentType.description`: most common call/incident description
- `topIncidentType.count`: how many times it occurred
- `topIncidentType.hotspot.incidentCount`: how many of that type fell inside the strongest radius cluster
- `topIncidentType.hotspot.center`: approximate center point for the hotspot
- `topIncidentType.hotspot.topLocations`: most common nearby street/intersection labels in that hotspot

## License

This project is **proprietary software** owned by Vincent Larkin.

- ✅ Personal and educational use permitted
- ❌ Commercial use prohibited
- ❌ Government use prohibited without authorization
- ⚠️ Attribution required: "Created by Vincent Larkin" with a link to `vincentlarkin.com` or `github.com/vincentlarkin`

See [LICENSE](LICENSE) for full terms.
