# Caddo 911 Live Feed

Real-time 911 incident tracker for Caddo Parish, Louisiana with interactive map visualization.

![Dashboard](https://img.shields.io/badge/Status-Live-red) ![Python](https://img.shields.io/badge/Python-3.10+-blue) ![License](https://img.shields.io/badge/License-Proprietary-orange)

**Created by [Vincent Larkin](https://vincentlarkin.com)** | [LinkedIn](https://linkedin.com/in/vincentwlarkin) | [GitHub](https://github.com/vincentlarkin)

## What It Does

- **Scrapes** the official [Caddo 911 Active Events](https://ias.ecc.caddo911.com/All_ActiveEvents.aspx) page every 60 seconds
- **Displays** incidents on an interactive dark-themed map with color-coded markers
- **Filters** by agency: CAD-FD/EMS (CFD*), SHVFD (SFD*), Police (SPD), Sheriff (CSO), or All
- **Caches** incidents to SQLite for live + historical views
- **Archives** older, inactive incidents to monthly archive databases
- **Geocodes** addresses using cross-street intersections for accuracy
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
```

### Environment (optional)

- `CADDO911_DB_PATH` (default: `caddo911.db`)
- `CADDO911_ARCHIVE_DAYS` (default: `30`)
- `CADDO911_AUTH_TOKEN` or `CADDO911_AUTH_USER` + `CADDO911_AUTH_PASS`
- `CADDO911_ENABLE_REFRESH_ENDPOINT` (set to `1` to enable `/api/refresh`)

### Self-hosting (NAS / Docker)

See the GitHub wiki page: [Self-hosting](https://github.com/vincentlarkin/Caddo911-Monitor/wiki/Self-hosting)

## Wiki (in-repo)

This repo also includes wiki pages in `wiki/`:

- [Home](wiki/Home.md)
- [Behavior](wiki/Behavior.md)
- [Scraping](wiki/Scraping.md)

## How It Works

1. **Scraping**: Uses `requests` + `BeautifulSoup` to parse the ASP.NET HTML table from Caddo 911's public feed (handles cookie/session requirements)
2. **Deduplication**: Each incident gets a unique hash based on agency, time, description, and location
3. **Geocoding**: Cross streets (e.g. "BAIRD RD & SUSAN DR") are prioritized over street names for more accurate intersection placement. Uses ArcGIS first (better US coverage) and falls back to OpenStreetMap's Nominatim.
4. **Storage**: Incidents stored in `caddo911.db` (SQLite) with timestamps and status
5. **Archiving**: Inactive incidents older than `CADDO911_ARCHIVE_DAYS` move to `caddo911_archive_YYYY_MM.db`
6. **Frontend**: Single-page app with Leaflet.js map, auto-refreshes every 15 seconds

## Agency Codes

| Code | Agency |
|------|--------|
| CFD1-9 | Caddo Fire Districts |
| SFD | Shreveport Fire Department |
| SPD | Shreveport Police Department |
| CSO | Caddo Sheriff's Office |

## Data Source

All data comes from the public Caddo Parish 911 Communications District feed at:
`https://ias.ecc.caddo911.com/All_ActiveEvents.aspx`

## Files

| File | Purpose |
|------|---------|
| `app.py` | Flask server, scraper, scheduler |
| `public/index.html` | Dashboard UI with map + filters |
| `public/styles.css` | Frontend styling |
| `public/images/` | Logos and agency icons |
| `caddo911.db` | SQLite database (auto-created) |
| `caddo911_archive_YYYY_MM.db` | Monthly archive databases (auto-created) |
| `requirements.txt` | Python dependencies |

## Tips

- **Geocoding improves over time**: The app stores geocode metadata and can re-geocode low-quality points automatically as new scrapes arrive (no DB wipe required).
- **Filter incidents**: Use the filter buttons (CAD-FD/EMS, SHVFD, Police, Sheriff) to focus on specific agency types
- **Historical view**: Switch to "History" tab and select a date to browse past incidents

## License

This project is **proprietary software** owned by Vincent Larkin.

- ✅ Personal and educational use permitted
- ❌ Commercial use prohibited
- ❌ Government use prohibited without authorization
- ⚠️ Attribution required: "Created by Vincent Larkin" with a link to `vincentlarkin.com` or `github.com/vincentlarkin`

See [LICENSE](LICENSE) for full terms.
