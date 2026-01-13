# Caddo 911 Live Feed

Real-time 911 incident tracker for Caddo Parish, Louisiana with interactive map visualization.

![Dashboard](https://img.shields.io/badge/Status-Live-red) ![Python](https://img.shields.io/badge/Python-3.10+-blue) ![License](https://img.shields.io/badge/License-Proprietary-orange)

**Created by [Vincent Larkin](https://vincentlarkin.com)** | [LinkedIn](https://linkedin.com/in/vincentwlarkin) | [GitHub](https://github.com/vincentlarkin)

## What It Does

- **Scrapes** the official [Caddo 911 Active Events](https://ias.ecc.caddo911.com/All_ActiveEvents.aspx) page every 60 seconds
- **Displays** incidents on an interactive dark-themed map with color-coded markers
- **Filters** by agency: CAD-FD/EMS (CFD*), SHVFD (SFD*), Police (SPD), Sheriff (CSO), or All
- **Caches** all incidents to SQLite database for historical viewing
- **Geocodes** addresses using cross-street intersections for accuracy

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

### Self-hosting (NAS / Docker)

See the GitHub wiki page: [Self-hosting](https://github.com/vincentlarkin/Caddo911-Monitor/wiki/Self-hosting)

## How It Works

1. **Scraping**: Uses `requests` + `BeautifulSoup` to parse the ASP.NET HTML table from Caddo 911's public feed (handles cookie/session requirements)
2. **Deduplication**: Each incident gets a unique hash based on agency, time, description, and location
3. **Geocoding**: Cross streets (e.g. "BAIRD RD & SUSAN DR") are prioritized over street names for more accurate intersection placement. Uses ArcGIS first (better US coverage) and falls back to OpenStreetMap's Nominatim.
4. **Storage**: All incidents stored in `caddo911.db` (SQLite) with timestamps - tracks when incidents appear and disappear
5. **Frontend**: Single-page app with Leaflet.js map, auto-refreshes every 15 seconds

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
| `caddo911.db` | SQLite database (auto-created) |
| `requirements.txt` | Python dependencies |

## Tips

- **Clear cache for fresh geocoding**: Delete `caddo911.db` and restart to re-geocode all incidents
- **Filter incidents**: Use the filter buttons (CAD-FD/EMS, SHVFD, Police, Sheriff) to focus on specific agency types
- **Historical view**: Switch to "History" tab and select a date to browse past incidents

## License

This project is **proprietary software** owned by Vincent Larkin.

- ✅ Personal and educational use permitted
- ❌ Commercial use prohibited
- ❌ Government use prohibited without authorization
- ⚠️ Attribution required: "Created by Vincent Larkin"

See [LICENSE](LICENSE) for full terms.
