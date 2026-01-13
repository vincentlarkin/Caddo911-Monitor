# Scraping

This page documents how the app collects active incidents from the public Caddo 911 feed.

## Data source

The app scrapes the public “All Active Events” page:

`https://ias.ecc.caddo911.com/All_ActiveEvents.aspx`

## Why a session is required (ASP.NET cookie flow)

The site is an ASP.NET app and may redirect through cookie-detection (ex: `AspxAutoDetectCookieSupport`).
To reliably fetch the HTML, the scraper:

- Creates a `requests.Session()` to persist cookies
- Performs an initial GET to establish the session cookie
- If redirected to cookie-detection, re-requests with `?AspxAutoDetectCookieSupport=1`

This behavior lives in `app.py` in `scrape_incidents()`.

## Parsing the HTML

The scraper uses BeautifulSoup to:

- Scan page text for the site-provided banner timestamp: **“Refreshed at:”**
- Find `<table>` elements and iterate rows (`<tr>`)
- Extract columns (`<td>`) into fields:
  - `agency`, `time`, `units`, `description`, `street`, `cross_streets`, `municipality`

### Basic validation / filtering

To avoid picking up layout tables, rows are only treated as incidents when:

- `agency` is present and short (feed codes like `SPD`, `CSO`, `CFD1`, `SFD`, etc.)
- `time` is numeric and small (3–4 digits, ex: `930`, `2145`)
- `description` is present

## Storage + deduplication

Each incident is given a stable hash based on key fields, and stored in SQLite:

- New incidents are inserted with `first_seen` / `last_seen`
- Existing incidents are marked active and get `last_seen` updated
- Incidents that disappear from the live feed are marked `is_active = 0`

## Scheduling

In “serve” and “gather” modes, the scraper runs in a background scheduler:

- `BackgroundScheduler` runs `background_scrape()` at a fixed interval (default 60s)
- Each scrape updates status metadata (start/finish timestamps) and persists the data

The frontend polls the API separately (every ~15s) and renders whatever the latest DB state is.

