# Behavior

This page documents user-visible runtime behavior of the dashboard and data pipeline.

## Map markers are triangles (not pinpoint dots)

To avoid implying “pinpoint precision” for geocoded incidents, the map does **not** draw a single exact dot.
Instead, each incident is rendered as a **small semi-transparent triangle** (Leaflet polygon) centered on the incident’s stored latitude/longitude.

- **Why triangles**: geocoding is an estimate; a small area communicates “general vicinity” better than a dot.
- **Deterministic rotation**: the triangle is rotated deterministically per incident so it doesn’t “wiggle” on refresh.

### Triangle size scales by geocode confidence

Triangle size is based on the incident’s stored geocoding metadata:

- **Smaller triangles**: clean intersection geocodes (ex: `geocode_quality = intersection-2`)
- **Larger triangles**: lower-confidence geocodes (ex: `fallback`, `city-only`, `cross-only`, `street-only`)

This makes uncertain placements visually less precise while keeping high-confidence placements tight.

## Geocoding is intersection-first and self-healing (no DB wipe required)

Incidents are geocoded using an intersection-first strategy:

- If two cross streets exist, try: `cross1 & cross2, City, State`
- Else try: `street & cross, City, State`
- Else fall back to `street, City, State` (and other safe fallbacks)

The app stores geocoding metadata on each incident:

- `geocode_source`: `arcgis` | `osm` | `fallback`
- `geocode_quality`: `intersection-2` | `street+cross` | `street-only` | `cross-only` | `city-only` | `fallback`
- `geocode_query`: the actual provider query used
- `geocoded_at`: when the geocode was produced (UTC)

### Existing DB rows can improve over time

When an incident already exists, the app can **re-geocode** it if the previous result was low-quality (ex: fallback/city-only/cross-only) and the improved result is meaningfully different.

This lets you keep your existing `caddo911.db` while improving “bad” points as new scrapes arrive.

