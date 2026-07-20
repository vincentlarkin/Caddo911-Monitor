# Louisiana911 Public Incident Monitor

[Louisiana911.com](https://louisiana911.com/) presents public incident information from participating Louisiana sources in one map and list.

Louisiana911 is not an emergency service. Call **911** for emergencies and follow instructions from local authorities.

## About

The site provides:

- Latest published incidents
- Date-based history
- Map and list views
- Source, severity, and agency filters
- Monthly summaries
- Mobile-friendly incident details

Locations are shown only when a usable public location can be validated. Map triangles communicate an area rather than a guaranteed pinpoint location.

## Coverage

Current sources include:

- [Caddo Parish 911 Communications District](https://ias.ecc.caddo911.com/All_ActiveEvents.aspx)
- [City of Baton Rouge traffic incidents](https://city.brla.gov/traffic/incidents.asp)
- [Lafayette Parish traffic feed](https://lafayette911.org/wp-json/traffic-feed/v1/data)
- [City of New Orleans / NOPD Calls for Service](https://data.nola.gov/resource/es9j-6y5d.json)

New Orleans information is published as a delayed daily record rather than a live feed. Source agencies may update, reclassify, delay, or remove records. Some locations are approximate, generalized, or unavailable.

## Map Guide

- **Red** — higher-severity incidents
- **Yellow** — medium-severity incidents
- **Blue** — lower-severity and public-service incidents
- **Striped purple** — medical or EMS incidents
- **Triangle size** — broader triangles indicate less precise placement

Colors are a visual organization aid, not an official emergency classification.

## Important Limits

Louisiana911 reflects what each public source publishes. It is not a complete record of emergency activity, dispatch operations, response status, or final outcomes. Times, descriptions, units, and locations can change at the source.

## Project Layout

- `sources/` — source adapters and normalization
- `public/` — site interface and coverage pages
- `tests/` — behavior and source checks
- `wiki/` — brief project notes

## Wiki

- [Behavior](wiki/Behavior.md)
- [Data sources](wiki/Data-Sources.md)

## License

See [LICENSE](LICENSE).
