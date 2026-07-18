"""New Orleans Police Department daily calls-for-service adapter."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import math
import os
import re
from zoneinfo import ZoneInfo

import requests


DATASET_ID = os.environ.get("LOUISIANA911_NOLA_DATASET_ID", "es9j-6y5d").strip() or "es9j-6y5d"
FEED_URL = f"https://data.nola.gov/resource/{DATASET_ID}.json"
CENTRAL_TZ = ZoneInfo("America/Chicago")
RECENT_ACTIVE_LIMIT = 150
EXCLUDED_GENERIC_CALL_TYPES = frozenset(
    {
        "AREA CHECK",
        "BUSINESS CHECK",
        "COMPLAINT OTHER",
        "DISTURBANCE (OTHER)",
        "FUGITIVE ATTACHMENT",
        "INCIDENT REQUESTED BY ANOTHER AGENCY",
        "MEDICAL",
        "MENTAL PATIENT",
        "RETURN FOR ADDITIONAL INFO",
        "TOW IMPOUNDED VEHICLE (PRIVATE)",
    }
)
APPROX_LOCATION_RE = re.compile(
    r"^\s*approx(?:imate)?\s+loc(?:ation)?\s*:\s*",
    flags=re.IGNORECASE,
)


def _clean_ws(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _normalize_call_type(value: str | None) -> str:
    return _clean_ws(value).upper()


def _is_excluded_generic_row(row: dict) -> bool:
    """Drop explicit noise types unless a non-noise initial call may be material."""
    final_type = _normalize_call_type(row.get("typetext"))
    initial_type = _normalize_call_type(row.get("initialtypetext"))
    return final_type in EXCLUDED_GENERIC_CALL_TYPES and (
        not initial_type or initial_type in EXCLUDED_GENERIC_CALL_TYPES
    )


def _display_call_type(row: dict) -> str:
    """Keep potentially material initial classifications visible and honest."""
    final_type = _clean_ws(row.get("typetext"))
    initial_type = _clean_ws(row.get("initialtypetext"))
    if (
        _normalize_call_type(final_type) in EXCLUDED_GENERIC_CALL_TYPES
        and initial_type
        and _normalize_call_type(initial_type) not in EXCLUDED_GENERIC_CALL_TYPES
    ):
        return f"{initial_type} (initial classification)"
    return final_type


def _socrata_string_list(values) -> str:
    return ",".join(
        "'" + str(value).replace("'", "''") + "'"
        for value in sorted(values)
    )


def _parse_local_timestamp(value: str | None) -> datetime | None:
    text = _clean_ws(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=CENTRAL_TZ)
    return parsed


def _published_coordinates(row: dict) -> tuple[float | None, float | None]:
    location = row.get("location") if isinstance(row, dict) else None
    coordinates = location.get("coordinates") if isinstance(location, dict) else None
    if not isinstance(coordinates, (list, tuple)) or len(coordinates) < 2:
        return None, None
    try:
        longitude = float(coordinates[0])
        latitude = float(coordinates[1])
    except (TypeError, ValueError):
        return None, None
    if not (math.isfinite(latitude) and math.isfinite(longitude)):
        return None, None
    # Data.NOLA uses POINT(0 0) when a usable public point is not supplied.
    # Treat that sentinel as missing so the application can try the separately
    # published block/intersection label instead of placing it off Africa.
    if abs(latitude) < 1e-9 and abs(longitude) < 1e-9:
        return None, None
    return latitude, longitude


def _normalize_row(row: dict, *, is_recent: bool) -> dict | None:
    item_id = _clean_ws(row.get("nopd_item"))
    if _is_excluded_generic_row(row):
        return None
    description = _display_call_type(row)
    final_description = _clean_ws(row.get("typetext"))
    initial_description = _clean_ws(row.get("initialtypetext"))
    created = _parse_local_timestamp(row.get("timecreate"))
    if not item_id or not description or created is None:
        return None

    public_location = _clean_ws(row.get("block_address"))
    latitude, longitude = _published_coordinates(row)
    return {
        "source": "neworleans",
        "source_id": item_id,
        "agency": "NOPD",
        "time": created.strftime("%H%M"),
        "units": 0,
        "description": description,
        "final_description": final_description,
        "initial_description": initial_description,
        # Preserve the public label exactly for display. The application may
        # geocode that same block/intersection text when the point is 0,0, but
        # it never replaces the label with a more specific address.
        "street": public_location,
        "cross_streets": "",
        "municipality": "New Orleans",
        "latitude": latitude,
        "longitude": longitude,
        "coordinates_published": latitude is not None and longitude is not None,
        "location_is_approximate": bool(APPROX_LOCATION_RE.match(public_location)),
        "occurred_at": created.astimezone(timezone.utc).isoformat(),
        # The latest slice appears in Latest; the complete imported log remains
        # available through History after records rotate out of the recent slice.
        "is_active": bool(is_recent),
    }


FIELDS = ",".join(
    (
        "nopd_item",
        "type_",
        "typetext",
        "priority",
        "initialtype",
        "initialtypetext",
        "initialpriority",
        "timecreate",
        "timedispatch",
        "timearrive",
        "timeclosed",
        "selfinitiated",
        "block_address",
        "zip",
        "policedistrict",
        "location",
    )
)


def _material_call_clause() -> str:
    noise_values = _socrata_string_list(EXCLUDED_GENERIC_CALL_TYPES)
    generic_initial_values = f"'',{noise_values}"
    return (
        "NOT ("
        f"upper(typetext) IN ({noise_values}) AND "
        f"upper(coalesce(initialtypetext, '')) IN ({generic_initial_values})"
        ")"
    )


def _session(*, user_agent: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "application/json",
        }
    )
    return session


def _latest_published_datetime(
    session: requests.Session,
    *,
    timeout_seconds: int,
) -> datetime | None:
    response = session.get(
        FEED_URL,
        params={
            "$select": "timecreate",
            "$where": f"selfinitiated = 'N' AND {_material_call_clause()}",
            "$order": "timecreate DESC",
            "$limit": 1,
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload:
        return None
    return _parse_local_timestamp(payload[0].get("timecreate"))


def _fetch_range_rows(
    session: requests.Session,
    *,
    start_date: date,
    end_date: date,
    timeout_seconds: int,
) -> list[dict]:
    """Fetch [start_date, end_date) with pagination from the official Socrata API."""
    date_clause = (
        f"timecreate >= '{start_date.isoformat()}T00:00:00' AND "
        f"timecreate < '{end_date.isoformat()}T00:00:00'"
    )
    # Self-initiated officer activity is useful police data, but it is not a
    # public request for service and should not be presented as a 911 call.
    where = f"{date_clause} AND selfinitiated = 'N' AND {_material_call_clause()}"
    rows: list[dict] = []
    page_size = 50000
    offset = 0
    while True:
        response = session.get(
            FEED_URL,
            params={
                "$select": FIELDS,
                "$where": where,
                "$order": "timecreate DESC,nopd_item DESC",
                "$limit": page_size,
                "$offset": offset,
            },
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        page = response.json()
        rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return rows


def _normalize_rows(rows: list[dict], *, latest_date: date | None) -> list[dict]:
    incidents: list[dict] = []
    recent_count = 0
    for row in rows:
        row_created = _parse_local_timestamp(row.get("timecreate"))
        is_latest_day = bool(latest_date and row_created and row_created.date() == latest_date)
        is_recent = is_latest_day and recent_count < RECENT_ACTIVE_LIMIT
        incident = _normalize_row(row, is_recent=is_recent)
        if incident is None:
            continue
        incidents.append(incident)
        if is_recent:
            recent_count += 1
    return incidents


def scrape(*, user_agent: str, timeout_seconds: int = 30) -> tuple[list[dict], str | None]:
    """Fetch the two newest published dates of citizen-initiated NOPD calls."""
    session = _session(user_agent=user_agent)

    latest_dt = _latest_published_datetime(session, timeout_seconds=timeout_seconds)
    if latest_dt is None:
        return [], None
    latest_date = latest_dt.date()
    prior_date = latest_date - timedelta(days=1)
    rows = _fetch_range_rows(
        session,
        start_date=prior_date,
        end_date=latest_date + timedelta(days=1),
        timeout_seconds=timeout_seconds,
    )
    incidents = _normalize_rows(rows, latest_date=latest_date)

    refreshed_at = (
        f"Daily log through {latest_date.strftime('%b')} "
        f"{latest_date.day}, {latest_date.year}"
    )
    return incidents, refreshed_at


def scrape_month(
    month: str,
    *,
    user_agent: str,
    timeout_seconds: int = 30,
) -> tuple[list[dict], str | None]:
    """Fetch a complete calendar month for a one-time, preservation-safe backfill."""
    try:
        month_start = datetime.strptime(month, "%Y-%m").date().replace(day=1)
    except (TypeError, ValueError) as exc:
        raise ValueError("month must use YYYY-MM") from exc

    if month_start.month == 12:
        month_end = date(month_start.year + 1, 1, 1)
    else:
        month_end = date(month_start.year, month_start.month + 1, 1)

    session = _session(user_agent=user_agent)
    latest_dt = _latest_published_datetime(session, timeout_seconds=timeout_seconds)
    if latest_dt is None or latest_dt.date() < month_start:
        return [], None
    effective_end = min(month_end, latest_dt.date() + timedelta(days=1))
    rows = _fetch_range_rows(
        session,
        start_date=month_start,
        end_date=effective_end,
        timeout_seconds=timeout_seconds,
    )
    latest_date = latest_dt.date() if month_start <= latest_dt.date() < month_end else None
    incidents = _normalize_rows(rows, latest_date=latest_date)
    covered_through = effective_end - timedelta(days=1)
    label = (
        f"Backfill {month_start.strftime('%b')} {month_start.year} "
        f"through {covered_through.strftime('%b')} {covered_through.day}, {covered_through.year}"
    )
    return incidents, label


def scrape_year(
    year: int,
    *,
    user_agent: str,
    timeout_seconds: int = 60,
) -> tuple[list[dict], str | None]:
    """Fetch every currently published, retained call for one calendar year."""
    if year < 2000 or year > 2100:
        raise ValueError("year must be between 2000 and 2100")

    year_start = date(year, 1, 1)
    year_end = date(year + 1, 1, 1)
    session = _session(user_agent=user_agent)
    latest_dt = _latest_published_datetime(session, timeout_seconds=timeout_seconds)
    if latest_dt is None or latest_dt.date() < year_start:
        return [], None

    effective_end = min(year_end, latest_dt.date() + timedelta(days=1))
    rows = _fetch_range_rows(
        session,
        start_date=year_start,
        end_date=effective_end,
        timeout_seconds=timeout_seconds,
    )
    latest_date = latest_dt.date() if year_start <= latest_dt.date() < year_end else None
    incidents = _normalize_rows(rows, latest_date=latest_date)
    covered_through = effective_end - timedelta(days=1)
    label = (
        f"Backfill {year} through {covered_through.strftime('%b')} "
        f"{covered_through.day}, {covered_through.year}"
    )
    return incidents, label
