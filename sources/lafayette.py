"""Lafayette 911 traffic source adapter."""

from __future__ import annotations

from datetime import datetime
import re

import requests
from bs4 import BeautifulSoup


FEED_URL = "https://lafayette911.org/wp-json/traffic-feed/v1/data"


def _clean_ws(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _parse_reported_time(value: str | None) -> str:
    text = _clean_ws(value)
    if not text:
        return ""
    try:
        dt = datetime.strptime(text, "%m/%d/%Y %H:%M")
        return dt.strftime("%H%M")
    except Exception:
        # Fallback to "HHMM" extraction if the timestamp format changes.
        match = re.search(r"(\d{1,2}):(\d{2})", text)
        if not match:
            return ""
        hour = int(match.group(1))
        minute = int(match.group(2))
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return f"{hour:02d}{minute:02d}"
        return ""


def _split_location(value: str | None) -> tuple[str, str, str]:
    """
    Parse Lafayette's 'Located At' field into (street, cross_streets, municipality).
    """
    raw = _clean_ws(value)
    if not raw:
        return "", "", ""

    municipality = ""
    base = raw
    city_match = re.search(r"([A-Z][A-Z\s]+),\s*LA\b", base)
    if city_match:
        municipality = _clean_ws(city_match.group(1))
        base = _clean_ws(base[: city_match.start()])

    if "/" in base:
        left, right = base.split("/", 1)
        street = _clean_ws(left)
        cross_streets = _clean_ws(right)
    else:
        street = base
        cross_streets = ""

    return street, cross_streets, municipality


def _normalize_assisting(value: str | None) -> str:
    raw = _clean_ws(value).upper()
    if not raw:
        return ""

    # Keep only known assisting units in feed order and present consistently.
    matches = list(re.finditer(r"\b(FIRE|POLICE|SHERIFF)\b", raw))
    if not matches:
        return raw

    ordered_units: list[str] = []
    seen: set[str] = set()
    for match in matches:
        unit = match.group(1)
        if unit in seen:
            continue
        seen.add(unit)
        ordered_units.append(unit)
    return " / ".join(ordered_units)


def scrape(*, user_agent: str, timeout_seconds: int = 15) -> tuple[list[dict], str | None]:
    response = requests.get(
        FEED_URL,
        headers={"User-Agent": user_agent, "Accept": "application/json"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()

    payload = response.json()
    if not payload.get("success"):
        return [], None

    html = payload.get("data") or ""
    soup = BeautifulSoup(html, "html.parser")

    incidents: list[dict] = []
    rows = soup.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        location_raw = cells[0].get_text(" ", strip=True)
        if _clean_ws(location_raw).lower() == "located at":
            continue

        description = _clean_ws(cells[1].get_text(" ", strip=True))
        time_val = _parse_reported_time(cells[2].get_text(" ", strip=True))
        agency = _normalize_assisting(cells[3].get_text(" ", strip=True))
        street, cross_streets, municipality = _split_location(location_raw)

        if not description:
            continue

        incidents.append(
            {
                "source": "lafayette",
                "agency": agency or "UNKNOWN",
                "time": time_val,
                "units": 1,
                "description": description,
                "street": street,
                "cross_streets": cross_streets,
                "municipality": municipality,
            }
        )

    # Endpoint does not expose a dedicated refreshed timestamp.
    return incidents, None
