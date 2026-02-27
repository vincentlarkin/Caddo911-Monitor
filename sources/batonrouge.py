"""Baton Rouge traffic incident source adapter."""

from __future__ import annotations

from datetime import datetime
import re

import requests
from bs4 import BeautifulSoup


FEED_URL = "https://city.brla.gov/traffic/incidents.asp"


def _clean_ws(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _parse_time_to_hhmm(value: str | None) -> str:
    text = _clean_ws(value)
    if not text:
        return ""

    for fmt in ("%I:%M:%S %p", "%I:%M %p"):
        try:
            return datetime.strptime(text, fmt).strftime("%H%M")
        except Exception:
            pass

    # Fallback if upstream formatting shifts.
    match = re.search(r"(\d{1,2}):(\d{2})(?::\d{2})?\s*([AP]M)", text, flags=re.IGNORECASE)
    if not match:
        return ""

    hour = int(match.group(1))
    minute = int(match.group(2))
    ampm = match.group(3).upper()
    if hour == 12:
        hour = 0
    if ampm == "PM":
        hour += 12
    if 0 <= hour <= 23 and 0 <= minute <= 59:
        return f"{hour:02d}{minute:02d}"
    return ""


def _extract_refreshed_at_text(soup: BeautifulSoup) -> str | None:
    text = _clean_ws(soup.get_text(" ", strip=True))
    if not text:
        return None
    match = re.search(r"Last Updated\s+(.+?)\s+Number of incidents\s*:", text, flags=re.IGNORECASE)
    if match:
        return _clean_ws(match.group(1))
    return None


def scrape(*, user_agent: str, timeout_seconds: int = 15) -> tuple[list[dict], str | None]:
    response = requests.get(
        FEED_URL,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    refreshed_at_text = _extract_refreshed_at_text(soup)

    incidents: list[dict] = []
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        time_raw = _clean_ws(cells[0].get_text(" ", strip=True))
        incident_type = _clean_ws(cells[1].get_text(" ", strip=True))
        agency = _clean_ws(cells[2].get_text(" ", strip=True))
        location = _clean_ws(cells[3].get_text(" ", strip=True))
        cross_street = _clean_ws(cells[4].get_text(" ", strip=True))

        if not incident_type:
            continue

        incidents.append(
            {
                "source": "batonrouge",
                "agency": agency or "UNKNOWN",
                "time": _parse_time_to_hhmm(time_raw),
                "units": 1,
                "description": incident_type,
                "street": location,
                "cross_streets": cross_street,
                "municipality": "Baton Rouge",
            }
        )

    return incidents, refreshed_at_text
