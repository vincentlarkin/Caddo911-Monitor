"""Caddo 911 source adapter."""

from __future__ import annotations

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://ias.ecc.caddo911.com/All_ActiveEvents.aspx"


def scrape(*, user_agent: str, timeout_seconds: int = 15) -> tuple[list[dict], str | None]:
    """
    Scrape active incidents from Caddo 911.

    Returns:
      (incidents, refreshed_at_text)
    """
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        }
    )

    # First request establishes ASP.NET cookies.
    response = session.get(BASE_URL, timeout=timeout_seconds, allow_redirects=True)
    if "AspxAutoDetectCookieSupport" in response.url or response.status_code == 302:
        response = session.get(f"{BASE_URL}?AspxAutoDetectCookieSupport=1", timeout=timeout_seconds)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    incidents: list[dict] = []
    refreshed_at_text: str | None = None

    for s in soup.stripped_strings:
        if "Refreshed at:" in s:
            refreshed_at_text = s.split("Refreshed at:", 1)[1].strip()
            break

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 6:
                continue

            agency = cells[0].get_text(strip=True)
            time_val = cells[1].get_text(strip=True)
            units = cells[2].get_text(strip=True)
            description = cells[3].get_text(strip=True)
            street = cells[4].get_text(strip=True)
            cross_streets = cells[5].get_text(strip=True)
            municipality = cells[6].get_text(strip=True) if len(cells) > 6 else ""

            # Ignore non-data rows/layout rows.
            if not (
                agency
                and len(agency) <= 10
                and time_val
                and time_val.isdigit()
                and len(time_val) <= 4
                and description
            ):
                continue

            incidents.append(
                {
                    "source": "caddo",
                    "agency": agency,
                    "time": time_val,
                    "units": int(units) if units.isdigit() else 1,
                    "description": description,
                    "street": street,
                    "cross_streets": cross_streets,
                    "municipality": municipality,
                }
            )

    return incidents, refreshed_at_text
