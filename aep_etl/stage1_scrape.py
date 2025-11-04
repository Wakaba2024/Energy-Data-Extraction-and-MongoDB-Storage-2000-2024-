"""
Stage 1 – Scrape Africa Energy Portal
--------------------------------------

This stage extracts country-level energy indicators for all African nations
from the official Africa Energy Portal (AEP) endpoint:
    POST https://africa-energy-portal.org/get-country-data

It uses Playwright to open each country’s page and intercept the
live /get-country-data request, ensuring Cloudflare protection is bypassed.
"""

from __future__ import annotations
import json, time
from typing import List, Dict, Any
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError
from aep_etl.utils import polite_sleep
from aep_etl.config import SETTINGS


# ---------------------------------------------------------------------------
# Parser helper
# ---------------------------------------------------------------------------

def parse_aep_payload(payload: list[dict], country: str, source_link: str) -> List[Dict[str, Any]]:
    """Parse the JSON structure returned by /get-country-data for one country."""
    rows: List[Dict[str, Any]] = []

    for item in payload:
        _id = item.get("_id", {})
        data_points = item.get("data", [])
        source = ", ".join(s for s in item.get("source", []) if s) or "Unknown"

        yearly = {}
        for dp in data_points:
            year = dp.get("year")
            value = dp.get("value")
            try:
                yearly[int(year)] = float(value)
            except (TypeError, ValueError):
                yearly[int(year)] = None

        rows.append(
            dict(
                country=country,
                country_serial=country.lower().replace(" ", "_"),
                metric=_id.get("indicator") or _id.get("title"),
                unit="",
                sector=_id.get("pillar", "Unknown"),
                sub_sector=None,
                sub_sub_sector=None,
                source_link=source_link,
                source=source,
                yearly=yearly,
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Fetcher – with retry, headless, and better waiting
# ---------------------------------------------------------------------------

def fetch_country_data(country: str, retries: int = 2) -> List[Dict[str, Any]]:
    """
    Opens a country's AEP page, waits for /get-country-data,
    and parses the response. Retries automatically if timeout occurs.
    """
    url = f"{SETTINGS.base_url}/country/{country.lower().replace(' ', '-')}"
    print(f"🌍 Opening {url}")

    for attempt in range(1, retries + 1):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)  # set True for silent runs
                ctx = browser.new_context()
                page = ctx.new_page()

                page.goto(url, wait_until="domcontentloaded")

                try:
                    # Compatible version of waiting logic (works on all builds)
                    with page.expect_response(lambda r: "get-country-data" in r.url and r.status == 200, timeout=60000) as resp_info:
                        # Wait until network is idle to ensure all XHRs fire
                        page.wait_for_load_state("networkidle")

                    response = resp_info.value
                    data = response.json()

                    browser.close()
                    rows = parse_aep_payload(data, country, response.url)
                    print(f"✅ Collected {len(rows)} rows for {country}")
                    polite_sleep(SETTINGS.throttle_ms)
                    return rows

                except Exception as e:
                    print(f"⚠️  Timeout or response issue (attempt {attempt}) for {country}: {e}")
                    browser.close()
                    polite_sleep(3000)

        except Exception as e:
            print(f"❌ Browser-level error for {country}: {e}")
            polite_sleep(3000)

    raise RuntimeError(f"Failed to fetch data for {country} after {retries} retries.")


# ---------------------------------------------------------------------------
# Stage 1 Runner (Single or Batch)
# ---------------------------------------------------------------------------

AFRICAN_COUNTRIES = [
    "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi", "Cabo Verde",
    "Cameroon", "Central African Republic", "Chad", "Comoros", "Congo", "Congo, Dem. Rep.",
    "Côte d’Ivoire", "Djibouti", "Egypt", "Equatorial Guinea", "Eritrea", "Eswatini",
    "Ethiopia", "Gabon", "Gambia", "Ghana", "Guinea", "Guinea-Bissau", "Kenya", "Lesotho",
    "Liberia", "Libya", "Madagascar", "Malawi", "Mali", "Mauritania", "Mauritius",
    "Morocco", "Mozambique", "Namibia", "Niger", "Nigeria", "Rwanda", "São Tomé and Príncipe",
    "Senegal", "Seychelles", "Sierra Leone", "Somalia", "South Africa", "South Sudan",
    "Sudan", "Tanzania", "Togo", "Tunisia", "Uganda", "Zambia", "Zimbabwe"
]


def run_stage1(countries: list[str] | None = None) -> List[Dict[str, Any]]:
    """Scrape one or more countries and return combined results."""
    targets = countries or AFRICAN_COUNTRIES
    all_rows: list[dict] = []

    for country in targets:
        try:
            rows = fetch_country_data(country)
            all_rows.extend(rows)
        except Exception as e:
            print(f"⚠️  {country}: {e}")
            all_rows.append(
                dict(
                    country=country,
                    country_serial=country.lower().replace(" ", "_"),
                    metric="__SCRAPE_ERROR__",
                    unit="",
                    sector="__SCRAPE_ERROR__",
                    sub_sector=None,
                    sub_sub_sector=None,
                    source_link=f"{SETTINGS.base_url}/get-country-data",
                    source="__ERROR__",
                    yearly={},
                )
            )
    return all_rows


def batch_fetch_all_countries():
    """Fetch all African countries and save JSON per country under reports/raw_json/"""
    raw_dir = Path("reports/raw_json")
    raw_dir.mkdir(parents=True, exist_ok=True)

    for country in AFRICAN_COUNTRIES:
        out_file = raw_dir / f"{country.lower().replace(' ', '_')}.json"
        if out_file.exists():
            print(f"✅ Skipping {country} (already scraped)")
            continue

        try:
            rows = fetch_country_data(country)
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(rows, f, indent=2, ensure_ascii=False)
            print(f"💾 Saved {len(rows)} rows for {country}\n")
        except Exception as e:
            print(f"❌ Failed {country}: {e}\n")
        time.sleep(3)

    print("🎯 Done! All countries processed.")


# ---------------------------------------------------------------------------
# CLI Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "dump_country_json":
            country = sys.argv[2] if len(sys.argv) > 2 else "Kenya"
            rows = fetch_country_data(country)
            out_dir = Path("reports/raw_json")
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{country.lower().replace(' ', '_')}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(rows, f, indent=2, ensure_ascii=False)
            print(f"✅ Saved {len(rows)} rows to {out_path}")
        elif cmd == "batch_fetch_all_countries":
            batch_fetch_all_countries()
    else:
        data = run_stage1(["Kenya"])
        print(f"Collected {len(data)} rows.")
