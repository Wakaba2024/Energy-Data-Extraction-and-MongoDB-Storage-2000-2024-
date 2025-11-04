"""
Stage 2: Data Formatting
------------------------
This stage reads the raw metric rows (from Stage 1) and transforms them into a
standardized, MongoDB-ready schema, ensuring consistent country IDs and full
year coverage from 2000–2024.
"""

from __future__ import annotations
import json, os
from pathlib import Path
from typing import List, Dict, Any

from aep_etl.constants import YEARS
from aep_etl.types_ import MetricRow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_country_serial(country: str) -> str:
    """
    Normalize country name into a serial-friendly string used for filenames
    and database identifiers.
    Example:
        "Côte d’Ivoire" -> "cote_divoire"
        "Congo, Dem. Rep." -> "congo_dem_rep"
    """
    return (
        country.lower()
        .replace(" ", "_")
        .replace(",", "")
        .replace("’", "")
        .replace("'", "")
        .replace("é", "e")
    )


# ---------------------------------------------------------------------------
# Core Formatter
# ---------------------------------------------------------------------------

def run_stage2(raw_rows: List[dict]) -> List[dict]:
    """
    Convert raw metric rows from Stage 1 into fully flattened MongoDB-ready docs.
    """
    docs: List[Dict[str, Any]] = []
    for r in raw_rows:
        mr = MetricRow(
            country=r["country"],
            country_serial=normalize_country_serial(r["country"]),
            metric=r.get("metric", "Unknown"),
            unit=r.get("unit", ""),
            sector=r.get("sector", "Unknown"),
            sub_sector=r.get("sub_sector"),
            sub_sub_sector=r.get("sub_sub_sector"),
            source_link=r.get("source_link", ""),
            source=r.get("source", "Africa Energy Portal"),
            yearly=r.get("yearly", {y: None for y in YEARS}),
        )

        doc = mr.to_mongo_doc()

        # Guarantee every year 2000–2024 exists as a key
        for y in YEARS:
            doc.setdefault(str(y), None)

        docs.append(doc)

    return docs


# ---------------------------------------------------------------------------
# JSON Loader & Saver Utilities
# ---------------------------------------------------------------------------

def load_raw_data(raw_dir: str = "reports/raw_json") -> List[dict]:
    """Load all country JSON files from Stage 1 output directory."""
    all_rows = []
    raw_path = Path(raw_dir)
    for file in raw_path.glob("*.json"):
        try:
            with open(file, "r", encoding="utf-8") as f:
                rows = json.load(f)
                all_rows.extend(rows)
            print(f"✅ Loaded {file.name} ({len(rows)} rows)")
        except Exception as e:
            print(f"⚠️  Failed to read {file.name}: {e}")
    return all_rows


def save_formatted_docs(docs: List[dict], out_path: str = "reports/formatted/formatted_data.json"):
    """Save the formatted documents into a single JSON file."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, indent=2, ensure_ascii=False)
    print(f"🎯 Saved {len(docs)} formatted records to {out_path}")


# ---------------------------------------------------------------------------
# CLI Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("🔄 Stage 2: Starting data formatting ...")

    raw_dir = Path("reports/raw_json")
    if not raw_dir.exists():
        print(f"⚠️  Directory not found: {raw_dir.resolve()}")
        print("❌ Please ensure Stage 1 has saved JSON files in 'reports/raw_json/'")
        exit(1)

    raw_data = load_raw_data(str(raw_dir))
    print(f"📦 Loaded {len(raw_data)} total raw rows from {raw_dir}")

    if not raw_data:
        print("⚠️  No raw JSON data found. Exiting.")
        exit(0)

    formatted_docs = run_stage2(raw_data)
    print(f"✅ Formatted {len(formatted_docs)} rows successfully.")

    out_path = "reports/formatted/formatted_data.json"
    save_formatted_docs(formatted_docs, out_path)

    print("🎯 Stage 2 completed successfully.")

