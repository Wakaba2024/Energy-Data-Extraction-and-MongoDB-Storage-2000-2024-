"""
Stage 4: Validation
-------------------
Checks for:
  - Missing years (data gaps)
  - Inconsistent units per metric
  - Countries with no data
Then writes a CSV validation report.
"""

from __future__ import annotations
from typing import List, Dict
from collections import defaultdict
import csv, os, json
from pathlib import Path
from aep_etl.constants import YEARS, AFRICAN_COUNTRIES


# ---------------------------------------------------------------------------
# Core Validation Logic
# ---------------------------------------------------------------------------

def run_stage4(
    formatted_docs: List[dict],
    report_path: str = "reports/validation_report.csv"
) -> Dict[str, int]:
    """
    Run validation checks on formatted data and export a summary CSV.
    Returns statistics for quick reporting.
    """
    os.makedirs(os.path.dirname(report_path), exist_ok=True)

    gaps = []               # missing years or scrape errors
    unit_issues = []        # metrics using multiple units
    by_metric_units = defaultdict(set)
    countries_with_data = set()

    # --- Check data completeness ---
    for d in formatted_docs:
        c = d["country"]
        countries_with_data.add(c)
        metric = d["metric"]
        unit = d.get("unit", "")
        by_metric_units[metric].add(unit)

        missing_years = [str(y) for y in YEARS if d.get(str(y)) is None]
        if missing_years:
            gaps.append((c, metric, ";".join(missing_years)))
        if metric == "__SCRAPE_ERROR__":
            gaps.append((c, metric, "ALL"))

    # --- Detect inconsistent units ---
    for metric, units in by_metric_units.items():
        clean_units = {u for u in units if u}
        if metric != "__SCRAPE_ERROR__" and len(clean_units) > 1:
            unit_issues.append((metric, ";".join(sorted(clean_units))))

    # --- Detect missing countries ---
    missing_countries = [c for c in AFRICAN_COUNTRIES if c not in countries_with_data]

    # --- Write validation report ---
    with open(report_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["issue_type", "country", "metric", "details"])
        for c, m, yrs in gaps:
            writer.writerow(["MISSING_YEARS_OR_ERROR", c, m, yrs])
        for m, units in unit_issues:
            writer.writerow(["UNIT_INCONSISTENCY", "*ALL*", m, units])
        for c in missing_countries:
            writer.writerow(["NO_DATA_FOR_COUNTRY", c, "*", "NO_ROWS"])

    summary = {
        "rows_with_gaps": len(gaps),
        "unit_conflicts": len(unit_issues),
        "missing_countries": len(missing_countries),
        "total_docs_checked": len(formatted_docs),
    }

    print(f"✅ Validation summary: {json.dumps(summary, indent=2)}")
    print(f"📄 Report written to {report_path}")
    return summary


# ---------------------------------------------------------------------------
# Utility: Load formatted data from Stage 2 output
# ---------------------------------------------------------------------------

def load_formatted_data(path: str = "reports/formatted/formatted_data.json") -> List[dict]:
    p = Path(path)
    if not p.exists():
        print(f"❌ Missing file: {p}")
        return []
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"✅ Loaded {len(data)} formatted records from {p}")
    return data


# ---------------------------------------------------------------------------
# CLI Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("🔍 Stage 4: Validation...")

    formatted_docs = load_formatted_data()
    if not formatted_docs:
        print("⚠️  No formatted data found. Run Stage 2 first.")
        exit(1)

    summary = run_stage4(formatted_docs)
    print(f"🎯 Stage 4 completed successfully.")
