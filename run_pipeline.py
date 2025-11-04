"""
Africa Energy Portal ETL Pipeline
---------------------------------
Stages:
  1. Scrape energy data via Playwright
  2. Format into standardized schema
  3. Store into MongoDB
  4. Validate & generate report

Usage:
  uv run python run_pipeline.py
  uv run python run_pipeline.py --countries Kenya Nigeria
  uv run python run_pipeline.py --dry
"""

from __future__ import annotations
import argparse
import json
import os
import time
from pathlib import Path

from aep_etl.stage1_scrape import run_stage1
from aep_etl.stage2_format import run_stage2
from aep_etl.stage3_store import run_stage3
from aep_etl.stage4_validate import run_stage4


def main():
    parser = argparse.ArgumentParser(description="Africa Energy Portal ETL pipeline")
    parser.add_argument(
        "--countries", nargs="*", help="Subset of countries to scrape (default: all)"
    )
    parser.add_argument(
        "--dry", action="store_true", help="Skip MongoDB write (dry run only)"
    )
    args = parser.parse_args()

    start_time = time.time()
    Path("reports").mkdir(exist_ok=True)

    # --- Stage 1: Scrape ---
    print("🌍 Stage 1: Scraping data from Africa Energy Portal ...")
    t1 = time.time()
    raw_rows = run_stage1(args.countries)
    print(f"✅ Collected {len(raw_rows)} raw rows in {time.time() - t1:.2f}s")

    # --- Stage 2: Format ---
    print("\n🧱 Stage 2: Formatting data ...")
    t2 = time.time()
    docs = run_stage2(raw_rows)
    formatted_dir = Path("reports/formatted")
    formatted_dir.mkdir(parents=True, exist_ok=True)
    formatted_path = formatted_dir / "formatted_data.json"
    with open(formatted_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, indent=2)
    print(f"✅ Formatted {len(docs)} documents in {time.time() - t2:.2f}s")
    print(f"📄 Saved formatted output to {formatted_path}")

    # --- Stage 3: MongoDB ---
    if args.dry:
        print("\n💾 Stage 3: Skipped MongoDB storage (dry run mode)")
    else:
        print("\n💾 Stage 3: Inserting into MongoDB ...")
        t3 = time.time()
        try:
            summary = run_stage3(docs)
            print(f"✅ MongoDB write summary: {json.dumps(summary)}")
        except Exception as e:
            print(f"❌ MongoDB insertion failed: {e}")
        print(f"⏱ Stage 3 duration: {time.time() - t3:.2f}s")

    # --- Stage 4: Validation ---
    print("\n🔍 Stage 4: Validating data consistency ...")
    t4 = time.time()
    v_summary = run_stage4(docs)
    print(f"✅ Validation summary: {json.dumps(v_summary, indent=2)}")
    print(f"📄 Validation report written to reports/validation_report.csv")
    print(f"⏱ Stage 4 duration: {time.time() - t4:.2f}s")

    # --- Pipeline Summary ---
    total = time.time() - start_time
    print("\n🎯 Pipeline completed successfully!")
    print(f"🕒 Total runtime: {total:.2f}s")
    print("📂 All outputs saved in the 'reports/' directory.")


if __name__ == "__main__":
    main()
