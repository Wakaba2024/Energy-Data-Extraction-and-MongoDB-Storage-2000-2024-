"""
Stage 3: MongoDB Storage
------------------------
This stage reads the formatted JSON dataset (from Stage 2)
and stores each record in a MongoDB collection, using upserts
to avoid duplicates.
"""

from __future__ import annotations
import json
from pathlib import Path
from pymongo import MongoClient, UpdateOne
from typing import List, Dict, Any

from aep_etl.config import SETTINGS


# ---------------------------------------------------------------------------
# MongoDB Connection Setup
# ---------------------------------------------------------------------------

def get_collection():
    """
    Connect to MongoDB and return the target collection.
    Automatically creates a unique index on (country, metric, source).
    """
    client = MongoClient(SETTINGS.mongo_uri)
    db = client[SETTINGS.mongo_db]
    coll = db[SETTINGS.mongo_collection]

    coll.create_index(
        [("country", 1), ("metric", 1), ("source", 1)],
        unique=True,
        name="uniq_country_metric_source"
    )

    return coll


# ---------------------------------------------------------------------------
# Main Upsert Logic
# ---------------------------------------------------------------------------

def run_stage3(formatted_docs: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Upsert all formatted docs into MongoDB.
    Returns summary stats.
    """
    coll = get_collection()
    if not formatted_docs:
        print("⚠️  No formatted documents provided.")
        return {"matched": 0, "modified": 0, "upserted": 0}

    print(f"📥 Inserting {len(formatted_docs)} documents into MongoDB...")

    ops = []
    for d in formatted_docs:
        key = {
            "country": d["country"],
            "metric": d["metric"],
            "source": d["source"],
        }
        ops.append(UpdateOne(key, {"$set": d}, upsert=True))

    try:
        res = coll.bulk_write(ops, ordered=False)
    except Exception as e:
        print(f"❌ MongoDB write failed: {e}")
        return {"matched": 0, "modified": 0, "upserted": 0}

    summary = {
        "matched": res.matched_count,
        "modified": res.modified_count,
        "upserted": len(res.upserted_ids),
    }

    print(f"✅ Mongo write summary: {summary}")
    return summary


# ---------------------------------------------------------------------------
# Utilities for loading the formatted JSON file
# ---------------------------------------------------------------------------

def load_formatted_data(path: str = "reports/formatted/formatted_data.json") -> List[Dict[str, Any]]:
    """Load formatted data from Stage 2 output."""
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
    print("🔄 Stage 3: MongoDB Insertion ...")

    formatted_data = load_formatted_data()
    if not formatted_data:
        print("⚠️  No data found. Run Stage 2 first.")
        exit(1)

    result = run_stage3(formatted_data)

    print(f"🎯 MongoDB Insertion Completed: {result}")
