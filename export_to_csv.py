"""
Export MongoDB collection (energy metrics) to CSV and Excel
"""
from pymongo import MongoClient
import pandas as pd
from aep_etl.config import SETTINGS
from pathlib import Path

def export_data():
    client = MongoClient(SETTINGS.mongo_uri)
    db = client[SETTINGS.mongo_db]
    coll = db[SETTINGS.mongo_collection]

    # Fetch all documents
    print("📥 Fetching documents from MongoDB...")
    docs = list(coll.find({}, {"_id": 0}))  # exclude Mongo _id

    if not docs:
        print("⚠️ No documents found in MongoDB collection.")
        return

    df = pd.DataFrame(docs)
    print(f"✅ Loaded {len(df)} records into DataFrame")

    # Ensure output folder exists
    out_dir = Path("reports/exports")
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "energy_metrics.csv"
    xlsx_path = out_dir / "energy_metrics.xlsx"

    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)

    print(f"💾 CSV saved to: {csv_path}")
    print(f"💾 Excel saved to: {xlsx_path}")

if __name__ == "__main__":
    export_data()
