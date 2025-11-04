# 🌍 Africa Energy Data ETL Pipeline

## 📘 Project Overview

This project builds a complete **ETL (Extract, Transform, Load)** pipeline for the [Africa Energy Portal (AEP)](https://africa-energy-portal.org/), automating the collection, cleaning, validation, and storage of **energy access metrics** across all African countries from **2000–2024**.

The pipeline extracts data from AEP’s public API, formats it into a consistent schema, validates for completeness, stores it in **MongoDB**, and finally exports it to **CSV and Excel** formats for use in Power BI, Excel, or other analytics tools.

---

## ⚡ Problem Statement

Energy data for African countries is often fragmented and inconsistently structured, making comparison across countries and years difficult.

This project addresses that problem by:
- **Extracting** complete time-series energy indicators (2000–2024) from the Africa Energy Portal.  
- **Transforming** raw JSONs into a unified tabular format for each country and metric.  
- **Validating** data integrity across units, countries, and years.  
- **Loading** results into MongoDB for analysis or export.

---

## 🧩 ETL Pipeline Overview

| Stage | Description | Output |
|:------|:-------------|:--------|
| **Stage 1 – Scraping** | Extracts data from AEP’s `/get-country-data` endpoint using Playwright automation. | Raw JSON (`reports/raw_json/`) |
| **Stage 2 – Formatting** | Cleans and reshapes the JSON into a structured table format. | Formatted JSON (`reports/formatted/`) |
| **Stage 3 – Storage** | Loads formatted data into a MongoDB collection with unique indexing. | MongoDB dataset |
| **Stage 4 – Validation** | Performs quality checks (missing years, unit inconsistencies, missing countries). | Validation CSV (`reports/validation_report.csv`) |
| **Export Stage** | Exports MongoDB data to CSV and Excel formats. | `reports/exports/energy_metrics.csv`, `.xlsx` |

---

## 🚀 Getting Started 

### **1️⃣ Clone the repository**
```bash
git clone 
cd Africa_Energy
```

### **2️⃣ Create and activate the environment**
```bash
uv sync
```

### **3️⃣ Install Playwright Browsers**
```bash
uv run playwright install
```

### **4️⃣ Configure MongoDB Connection**
Update `aep_etl/config.py`:
```python
mongo_uri: str = "mongodb://localhost:27017"
mongo_db: str = "aep"
mongo_collection: str = "energy_metrics"
```

### **5️⃣ Run the full ETL pipeline**
```bash
uv run python run_pipeline.py
```

### **6️⃣ Export the Data**
```bash
uv run python export_to_csv.py
```

---

## 📊 Results & Key Takeaways

- Extracted **energy data for 45+ African countries**
- Stored **570+ normalized records** in MongoDB
- Generated validated, ready-to-analyze files in CSV/Excel format

---

## ⚠️ Challenges Faced

| Category | Description |
|-----------|-------------|
| **Website Protections (403 & 500 Errors)** | The Africa Energy Portal uses **Cloudflare** protection, which initially blocked direct HTTP requests. The scraper had to be adapted to work with **Playwright**, simulating real browser behavior. |
| **Slow Network Response / Timeouts** | Some country pages took too long to load (e.g., Cabo Verde, Côte d’Ivoire), requiring **increased timeout limits** and **retry logic** to ensure reliability. |
| **Inconsistent URL Naming** | Country URLs vary in formatting (e.g., `cote-d’ivoire`, `sao-tome-and-principe`), so normalization logic was added for automated handling. |
| **Incomplete Yearly Data** | Not all metrics covered every year between 2000–2024; Stage 4 handled **missing year validation** and logged issues in `validation_report.csv`. |
| **Browser Resource Use** | Running Playwright for all African countries consumed significant system resources and time (~30 minutes total runtime). |


---
