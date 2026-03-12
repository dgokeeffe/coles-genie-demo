# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Grocery Industry PDF Downloader
# MAGIC Downloads publicly available grocery/supermarket PDFs
# MAGIC (ACCC reports, Coles annual reports) to a Unity Catalog volume.

# COMMAND ----------

dbutils.widgets.text("catalog", "daveok", "Catalog")
dbutils.widgets.text("schema", "coles_genie_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_data")

volume_path = f"/Volumes/{catalog}/{schema}/raw_data/grocery_pdfs"
dbutils.fs.mkdirs(volume_path)

print(f"Volume path: {volume_path}")

# COMMAND ----------

PDF_SOURCES = [
    {
        "filename": "accc_supermarkets_inquiry_final_2025.pdf",
        "url": "https://www.accc.gov.au/system/files/supermarkets-inquiry_1.pdf",
        "description": "ACCC Supermarkets Inquiry Final Report (Feb 2025) - pricing practices, competition, market power",
    },
    {
        "filename": "accc_supermarkets_inquiry_interim_2024.pdf",
        "url": "https://www.accc.gov.au/system/files/supermarkets-inquiry-2024-2025-interim-report.pdf",
        "description": "ACCC Supermarkets Inquiry Interim Report (Aug 2024) - preliminary findings on pricing and competition",
    },
    {
        "filename": "accc_supermarkets_inquiry_summary.pdf",
        "url": "https://www.accc.gov.au/system/files/supermarkets-inquiry-summary_1.pdf",
        "description": "ACCC Supermarkets Inquiry Summary Fact Sheet - key findings and recommendations",
    },
    {
        "filename": "coles_annual_report_fy25.pdf",
        "url": "https://www.colesgroup.com.au/FormBuilder/_Resource/_module/ir5sKeTxxEOndzdh00hWJw/file/Annual_Report.pdf",
        "description": "Coles Group FY25 Annual Report - financials, operations, strategy, store network",
    },
    {
        "filename": "coles_sustainability_report_fy25.pdf",
        "url": "https://www.colesgroup.com.au/FormBuilder/_Resource/_module/ir5sKeTxxEOndzdh00hWJw/file/Sustainability_Report.pdf",
        "description": "Coles Group FY25 Sustainability Report - ESG, responsible sourcing, waste reduction",
    },
]

# COMMAND ----------

import requests
import json

results = []
for pdf in PDF_SOURCES:
    filepath = f"{volume_path}/{pdf['filename']}"
    try:
        resp = requests.get(pdf["url"], timeout=120, allow_redirects=True)
        resp.raise_for_status()

        # Write binary content via local FUSE path
        local_path = filepath.replace("/Volumes/", "/Volumes/")
        with open(local_path, "wb") as f:
            f.write(resp.content)

        size_mb = len(resp.content) / (1024 * 1024)
        print(f"OK  {pdf['filename']} ({size_mb:.1f} MB)")
        results.append({**pdf, "status": "ok", "size_bytes": len(resp.content)})
    except Exception as e:
        print(f"ERR {pdf['filename']}: {e}")
        results.append({**pdf, "status": "error", "error": str(e)})

# COMMAND ----------

manifest = {
    "source": "ACCC + Coles Group public PDFs",
    "total_pdfs": len(results),
    "successful": sum(1 for r in results if r["status"] == "ok"),
    "files": results,
    "download_timestamp": str(spark.sql("SELECT current_timestamp()").collect()[0][0]),
}
dbutils.fs.put(
    f"{volume_path}/_manifest.json",
    json.dumps(manifest, indent=2),
    overwrite=True,
)
print(f"\nDownloaded {manifest['successful']}/{manifest['total_pdfs']} PDFs")
