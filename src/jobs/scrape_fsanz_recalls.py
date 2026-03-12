# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # FSANZ Food Recall Scraper
# MAGIC Scrapes food recall notices from Food Standards Australia New Zealand
# MAGIC and writes raw JSON to a Unity Catalog volume.

# COMMAND ----------

dbutils.widgets.text("catalog", "daveok", "Catalog")
dbutils.widgets.text("schema", "coles_genie_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_data")

volume_path = f"/Volumes/{catalog}/{schema}/raw_data/fsanz_recalls"
dbutils.fs.mkdirs(volume_path)

print(f"Volume path: {volume_path}")

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://www.foodstandards.gov.au"
LISTING_URL = f"{BASE_URL}/food-recalls/recalls"

# COMMAND ----------

def fetch_listing_page(page_num):
    """Fetch a listing page and extract recall URLs."""
    resp = requests.get(
        LISTING_URL,
        params={"items_per_page": 100, "page": page_num},
        timeout=60,
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    recalls = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "/food-recalls/recall-alert/" in href:
            full_url = BASE_URL + href if href.startswith("/") else href
            recalls.append({
                "url": full_url,
                "title": link.get_text(strip=True),
            })

    seen = set()
    unique = []
    for r in recalls:
        if r["url"] not in seen:
            seen.add(r["url"])
            unique.append(r)
    return unique

# COMMAND ----------

all_recall_urls = []
for page in range(20):
    page_recalls = fetch_listing_page(page)
    all_recall_urls.extend(page_recalls)
    print(f"Page {page}: found {len(page_recalls)} recalls")
    if len(page_recalls) < 100:
        break
    time.sleep(0.5)

print(f"\nTotal: {len(all_recall_urls)} recall URLs")

# COMMAND ----------

def fetch_recall_detail(recall_info):
    """Fetch a single recall detail page and extract all content."""
    url = recall_info["url"]
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        main = soup.find("main") or soup.find("article") or soup.find("body")
        full_text = main.get_text(separator="\n", strip=True) if main else ""

        data = {
            "url": url,
            "title": recall_info.get("title", ""),
            "full_text": full_text,
        }

        time_tag = soup.find("time")
        if time_tag:
            data["publication_date"] = time_tag.get("datetime", time_tag.get_text(strip=True))

        return data
    except Exception as e:
        return {
            "url": url,
            "title": recall_info.get("title", ""),
            "full_text": "",
            "error": str(e),
        }

# COMMAND ----------

results = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_recall_detail, r): r for r in all_recall_urls}
    for future in as_completed(futures):
        results.append(future.result())
        if len(results) % 50 == 0:
            print(f"Fetched {len(results)}/{len(all_recall_urls)} recalls")

print(f"\nCompleted: {len(results)} recalls fetched")
errors = [r for r in results if "error" in r]
if errors:
    print(f"Errors: {len(errors)}")

# COMMAND ----------

for recall in results:
    slug = recall["url"].rstrip("/").split("/")[-1][:200]
    file_path = f"{volume_path}/{slug}.json"
    dbutils.fs.put(
        file_path,
        json.dumps(recall, ensure_ascii=False, indent=2),
        overwrite=True,
    )

print(f"Wrote {len(results)} JSON files to {volume_path}")

# COMMAND ----------

manifest = {
    "source": LISTING_URL,
    "total_recalls": len(results),
    "errors": len(errors),
    "scrape_timestamp": str(spark.sql("SELECT current_timestamp()").collect()[0][0]),
}
dbutils.fs.put(
    f"{volume_path}/_manifest.json",
    json.dumps(manifest, indent=2),
    overwrite=True,
)
print("Scrape complete!")
