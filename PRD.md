# PRD: Grocery Intelligence Platform — Data Pipeline

## Overview

Build a Lakeflow Declarative Pipeline that ingests four public Australian data sources through a medallion architecture (Bronze → Silver → Gold), producing analytics-ready tables for Databricks Genie and AI/BI dashboards.

**Deployment target:** Databricks Asset Bundles (DABs)
**Catalog:** `${var.catalog}` / **Schema:** `${var.schema}`
**Pipeline mode:** Serverless, CURRENT channel

---

## Data Sources

| Source | Type | Endpoint / Location |
|--------|------|-------------------|
| ABS Retail Trade | SDMX REST API (CSV) | `https://data.api.abs.gov.au/data/ABS,RT,1.0.0/M1.20+41+42+43+44+45.20.1+2+3+4+5+6+7+8.M?format=csv&startPeriod=2010-01` |
| ABS CPI Food | SDMX REST API (CSV) | `https://data.api.abs.gov.au/data/ABS,CPI,2.0.0/1.10001+20001.10.1+2+3+4+5+6+7+8.Q?format=csv&startPeriod=2010-Q1` |
| FSANZ Food Recalls | Web scraper → JSON in UC Volume | `/Volumes/{catalog}/{schema}/raw_data/fsanz_recalls/*.json` |
| ACCC Grocery Reports | PDF download → UC Volume | `/Volumes/{catalog}/{schema}/raw_data/grocery_pdfs/*.pdf` |

---

## Pipeline Architecture

```
Bronze (raw ingestion)          Silver (cleaned & enriched)       Gold (analytics-ready)
─────────────────────           ────────────────────────          ──────────────────────
abs_retail_trade_bronze    →    retail_turnover              →    retail_summary
abs_cpi_food_bronze        →    food_price_index             →    food_inflation_yoy
fsanz_recalls_bronze       →    food_recalls
grocery_pdfs_bronze        →    grocery_documents
```

All tables in Unity Catalog. Gold layer uses `@dp.materialized_view`. All others use `@dp.table`.

---

## Acceptance Criteria

### AC-1: Bronze — ABS Retail Trade Ingestion

**Table:** `abs_retail_trade_bronze`

- Ingests CSV from ABS Retail Trade SDMX API via `requests` + `pandas`
- Contains all original columns from the API response
- Data quality expectations:
  - `valid_obs_value`: `OBS_VALUE IS NOT NULL`
  - `valid_time_period`: `TIME_PERIOD IS NOT NULL`
  - `valid_region`: `REGION IS NOT NULL`
- Covers data from 2010-01 onward
- States: 1–8 (NSW, VIC, QLD, SA, WA, TAS, NT, ACT)
- Industries: 20 (Food), 41–45 (Clothing, Department, Household, Other, Cafes)

### AC-2: Bronze — ABS CPI Food Ingestion

**Table:** `abs_cpi_food_bronze`

- Ingests CSV from ABS CPI SDMX API via `requests` + `pandas`
- Contains all original columns from the API response
- Data quality expectations:
  - `valid_obs_value`: `OBS_VALUE IS NOT NULL`
  - `valid_time_period`: `TIME_PERIOD IS NOT NULL`
- Covers quarterly data from 2010-Q1 onward
- Index categories: 10001 (All groups CPI), 20001 (Food and non-alcoholic beverages)

### AC-3: Bronze — FSANZ Food Recalls Ingestion

**Table:** `fsanz_recalls_bronze`

- Reads JSON files from UC Volume path `/Volumes/{catalog}/{schema}/raw_data/fsanz_recalls`
- Schema: `url` (string), `title` (string), `full_text` (string), `publication_date` (string), `error` (string)
- Filters out rows where `url IS NULL`
- Adds `source_file` column from `_metadata.file_path`
- Excludes manifest files (glob: `[!_]*.json`)
- Data quality expectations:
  - `valid_url`: `url IS NOT NULL`
  - `has_content`: `full_text IS NOT NULL AND length(full_text) > 0`

### AC-4: Bronze — Grocery PDF Ingestion

**Table:** `grocery_pdfs_bronze`

- Reads binary PDF files from UC Volume path `/Volumes/{catalog}/{schema}/raw_data/grocery_pdfs`
- Uses `spark.read.format("binaryFile")`
- Output columns: `path`, `filename` (extracted from path), `length`, `modified_at`, `content` (binary)
- Excludes manifest files (glob: `[!_]*.pdf`)
- Data quality expectations:
  - `valid_content`: `length > 0`

### AC-5: Silver — Retail Turnover

**Table:** `retail_turnover`

- Reads from `abs_retail_trade_bronze`
- Decodes `REGION` codes to full state names:
  - 1=New South Wales, 2=Victoria, 3=Queensland, 4=South Australia, 5=Western Australia, 6=Tasmania, 7=Northern Territory, 8=Australian Capital Territory
- Decodes `INDUSTRY` codes to readable names:
  - 20=Food retailing, 41=Clothing footwear and personal accessory retailing, 42=Department stores, 43=Household goods retailing, 44=Other retailing, 45=Cafes restaurants and takeaway food services
- Parses `TIME_PERIOD` (format `yyyy-MM`) to `month` (date type)
- Casts `OBS_VALUE` to `turnover_millions` (double)
- Adds derived columns: `year` (int), `quarter` (int)
- Data quality expectations:
  - `valid_state`: `state IS NOT NULL`
  - `valid_industry`: `industry IS NOT NULL`
  - `valid_turnover`: `turnover_millions IS NOT NULL AND turnover_millions >= 0`

### AC-6: Silver — Food Price Index

**Table:** `food_price_index`

- Reads from `abs_cpi_food_bronze`
- Decodes `REGION` codes to full state names (same mapping as AC-5)
- Decodes `INDEX` codes: 10001=All groups CPI, 20001=Food and non-alcoholic beverages
- Output columns: `state`, `index_category`, `quarter` (string, original TIME_PERIOD), `cpi_index` (double)
- Data quality expectations:
  - `valid_state`: `state IS NOT NULL`
  - `valid_cpi`: `cpi_index IS NOT NULL AND cpi_index > 0`

### AC-7: Silver — Food Recalls

**Table:** `food_recalls`

- Reads from `fsanz_recalls_bronze`
- Parses `publication_date` to `recall_date` (date type)
- Extracts `company` from title using regex: text before first dash/en-dash
- Extracts `product_name` from title using regex: text after first dash/en-dash
- Preserves `title` as `recall_title` and `full_text` as `description`
- Extracts `affected_states` by scanning description text for state names and abbreviations (NSW, VIC, QLD, SA, WA, TAS, NT, ACT), returns comma-separated string
- Data quality expectations:
  - `valid_date`: `recall_date IS NOT NULL`

### AC-8: Silver — Grocery Documents

**Table:** `grocery_documents`

- Reads from `grocery_pdfs_bronze`
- Extracts text from each PDF page using `pypdf.PdfReader`
- Explodes pages into one row per page using `posexplode`
- Page numbers are 1-indexed
- Filters out null or empty page text
- Output columns: `filename`, `source_path`, `file_size_bytes`, `page_number` (int), `page_text` (string)
- Table comment: "Page-level text extracted from grocery industry PDF reports for Genie Q&A"
- Data quality expectations:
  - `has_text`: `page_text IS NOT NULL AND length(page_text) > 10`

### AC-9: Gold — Retail Summary

**Materialized view:** `retail_summary`

- Reads from `retail_turnover`
- Adds `turnover_3m_avg`: 3-month rolling average of `turnover_millions`, partitioned by (state, industry), ordered by month, window rows -2 to 0, rounded to 2 decimal places
- Adds `turnover_12m_avg`: 12-month rolling average, same partitioning, window rows -11 to 0, rounded to 2 decimal places
- Adds `yoy_growth_pct`: year-over-year growth percentage = `(current - 12_months_ago) / 12_months_ago * 100`, rounded to 2 decimal places, using `lag(turnover_millions, 12)`
- Drops intermediate `turnover_12m_ago` column

### AC-10: Gold — Food Inflation YoY

**Materialized view:** `food_inflation_yoy`

- Reads from `food_price_index`
- Adds `yoy_change_pct`: year-over-year CPI change = `(current - 4_quarters_ago) / 4_quarters_ago * 100`, rounded to 2 decimal places, using `lag(cpi_index, 4)` partitioned by (state, index_category), ordered by quarter
- Drops intermediate `cpi_index_4q_ago` column

### AC-11: Deployment — Databricks Asset Bundles

- `databricks.yml` defines bundle with parameterised `catalog` and `schema` variables
- Dev target: development mode, uses workspace profile
- Prod target: production mode
- Pipeline resource YAML includes all bronze, silver, and gold notebooks
- Pipeline is serverless with CURRENT channel
- Supports variable override: `databricks bundle deploy -t dev --var="catalog=X" --var="schema=Y"`

### AC-12: Data Collection Jobs

- **FSANZ scraper job**: Fetches recall listing pages (up to 20 pages × 100 items), scrapes detail pages with 10 concurrent workers, writes individual JSON files to UC Volume, creates `_manifest.json` with scrape timestamp and counts
- **PDF downloader job**: Downloads 5 specific PDFs (ACCC Supermarkets Inquiry reports + Coles annual/sustainability reports), writes binary to UC Volume, creates `_manifest.json` with download status and file sizes

---

## Agent Handoff

### Gate tests to generate

| AC | Test name | Verification method |
|----|-----------|-------------------|
| AC-1 | `test_bronze_retail_trade_schema` | Assert expected columns exist, OBS_VALUE/TIME_PERIOD/REGION are non-null |
| AC-1 | `test_bronze_retail_trade_data_range` | Assert data starts from 2010, contains all 8 states |
| AC-2 | `test_bronze_cpi_food_schema` | Assert expected columns, non-null OBS_VALUE/TIME_PERIOD |
| AC-2 | `test_bronze_cpi_food_categories` | Assert both index categories present (10001, 20001) |
| AC-3 | `test_bronze_fsanz_recalls_schema` | Assert schema matches (url, title, full_text, publication_date, error, source_file) |
| AC-3 | `test_bronze_fsanz_filters_nulls` | Given input with null url rows, assert they are filtered out |
| AC-4 | `test_bronze_pdfs_binary_read` | Assert filename extracted, length > 0, content is binary |
| AC-5 | `test_silver_retail_state_decode` | Given REGION=1, assert state="New South Wales" |
| AC-5 | `test_silver_retail_industry_decode` | Given INDUSTRY=20, assert industry="Food retailing" |
| AC-5 | `test_silver_retail_date_parse` | Given TIME_PERIOD="2024-01", assert month is date 2024-01-01 |
| AC-5 | `test_silver_retail_turnover_cast` | Assert turnover_millions is double, non-negative |
| AC-6 | `test_silver_cpi_state_decode` | Given REGION=2, assert state="Victoria" |
| AC-6 | `test_silver_cpi_index_decode` | Given INDEX=20001, assert index_category="Food and non-alcoholic beverages" |
| AC-7 | `test_silver_recalls_title_parse` | Given title "Company X – Product Y", assert company="Company X", product="Product Y" |
| AC-7 | `test_silver_recalls_state_extract` | Given description mentioning "NSW" and "Victoria", assert affected_states contains both |
| AC-8 | `test_silver_documents_page_extract` | Assert one row per page, page_number starts at 1, page_text is non-empty |
| AC-9 | `test_gold_retail_3m_rolling_avg` | Given 3 months of data, assert 3m average is correct |
| AC-9 | `test_gold_retail_12m_rolling_avg` | Given 12 months of data, assert 12m average is correct |
| AC-9 | `test_gold_retail_yoy_growth` | Given month with value 110 and 12-months-ago value 100, assert yoy_growth_pct=10.0 |
| AC-10 | `test_gold_inflation_yoy_change` | Given quarter with CPI 130 and 4-quarters-ago CPI 120, assert yoy_change_pct=8.33 |
| AC-11 | `test_dab_config_valid` | Assert databricks.yml parses, contains dev and prod targets |
| AC-12 | `test_fsanz_scraper_writes_json` | Assert JSON files written to volume path with expected keys |
| AC-12 | `test_pdf_downloader_writes_files` | Assert PDFs written to volume path, manifest created |

### Implementation order

1. Jobs first (AC-12) — FSANZ scraper + PDF downloader populate UC Volumes
2. Bronze layer (AC-1 through AC-4) — raw ingestion from APIs and volumes
3. Silver layer (AC-5 through AC-8) — decode, clean, enrich, extract
4. Gold layer (AC-9, AC-10) — rolling averages and YoY calculations
5. DABs deployment (AC-11) — bundle config and resource YAMLs

### Key constraints

- All pipeline notebooks use `from pyspark import pipelines as dp`
- Bronze tables use `@dp.table`, Gold uses `@dp.materialized_view`
- All tables include `comment` parameter for Genie discoverability
- Data quality enforced via `@dp.expect()` decorators
- Catalog and schema are parameterised via `spark.conf.get("pipelines.catalog")` for volume paths
- UC Volume paths follow pattern: `/Volumes/{catalog}/{schema}/raw_data/{source}/`
- PDF text extraction uses `pypdf` library (available on serverless compute)
