# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Databricks Asset Bundle (DAB) project: Lakeflow Declarative Pipeline ingesting 4 Australian public data sources through Bronze → Silver → Gold for Databricks Genie and AI/BI dashboards.

## Commands

```bash
# Validate bundle config
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev

# Run the pipeline
databricks bundle run abs_retail_pipeline -t dev

# Run data collection jobs (must run before pipeline if volumes are empty)
databricks bundle run scrape_fsanz_recalls -t dev
databricks bundle run download_grocery_pdfs -t dev

# Override catalog/schema
databricks bundle deploy -t dev --var="catalog=my_catalog" --var="schema=my_schema"
```

## Architecture

All pipeline code is Databricks notebooks using `from pyspark import pipelines as dp`. No local Python environment — runs on serverless compute.

**Pipeline layers:**
- **Bronze** (`src/bronze/`) — raw ingestion. Two patterns: (1) SDMX API → `requests` + `pandas` → Spark DF, (2) UC Volume files → `spark.read.format("binaryFile")` or `.json()` with glob `[!_]*.ext`
- **Silver** (`src/silver/`) — decode codes to labels, parse dates, extract text. All read upstream via `dp.read("bronze_table")`
- **Gold** (`src/gold/`) — materialized views with window functions (rolling averages, lag for YoY). Use `@dp.materialized_view` not `@dp.table`
- **Jobs** (`src/jobs/`) — standalone data collection. Parameterised via `dbutils.widgets`, create schema/volume if not exists, write files + `_manifest.json`

**Key convention — column mapping:** Silver tables decode ABS codes using a dynamic `_map_column()` helper that builds chained `when().when()...otherwise()` expressions from a dictionary. Don't use `case/when` SQL or joins — follow this pattern.

**UC Volume path construction:** Always use `spark.conf.get("pipelines.catalog")` and `spark.conf.get("pipelines.schema")` with fallback defaults. Never hardcode catalog/schema in notebook code.

## Data Quality

Every table uses `@dp.expect("name", "SQL condition")` decorators. Follow the existing pattern:
- Bronze: null checks on key columns (`OBS_VALUE IS NOT NULL`)
- Silver: null checks on decoded columns + value range checks (`turnover_millions >= 0`, `cpi_index > 0`)
- Gold: no explicit expectations (materialized views)

## Data Sources

| Source | Bronze Table | Type |
|--------|-------------|------|
| ABS Retail Trade SDMX API | `abs_retail_trade_bronze` | Monthly turnover by state (1-8) and industry (20, 41-45) from 2010 |
| ABS CPI Food SDMX API | `abs_cpi_food_bronze` | Quarterly CPI by state, categories 10001 (All groups) and 20001 (Food) |
| FSANZ food recalls | `fsanz_recalls_bronze` | JSON files scraped to `/Volumes/{cat}/{sch}/raw_data/fsanz_recalls/` |
| ACCC/Coles PDFs | `grocery_pdfs_bronze` | Binary PDFs downloaded to `/Volumes/{cat}/{sch}/raw_data/grocery_pdfs/` |

## State/Industry Code Mappings

States: 1=NSW, 2=VIC, 3=QLD, 4=SA, 5=WA, 6=TAS, 7=NT, 8=ACT
Industries: 20=Food retailing, 41=Clothing/footwear, 42=Department stores, 43=Household goods, 44=Other retailing, 45=Cafes/restaurants
CPI indices: 10001=All groups CPI, 20001=Food and non-alcoholic beverages

## Gold Layer Calculations

- `retail_summary`: 3-month rolling avg (rows -2 to 0), 12-month rolling avg (rows -11 to 0), YoY growth % via `lag(12)`. All partitioned by (state, industry), ordered by month, rounded to 2 decimals.
- `food_inflation_yoy`: YoY CPI change via `lag(4)` partitioned by (state, index_category), ordered by quarter, rounded to 2 decimals.

## Deployment Targets

- **dev**: catalog `daveok`, schema `coles_genie_demo`, workspace profile `daveok`
- **prod**: catalog `david_okeeffe`, schema `coles_genie_demo_prod`
