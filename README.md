# Coles Genie Demo

Databricks Asset Bundle project that ingests Australian Bureau of Statistics (ABS)
retail and CPI data via Lakeflow Declarative Pipelines for use with Databricks Genie.

## Project structure

```
├── databricks.yml                  # Bundle configuration
├── resources/
│   └── abs_retail_pipeline.yml     # Pipeline resource definition
└── src/
    ├── bronze/                     # Raw ingestion from ABS SDMX APIs
    │   ├── abs_retail_trade.py
    │   └── abs_cpi_food.py
    ├── silver/                     # Cleaned and enriched tables
    │   ├── retail_turnover.py
    │   └── food_price_index.py
    └── gold/                       # Analytics-ready materialized views
        ├── retail_summary.py
        └── food_inflation.py
```

## Pipeline layers

| Layer | Table | Type | Source |
|-------|-------|------|--------|
| Bronze | `abs_retail_trade_bronze` | Table | ABS Retail Trade SDMX API — monthly turnover by state and industry |
| Bronze | `abs_cpi_food_bronze` | Table | ABS CPI SDMX API — quarterly food price index by state |
| Silver | `retail_turnover` | Table | Cleaned retail data with human-readable state/industry names |
| Silver | `food_price_index` | Table | Cleaned CPI data with human-readable labels |
| Gold | `retail_summary` | Materialized View | Monthly retail with 3m/12m rolling averages and YoY growth |
| Gold | `food_inflation_yoy` | Materialized View | Year-over-year CPI change by food category and state |

## Deployment

```bash
# Validate
databricks bundle validate

# Deploy to dev
databricks bundle deploy -t dev

# Run pipeline
databricks bundle run abs_retail_pipeline -t dev

# Deploy to prod
databricks bundle deploy -t prod
```

## Configuration

Target catalog and schema are parameterised in `databricks.yml`. Override with:

```bash
databricks bundle deploy -t dev --var="catalog=my_catalog" --var="schema=my_schema"
```
