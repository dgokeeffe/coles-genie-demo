# Coles Genie Demo

Lakeflow Declarative Pipeline that ingests Australian Bureau of Statistics (ABS) retail and CPI data for use with Databricks Genie.

## Pipeline layers

| Layer | Table | Source |
|-------|-------|--------|
| Bronze | `abs_retail_trade_bronze` | ABS Retail Trade SDMX API - monthly turnover by state and industry |
| Bronze | `abs_cpi_food_bronze` | ABS CPI SDMX API - quarterly food price index by state |
| Silver | `retail_turnover` | Cleaned retail data with human-readable state/industry names |
| Silver | `food_price_index` | Cleaned CPI data with human-readable labels |
| Gold | `retail_summary` | Monthly retail with 3-month/12-month rolling averages and YoY growth |
| Gold | `food_inflation_yoy` | Year-over-year CPI change by food category and state |

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
