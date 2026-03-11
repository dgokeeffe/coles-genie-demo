# Databricks notebook source

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, to_date, year, quarter, lit

# COMMAND ----------

STATE_MAP = {
    "1": "New South Wales",
    "2": "Victoria",
    "3": "Queensland",
    "4": "South Australia",
    "5": "Western Australia",
    "6": "Tasmania",
    "7": "Northern Territory",
    "8": "Australian Capital Territory",
}

INDUSTRY_MAP = {
    "20": "Food retailing",
    "41": "Clothing, footwear and personal accessory retailing",
    "42": "Department stores",
    "43": "Household goods retailing",
    "44": "Other retailing",
    "45": "Cafes, restaurants and takeaway food services",
}

# COMMAND ----------

def _map_column(col_name, mapping):
    """Build a chained WHEN expression to map codes to labels."""
    expr = None
    for code, label in mapping.items():
        condition = F.col(col_name) == code
        if expr is None:
            expr = when(condition, lit(label))
        else:
            expr = expr.when(condition, lit(label))
    return expr.otherwise(F.col(col_name))

# COMMAND ----------

@dp.table(
    name="retail_turnover",
    comment="Monthly retail turnover by Australian state and industry, cleaned and enriched",
)
@dp.expect("valid_state", "state IS NOT NULL")
@dp.expect("valid_industry", "industry IS NOT NULL")
@dp.expect("valid_turnover", "turnover_millions IS NOT NULL AND turnover_millions >= 0")
def retail_turnover():
    df = dp.read("abs_retail_trade_bronze")
    return (
        df.select(
            _map_column("REGION", STATE_MAP).alias("state"),
            _map_column("INDUSTRY", INDUSTRY_MAP).alias("industry"),
            to_date(col("TIME_PERIOD"), "yyyy-MM").alias("month"),
            col("OBS_VALUE").cast("double").alias("turnover_millions"),
        )
        .withColumn("year", year("month"))
        .withColumn("quarter", quarter("month"))
    )
