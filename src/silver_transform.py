# Databricks notebook source

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, to_date, year, quarter, concat, lit

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

FOOD_CATEGORY_MAP = {
    "10": "Food",
    "20": "Non-food",
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

@dlt.table(
    name="retail_turnover",
    comment="Monthly retail turnover by Australian state and industry, cleaned and enriched",
)
def retail_turnover():
    df = dlt.read("abs_retail_trade_bronze")
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

# COMMAND ----------

@dlt.table(
    name="food_price_index",
    comment="Quarterly CPI food price index by Australian state and category",
)
def food_price_index():
    df = dlt.read("abs_cpi_food_bronze")
    return df.select(
        _map_column("REGION", STATE_MAP).alias("state"),
        _map_column("CPI_MEASURE", FOOD_CATEGORY_MAP).alias("food_category"),
        col("TIME_PERIOD").alias("quarter"),
        col("OBS_VALUE").cast("double").alias("cpi_index"),
    )
