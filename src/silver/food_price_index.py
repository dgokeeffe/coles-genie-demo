# Databricks notebook source

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit

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

@dp.table(
    name="food_price_index",
    comment="Quarterly CPI food price index by Australian state and category",
)
@dp.expect("valid_state", "state IS NOT NULL")
@dp.expect("valid_cpi", "cpi_index IS NOT NULL AND cpi_index > 0")
def food_price_index():
    df = dp.read("abs_cpi_food_bronze")
    return df.select(
        _map_column("REGION", STATE_MAP).alias("state"),
        _map_column("CPI_MEASURE", FOOD_CATEGORY_MAP).alias("food_category"),
        col("TIME_PERIOD").alias("quarter"),
        col("OBS_VALUE").cast("double").alias("cpi_index"),
    )
