# Databricks notebook source

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, lag, round
from pyspark.sql.window import Window

# COMMAND ----------

@dp.materialized_view(
    name="food_inflation_yoy",
    comment="Year-over-year CPI change by food category and state",
)
def food_inflation_yoy():
    df = dp.read("food_price_index")

    window_yoy = (
        Window.partitionBy("state", "index_category")
        .orderBy("quarter")
    )

    return (
        df.withColumn(
            "cpi_index_4q_ago",
            lag("cpi_index", 4).over(window_yoy),
        )
        .withColumn(
            "yoy_change_pct",
            round(
                (col("cpi_index") - col("cpi_index_4q_ago"))
                / col("cpi_index_4q_ago")
                * 100,
                2,
            ),
        )
        .drop("cpi_index_4q_ago")
    )
