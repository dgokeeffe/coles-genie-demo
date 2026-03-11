# Databricks notebook source

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum, avg, lag, round
from pyspark.sql.window import Window

# COMMAND ----------

@dlt.table(
    name="retail_summary",
    comment="Monthly retail summary aggregated by state and industry with rolling averages",
)
def retail_summary():
    df = dlt.read("retail_turnover")

    window_3m = (
        Window.partitionBy("state", "industry")
        .orderBy("month")
        .rowsBetween(-2, 0)
    )

    window_12m = (
        Window.partitionBy("state", "industry")
        .orderBy("month")
        .rowsBetween(-11, 0)
    )

    window_yoy = (
        Window.partitionBy("state", "industry")
        .orderBy("month")
    )

    return (
        df.withColumn("turnover_3m_avg", round(avg("turnover_millions").over(window_3m), 2))
        .withColumn("turnover_12m_avg", round(avg("turnover_millions").over(window_12m), 2))
        .withColumn(
            "turnover_12m_ago",
            lag("turnover_millions", 12).over(window_yoy),
        )
        .withColumn(
            "yoy_growth_pct",
            round(
                (col("turnover_millions") - col("turnover_12m_ago"))
                / col("turnover_12m_ago")
                * 100,
                2,
            ),
        )
        .drop("turnover_12m_ago")
    )

# COMMAND ----------

@dlt.table(
    name="food_inflation_yoy",
    comment="Year-over-year CPI change by food category and state",
)
def food_inflation_yoy():
    df = dlt.read("food_price_index")

    window_yoy = (
        Window.partitionBy("state", "food_category")
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
