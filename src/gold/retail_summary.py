# Databricks notebook source

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, avg, lag, round
from pyspark.sql.window import Window

# COMMAND ----------

@dp.materialized_view(
    name="retail_summary",
    comment="Monthly retail summary aggregated by state and industry with rolling averages and YoY growth",
)
def retail_summary():
    df = dp.read("retail_turnover")

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
