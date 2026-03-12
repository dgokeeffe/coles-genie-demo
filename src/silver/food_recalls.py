# Databricks notebook source

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, regexp_extract, to_date, when, lit, lower, concat_ws,
)

# COMMAND ----------

STATE_ABBREV_TO_FULL = {
    "NSW": "New South Wales",
    "VIC": "Victoria",
    "QLD": "Queensland",
    "SA": "South Australia",
    "WA": "Western Australia",
    "TAS": "Tasmania",
    "NT": "Northern Territory",
    "ACT": "Australian Capital Territory",
}

# COMMAND ----------

@dp.table(
    name="food_recalls",
    comment="FSANZ food recall notices enriched with extracted company, product, hazard, and affected states",
)
@dp.expect("valid_date", "recall_date IS NOT NULL")
def food_recalls():
    df = dp.read("fsanz_recalls_bronze")

    parsed = df.select(
        to_date(col("publication_date")).alias("recall_date"),
        regexp_extract("title", r"^(.+?)\s*[-–]\s*", 1).alias("company"),
        regexp_extract("title", r"[-–]\s*(.+)$", 1).alias("product_name"),
        col("title").alias("recall_title"),
        col("full_text").alias("description"),
        col("url").alias("source_url"),
    )

    # Extract affected states from description text
    for abbrev, full_name in STATE_ABBREV_TO_FULL.items():
        parsed = parsed.withColumn(
            f"_has_{abbrev}",
            when(
                lower(col("description")).contains(full_name.lower())
                | col("description").contains(abbrev),
                lit(abbrev),
            ),
        )

    state_cols = [f"_has_{a}" for a in STATE_ABBREV_TO_FULL]
    parsed = parsed.withColumn(
        "affected_states",
        concat_ws(", ", *[col(c) for c in state_cols]),
    )

    return parsed.drop(*state_cols)
