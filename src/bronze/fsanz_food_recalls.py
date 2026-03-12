# Databricks notebook source

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, input_file_name

# COMMAND ----------

catalog = spark.conf.get("pipelines.catalog", "daveok")
schema = spark.conf.get("pipelines.schema", "coles_genie_demo")
VOLUME_PATH = f"/Volumes/{catalog}/{schema}/raw_data/fsanz_recalls"

# COMMAND ----------

@dp.table(
    name="fsanz_recalls_bronze",
    comment="FSANZ food recall notices, raw JSON ingested from Unity Catalog volume",
)
@dp.expect("valid_url", "url IS NOT NULL")
@dp.expect("has_content", "full_text IS NOT NULL AND length(full_text) > 0")
def fsanz_recalls_bronze():
    return (
        spark.read.option("pathGlobFilter", "[!_]*.json").json(VOLUME_PATH)
        .filter(col("url").isNotNull())
        .withColumn("source_file", input_file_name())
    )
