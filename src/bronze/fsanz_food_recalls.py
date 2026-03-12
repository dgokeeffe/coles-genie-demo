# Databricks notebook source

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

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
    recall_schema = StructType([
        StructField("url", StringType()),
        StructField("title", StringType()),
        StructField("full_text", StringType()),
        StructField("publication_date", StringType()),
        StructField("error", StringType()),
    ])
    return (
        spark.read.option("multiLine", "true")
        .option("pathGlobFilter", "[!_]*.json")
        .schema(recall_schema)
        .json(VOLUME_PATH)
        .filter(col("url").isNotNull())
        .withColumn("source_file", col("_metadata.file_path"))
    )
