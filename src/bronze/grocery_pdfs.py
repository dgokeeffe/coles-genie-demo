# Databricks notebook source

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, regexp_extract

# COMMAND ----------

catalog = spark.conf.get("pipelines.catalog", "daveok")
schema = spark.conf.get("pipelines.schema", "coles_genie_demo")
VOLUME_PATH = f"/Volumes/{catalog}/{schema}/raw_data/grocery_pdfs"

# COMMAND ----------

@dp.table(
    name="grocery_pdfs_bronze",
    comment="Raw PDF binary files from grocery industry reports (ACCC, Coles Group)",
)
@dp.expect("valid_content", "length > 0")
def grocery_pdfs_bronze():
    return (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "[!_]*.pdf")
        .load(VOLUME_PATH)
        .withColumn(
            "filename",
            regexp_extract(col("path"), r"([^/]+)$", 1),
        )
        .select(
            col("path"),
            col("filename"),
            col("length"),
            col("modificationTime").alias("modified_at"),
            col("content"),
        )
    )
