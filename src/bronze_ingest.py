# Databricks notebook source

# COMMAND ----------

import dlt
import requests
import tempfile
import os
from pyspark.sql.types import *

# COMMAND ----------

def _download_csv(url, filename):
    """Download CSV from ABS API to a temp file and return the path."""
    tmp_path = os.path.join(tempfile.gettempdir(), filename)
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    with open(tmp_path, "w") as f:
        f.write(resp.text)
    return tmp_path

# COMMAND ----------

ABS_RETAIL_URL = (
    "https://data.api.abs.gov.au/data/ABS,RT,1.0.0/"
    "1.20+41+42+43+44+45.1+2+3+4+5+6+7+8.M"
    "?format=csv&startPeriod=2010-01"
)

@dlt.table(
    name="abs_retail_trade_bronze",
    comment="ABS Retail Trade monthly turnover by state and industry, raw from SDMX API",
)
def abs_retail_trade_bronze():
    tmp_path = _download_csv(ABS_RETAIL_URL, "abs_retail_trade.csv")
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"file:{tmp_path}")
    )

# COMMAND ----------

ABS_CPI_FOOD_URL = (
    "https://data.api.abs.gov.au/data/ABS,CPI,1.0.0/"
    "1.10001.10+20.1+2+3+4+5+6+7+8.Q"
    "?format=csv&startPeriod=2010-Q1"
)

@dlt.table(
    name="abs_cpi_food_bronze",
    comment="ABS CPI food categories by state, quarterly, raw from SDMX API",
)
def abs_cpi_food_bronze():
    tmp_path = _download_csv(ABS_CPI_FOOD_URL, "abs_cpi_food.csv")
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"file:{tmp_path}")
    )
