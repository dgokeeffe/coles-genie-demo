# Databricks notebook source

# COMMAND ----------

import dlt
import requests
import io
import pandas as pd

# COMMAND ----------

def _fetch_csv_as_df(spark, url):
    """Fetch CSV from ABS API and return a Spark DataFrame."""
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    pdf = pd.read_csv(io.StringIO(resp.text))
    return spark.createDataFrame(pdf)

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
    return _fetch_csv_as_df(spark, ABS_RETAIL_URL)

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
    return _fetch_csv_as_df(spark, ABS_CPI_FOOD_URL)
