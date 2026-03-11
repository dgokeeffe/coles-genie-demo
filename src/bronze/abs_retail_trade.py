# Databricks notebook source

# COMMAND ----------

from pyspark import pipelines as dp
import requests
import io
import pandas as pd

# COMMAND ----------

ABS_RETAIL_URL = (
    "https://data.api.abs.gov.au/data/ABS,RT,1.0.0/"
    "1.20+41+42+43+44+45.1+2+3+4+5+6+7+8.M"
    "?format=csv&startPeriod=2010-01"
)

# COMMAND ----------

@dp.table(
    name="abs_retail_trade_bronze",
    comment="ABS Retail Trade monthly turnover by state and industry, raw from SDMX API",
)
@dp.expect("valid_obs_value", "OBS_VALUE IS NOT NULL")
@dp.expect("valid_time_period", "TIME_PERIOD IS NOT NULL")
@dp.expect("valid_region", "REGION IS NOT NULL")
def abs_retail_trade_bronze():
    resp = requests.get(ABS_RETAIL_URL, timeout=120)
    resp.raise_for_status()
    pdf = pd.read_csv(io.StringIO(resp.text))
    return spark.createDataFrame(pdf)
