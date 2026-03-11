# Databricks notebook source

# COMMAND ----------

from pyspark import pipelines as dp
import requests
import io
import pandas as pd

# COMMAND ----------

ABS_CPI_FOOD_URL = (
    "https://data.api.abs.gov.au/data/ABS,CPI,1.0.0/"
    "1.10001.10+20.1+2+3+4+5+6+7+8.Q"
    "?format=csv&startPeriod=2010-Q1"
)

# COMMAND ----------

@dp.table(
    name="abs_cpi_food_bronze",
    comment="ABS CPI food categories by state, quarterly, raw from SDMX API",
)
@dp.expect("valid_obs_value", "OBS_VALUE IS NOT NULL")
@dp.expect("valid_time_period", "TIME_PERIOD IS NOT NULL")
def abs_cpi_food_bronze():
    resp = requests.get(ABS_CPI_FOOD_URL, timeout=120)
    resp.raise_for_status()
    pdf = pd.read_csv(io.StringIO(resp.text))
    return spark.createDataFrame(pdf)
