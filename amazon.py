# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

from pyspark.sql import SparkSession
from dotenv import load_dotenv

import os

load_dotenv()

use_local = bool(os.getenv("USE_LOCAL"))
spark = SparkSession.builder.appName("Amazon books example")

if use_local:
	spark = spark.master("local").getOrCreate()
else:
	spark = spark.getOrCreate()

df = spark.read.json(os.getenv("DATA_FILE_PATH"))
df.createOrReplaceTempView("ratings")

ratings_count_df = spark.sql("select overall as rating, count(overall) as count from ratings group by overall order by count desc")
ratings_count_df.write.format("parquet").saveAsTable(os.getenv("RESULT_TABLE_NAME"))


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC select overall as rating, count(overall) as count from `musical_instruments` group by overall order by count desc 
