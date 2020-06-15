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

# Create a temp table.
df = spark.read.json(os.getenv("DATA_FILE_PATH"))
df.createOrReplaceTempView("ratings")

# Query the temp table.
ratings_count_df = spark.sql(
    "select overall as rating, count(overall) as count from ratings group by overall order by count desc"
)

# Save the results to a permanent table.
ratings_count_df.write.format(os.getenv("RESULT_TABLE_FORMAT")).saveAsTable(
    os.getenv("RESULT_TABLE_NAME")
)
