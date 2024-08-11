from pyspark.sql import SparkSession

import os
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CheckSparkVersion") \
    .getOrCreate()

# Get Spark version
spark_version = spark.version
print(f"Spark Version: {spark_version}")

# Stop the Spark session
spark.stop()