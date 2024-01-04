"""Prepare raw logs for analysis.

This script parse, format and normalize raw logs into a structured format.

It convert raw text log entry into structured logs schema below:
root
 |-- timestamp: timestamp (nullable = true)
 |-- level: string (nullable = true)
 |-- message: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- hour: integer (nullable = true)
 |-- minute: integer (nullable = true)
 |-- second: integer (nullable = true)


Command-Line Interface (CLI) Usage:
    $ pip install pyspark[sql]
    $ python prepare.py

"""

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

SPARK_APP_NAME = "Python-Spark-Log-Analysis"
SPARK_SHOW_NUM = 2

# Start Spark session
spark = (
    SparkSession.builder.appName(SPARK_APP_NAME)
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()
)


# Load raw logs data from text files into a DataFrame
raw_logs_path = "data/raw/*.txt"
df = spark.read.text(raw_logs_path).withColumnRenamed("value", "text")

# Print raw logs dataframe schema
print("\nRaw logs dataframe schema:")
df.printSchema()

# Print number of raw log entries
print("\nRaw log entries number:", df.count())

# Print top N raw log entries
print(f"\nTop {SPARK_SHOW_NUM} raw log entries:")
df.show(n=SPARK_SHOW_NUM, truncate=False)


# Extract and cast log entry timestamp, level and message into their respective data types
df = df.select(
    F.regexp_extract(F.col("text"), r"\[(.*?)\]", 1).alias("timestamp").cast(T.TimestampType()),
    F.regexp_extract(F.col("text"), r"\] (\w+):", 1).alias("level").cast(T.StringType()),
    F.regexp_extract(F.col("text"), r": (.*)", 1).alias("message").cast(T.StringType()),
)

# Extract log entry year, month, dayofmonth, hour, minute and seconds from timestamp
df = (
    df.withColumn("year", F.year(F.col("timestamp")))
    .withColumn("month", F.month(F.col("timestamp")))
    .withColumn("day", F.dayofmonth(F.col("timestamp")))
    .withColumn("hour", F.hour(F.col("timestamp")))
    .withColumn("minute", F.minute(F.col("timestamp")))
    .withColumn("second", F.second(F.col("timestamp")))
)

# Print stuctured logs dataframe schema
print("\n\nStructured logs dataframe schema:")
df.printSchema()

# Print number of structure log entries
print("\nStructured log entries number:", df.count())

# Print top N structured log entries
print(f"\nTop {SPARK_SHOW_NUM} stuctured log entries:")
df.show(n=SPARK_SHOW_NUM, truncate=False)

# Write structured logs dataframe
structured_logs_path = "data/interim/structured"
print(f"Write structured logs dataframe at {structured_logs_path}")
df.write.mode("overwrite").parquet(structured_logs_path)

# Stop Spark session
spark.stop()
