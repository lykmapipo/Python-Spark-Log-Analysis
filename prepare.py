"""Prepare raw logs for analysis.

This script parse, format and normalize raw logs into a structured format.

It convert raw text log entry into structured logs schema below:
root
 |-- log_timestamp: timestamp (nullable = true)
 |-- log_level: string (nullable = true)
 |-- log_message: string (nullable = true)
 |-- log_length: integer (nullable = true)
 |-- log_year: integer (nullable = true)
 |-- log_month: integer (nullable = true)
 |-- log_day: integer (nullable = true)
 |-- log_hour: integer (nullable = true)
 |-- log_minute: integer (nullable = true)
 |-- log_second: integer (nullable = true)
 |-- log_message_length: integer (nullable = true)


Command-Line Interface (CLI) Usage:
    $ pip install pyarrow pyspark[sql]
    $ python prepare.py

"""

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

SPARK_APP_NAME = "Python-Spark-Log-Analysis"
SPARK_MASTER = "local[*]"
SPARK_SHOW_NUM = 2


# Start Spark session
spark = (
    SparkSession.builder.appName(SPARK_APP_NAME)
    .master(SPARK_MASTER)
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


# Extract and cast log entry timestamp, level, message and length into
# their respective data types
df = df.select(
    F.regexp_extract(F.col("text"), r"\[(.*?)\]", 1).alias("log_timestamp").cast(T.TimestampType()),
    F.regexp_extract(F.col("text"), r"\] (\w+):", 1).alias("log_level").cast(T.StringType()),
    F.regexp_extract(F.col("text"), r": (.*)", 1).alias("log_message").cast(T.StringType()),
    F.length("text").alias("log_length").cast(T.IntegerType()),
)

# Extract log entry year, month, dayofmonth, hour, minute and seconds from timestamp
# and length of the log message
df = (
    df.withColumn("log_year", F.year(F.col("log_timestamp")))
    .withColumn("log_month", F.month(F.col("log_timestamp")))
    .withColumn("log_day", F.dayofmonth(F.col("log_timestamp")))
    .withColumn("log_hour", F.hour(F.col("log_timestamp")))
    .withColumn("log_minute", F.minute(F.col("log_timestamp")))
    .withColumn("log_second", F.second(F.col("log_timestamp")))
    .withColumn("log_message_length", F.length(F.col("log_message")))
)
# TODO: use timestamp as log_id

# Print stuctured logs dataframe schema
print("\n\nStructured logs dataframe schema:")
df.printSchema()

# Print number of structure log entries
print("\nStructured log entries number:", df.count())

# Print top N structured log entries
print(f"\nTop {SPARK_SHOW_NUM} stuctured log entries:")
df.show(n=SPARK_SHOW_NUM, truncate=False)


# Write structured logs dataframe
structured_logs_path = "data/interim/structured-logs"
print(f"Write structured logs dataframe at {structured_logs_path}")
df.write.mode("overwrite").parquet(structured_logs_path)


# Stop Spark session
spark.stop()
