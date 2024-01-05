"""Generate structured logs summary reports.

This script perform basic statistics, aggregation and summarization
of the structured logs.

It uses the structured logs schema below:
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
    $ pip install pandas pyarrow pyspark[sql]
    $ python summarize.py

"""


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


# Load structure logs
structured_logs_path = "data/interim/structured"
df = spark.read.parquet(structured_logs_path)


# Create Spark view for structured logs
df.createOrReplaceTempView("structured_logs")

# Read summarize Spark SQL query
with open("summarize.sql", "r") as query_file:
    summary_query = query_file.read()

# Execute summary query
summary_df = spark.sql(summary_query)

# Print summary report dataframe
print("\nSummary reports:")
summary_df.show(truncate=False)


# Write summary report dataframe
summary_path = "data/reports/summary_report.csv"
print(f"Write summary report dataframe at {summary_path}")
summary_df.toPandas().to_csv(summary_path, header=True, index=False)


# Stop Spark session
spark.stop()
