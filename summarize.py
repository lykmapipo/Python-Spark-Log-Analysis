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
SPARK_SHOW_NUM = 2


# Start Spark session
spark = (
    SparkSession.builder.appName(SPARK_APP_NAME)
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()
)


# Load structure logs
structured_logs_path = "data/interim/structured"
df = spark.read.parquet(structured_logs_path)

# Create a summary report
df.createOrReplaceTempView("structured_logs")
summary_df = spark.sql(
    """
    SELECT
      'Total number of log entries' AS ANALYSIS,
      COUNT(*) AS VALUE
    FROM structured_logs

    UNION

    SELECT
      'Average log entry length' AS ANALYSIS,
      AVG(log_length) AS VALUE
    FROM structured_logs

    UNION

    SELECT
      CONCAT('Total number of ', log_level, ' log entries') AS ANALYSIS,
      COUNT(*) AS VALUE
    FROM structured_logs
    GROUP BY log_level

    UNION

    SELECT
      'Average log message length' AS ANALYSIS,
      AVG(log_message_length) AS VALUE
    FROM structured_logs
    """
)

print("\nSummary reports:")
summary_df.show(truncate=False)


# Write summary report dataframe
summary_path = "data/reports/summary_report.csv"
print(f"Write summary report dataframe at {summary_path}")
summary_df.toPandas().to_csv(summary_path, header=True, index=False)


# Stop Spark session
spark.stop()
