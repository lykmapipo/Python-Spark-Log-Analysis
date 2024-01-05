"""Count word on structured log messages.

This script perform word frequency analysis on the `log_message`
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
    $ pip install pandas pyarrow pyspark[sql] spark-nlp
    $ python count_words.py

"""

import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from sparknlp.annotator import LemmatizerModel, Normalizer, StopWordsCleaner, Tokenizer
from sparknlp.base import DocumentAssembler

SPARK_APP_NAME = "Python-Spark-Log-Analysis"
SPARK_MASTER = "local[*]"
SPARK_SHOW_NUM = 2


# Start Spark session
spark = (
    SparkSession.builder.appName(SPARK_APP_NAME)
    .master(SPARK_MASTER)
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.2.2")
    .getOrCreate()
)


# Load structure logs
structured_logs_path = "data/interim/structured-logs"
df = spark.read.parquet(structured_logs_path).select("log_message")


# Define word count NLP pipeline stages
assembler = DocumentAssembler().setInputCol("log_message").setOutputCol("document")
tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("tokens")
normalizer = Normalizer().setInputCols(["tokens"]).setOutputCol("normalized_tokens").setLowercase(True)
lemmatizer = (
    LemmatizerModel.pretrained(name="lemma_antbnc", lang="en")
    .setInputCols(["normalized_tokens"])
    .setOutputCol("lemmatized_tokens")
)
cleaner = StopWordsCleaner().setInputCols(["lemmatized_tokens"]).setOutputCol("cleaned_tokens").setCaseSensitive(False)

# Create word count NLP pipeline
pipeline = Pipeline(stages=[assembler, tokenizer, normalizer, lemmatizer, cleaner])

# Fit word count NLP pipeline and tranform the log message input dataframe
analysis_df = pipeline.fit(df).transform(df)

# Count words of log messages
word_count_df = (
    analysis_df.select(F.explode(F.col("cleaned_tokens.result")).alias("word"))
    .groupBy(F.col("word"))
    .count()
    .alias("count")
    .orderBy(F.col("count"), ascending=False)
)

print("\nTop 20 word in log entry messages:")
word_count_df.show(n=20, truncate=False)


# Write word count report dataframe
word_count_path = "data/reports/word_count.csv"
print(f"Write word count report dataframe at {word_count_path}")
word_count_df.toPandas().to_csv(word_count_path, header=True, index=False)


# Stop Spark session
spark.stop()
