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
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from sparknlp.annotator import (
    LemmatizerModel,
    Normalizer,
    SentenceDetector,
    StopWordsCleaner,
    Tokenizer,
)
from sparknlp.base import DocumentAssembler

SPARK_APP_NAME = "Python-Spark-Log-Analysis"
SPARK_MASTER = "local[*]"
SPARK_NLP_VERSION = "2.12:5.2.2"
SPARK_SHOW_NUM = 2


def pretrained(model=LemmatizerModel, name="lemma_antbnc", lang="en"):
    """Load or download pretrained models."""
    try:
        cache_path = Path("~/cache_pretrained").expanduser().resolve()
        models_path = cache_path.glob(f"{name}_{lang}*")
        model_path = str(next(iter(models_path), None))
        return model.load(str(model_path))
    except Exception:
        return model.pretrained(name=name, lang=lang)


# Start Spark session
spark = (
    SparkSession.builder.appName(SPARK_APP_NAME)
    .master(SPARK_MASTER)
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.jars.packages", f"com.johnsnowlabs.nlp:spark-nlp_{SPARK_NLP_VERSION}")
    .getOrCreate()
)


# Load structure logs
structured_logs_path = "data/interim/structured-logs"
df = spark.read.parquet(structured_logs_path).select("log_message")


# Define word count NLP pipeline stages
documentizer = DocumentAssembler().setInputCol("log_message").setOutputCol("document")  # prepares data for NLP
sentencizer = SentenceDetector().setInputCols(["document"]).setOutputCol("sentences")  # detects sentence boundaries
tokenizer = Tokenizer().setInputCols(["sentences"]).setOutputCol("tokens")  # tokenizes raw text
normalizer = (  # cleans out tokens
    Normalizer().setInputCols(["tokens"]).setOutputCol("normalized_tokens").setLowercase(True)
)
lemmatizer = (  # find lemmas out of tokens
    pretrained(model=LemmatizerModel, name="lemma_antbnc", lang="en")
    .setInputCols(["normalized_tokens"])
    .setOutputCol("lemmatized_tokens")
)
cleaner = (  # drops all the stop words
    pretrained(model=StopWordsCleaner, name="stopwords_en", lang="en")
    .setInputCols(["lemmatized_tokens"])
    .setOutputCol("cleaned_tokens")
    .setCaseSensitive(False)
)

# Create word count NLP pipeline
pipeline = Pipeline(stages=[documentizer, sentencizer, tokenizer, normalizer, lemmatizer, cleaner])

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
