"""Extract keywords from structured log messages.

This script perform keywords extraction analysis on the `log_message`
of the structured logs using YAKE algorithm.


Command-Line Interface (CLI) Usage:
    $ pip install pandas pyarrow pyspark[sql] spark-nlp
    $ python keywords.py

"""
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from sparknlp.annotator import (
    LemmatizerModel,
    Normalizer,
    SentenceDetectorDLModel,
    StopWordsCleaner,
    Tokenizer,
    YakeKeywordExtraction,
)
from sparknlp.base import DocumentAssembler

SPARK_APP_NAME = "Python-Spark-Log-Analysis"
SPARK_MASTER = "local[*]"
SPARK_NLP_VERSION = "2.12:5.2.2"
SPARK_SHOW_NUM = 2


def pretrained(model=LemmatizerModel, name=None, lang="en"):
    """Load or download pretrained models."""
    try:
        cache_path = Path("~/cache_pretrained").expanduser().resolve()
        models_path = cache_path.glob(f"{name}_{lang}*")
        model_path = next(iter(models_path), None)
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
# TODO: include other fields to futher analysis


# Define keywords extraction NLP pipeline stages
documentizer = DocumentAssembler().setInputCol("log_message").setOutputCol("document")  # prepares data for NLP
sentencizer = (  # detects sentence boundaries
    pretrained(model=SentenceDetectorDLModel, name="sentence_detector_dl", lang="en")
    .setInputCols(["document"])
    .setOutputCol("sentences")
)
tokenizer = (  # tokenizes raw text
    Tokenizer().setInputCols(["sentences"]).setOutputCol("tokens").setSplitChars(["#", "=", "_", "/", "@", "."])
)
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

keywordizer = (  # Extract keywords
    YakeKeywordExtraction()
    .setInputCols(["cleaned_tokens"])
    .setOutputCol("keywords")
    .setMinNGrams(2)
    .setMaxNGrams(3)
    .setThreshold(-1)
    .setWindowSize(3)
    .setNKeywords(30)
)

# Create keywords extraction NLP pipeline
pipeline = Pipeline(stages=[documentizer, sentencizer, tokenizer, normalizer, lemmatizer, cleaner, keywordizer])

# Fit keyword extraction NLP pipeline and tranform the log message input dataframe
analysis_df = pipeline.fit(df).transform(df)

# Get extracted keywords of log messages
keywords_df = (
    analysis_df.select(
        F.col("log_message").alias("log_message"),
        F.col("keywords").alias("keywords"),
    )
    .select(
        F.col("log_message").alias("log_message"),
        F.explode(F.col("keywords")).alias("keywords"),
    )
    .select(
        F.col("log_message").alias("log_message"),
        F.col("keywords.result").alias("keywords"),
        F.col("keywords.metadata").getItem("score").alias("score"),
    )
)

print("\nSample keywords in each log entry messages:")
keywords_df.show(n=20, truncate=False)

# TODO: count unique keywords

# Write keywords extraction report dataframe
keywords_path = "data/reports/keywords.csv"
print(f"Write keywords report dataframe at {keywords_path}")
keywords_df.toPandas().to_csv(keywords_path, header=True, index=False)


# Stop Spark session
spark.stop()
