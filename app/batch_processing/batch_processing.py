from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, to_timestamp, explode, split, count, desc, when, length
from pyspark.sql.types import FloatType
from textblob import TextBlob
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("EnhancedYouTubeCommentsProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "kafka:9092"
kafka_topic = "youtube-comments-batch"

# HDFS paths
hdfs_namenode = "hdfs://namenode:9000"
raw_path = f"{hdfs_namenode}/data/youtube_comments/raw"
processed_path = f"{hdfs_namenode}/data/youtube_comments/processed"
batch_output_path = f"{hdfs_namenode}/data/youtube_comments/batch_results"
sentiment_path = f"{hdfs_namenode}/data/youtube_comments/sentiment_analysis"
wordcount_path = f"{hdfs_namenode}/data/youtube_comments/word_counts"

# 1. Read from Kafka
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Convert binary value to string
comments_df = df.selectExpr("CAST(value AS STRING)")

# 2. Save raw data to HDFS
comments_df.write \
    .mode("overwrite") \
    .format("text") \
    .save(raw_path)

# 3. Process the data (parse timestamp, username, and comment)
processed_df = comments_df.withColumn(
    "timestamp", 
    to_timestamp(
        regexp_extract(col("value"), r"^\[(.*?)\]", 1), 
        "yyyy-MM-dd HH:mm:ss"
    )
).withColumn(
    "username",
    regexp_extract(col("value"), r"^\[.*?\] (.*?):", 1)
).withColumn(
    "comment",
    regexp_extract(col("value"), r"^\[.*?\] .*?: (.*)", 1)
).drop("value")

# Save processed data
processed_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(processed_path)

# 4. Sentiment Analysis
def analyze_sentiment(text):
    try:
        return TextBlob(text).sentiment.polarity
    except:
        return 0.0

sentiment_udf = spark.udf.register("sentiment_udf", analyze_sentiment, FloatType())

sentiment_df = processed_df.withColumn(
    "sentiment_score", 
    sentiment_udf(col("comment"))
)

# Classify sentiment
sentiment_df = sentiment_df.withColumn(
    "sentiment",
    when(col("sentiment_score") > 0.2, "positive")
    .when(col("sentiment_score") < -0.2, "negative")
    .otherwise("neutral")
)

# Save sentiment analysis results
sentiment_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(sentiment_path)

# 5. Hate Speech Detection (simple keyword-based approach)
hate_keywords = ["hate", "stupid", "idiot", "kill", "die", "dead", "ugly", "dumb"]  # Add more keywords

def detect_hate_speech(text):
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in hate_keywords)

hate_speech_udf = spark.udf.register("hate_speech_udf", detect_hate_speech)

hate_speech_df = processed_df.withColumn(
    "is_hate_speech", 
    hate_speech_udf(col("comment"))
)

# Calculate hate speech percentage
total_comments = hate_speech_df.count()
hate_comments = hate_speech_df.filter(col("is_hate_speech") == True).count()
hate_percentage = (hate_comments / total_comments) * 100

print(f"\n=== Hate Speech Analysis ===")
print(f"Total comments: {total_comments}")
print(f"Hate speech comments: {hate_comments}")
print(f"Hate speech percentage: {hate_percentage:.2f}%")

# 6. Word Count Analysis
words_df = processed_df.select(
    explode(split(col("comment"), " ")).alias("word")
).groupBy("word") \
 .agg(count("*").alias("count")) \
 .orderBy(desc("count"))

# Filter out stop words and short words
stop_words = ["the", "and", "a", "an", "to", "in", "it", "is", "that", "of", "for"]
words_df = words_df.filter(
    (length(col("word")) > 2) & 
    (~col("word").isin(stop_words))
)

# Save word count results
words_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(wordcount_path)

# 7. Show top results
print("\n=== Top 20 Most Frequent Words ===")
words_df.show(20, truncate=False)

print("\n=== Sentiment Distribution ===")
sentiment_df.groupBy("sentiment").count().show()

print("\n=== Top 10 Users by Comment Count ===")
processed_df.groupBy("username").count().orderBy(desc("count")).show(10)

spark.stop()