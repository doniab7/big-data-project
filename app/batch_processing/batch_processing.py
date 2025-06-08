from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, to_timestamp, explode, split, count, desc, when, length
from pyspark.sql.types import FloatType
from textblob import TextBlob
import json
from datetime import datetime

# Custom JSON encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("YouTubeCommentsProcessingWithJSONExport") \
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
).withColumn(
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

# 5. Hate Speech Detection
hate_keywords = ["hate", "stupid", "idiot", "kill", "die", "dead", "ugly", "dumb"]
def detect_hate_speech(text):
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in hate_keywords)

hate_speech_udf = spark.udf.register("hate_speech_udf", detect_hate_speech)
hate_speech_df = processed_df.withColumn("is_hate_speech", hate_speech_udf(col("comment")))

# 6. Word Count Analysis
stop_words = ["the", "and", "a", "an", "to", "in", "it", "is", "that", "of", "for"]
words_df = processed_df.select(
    explode(split(col("comment"), " ")).alias("word")
).groupBy("word") \
 .agg(count("*").alias("count")) \
 .filter((length(col("word")) > 2) & (~col("word").isin(stop_words))) \
 .orderBy(desc("count"))

# Save word count results
words_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(wordcount_path)

# 7. Prepare JSON results
results = {
    # Processed comments sample
    "processed_comments": [
        {k: v.isoformat() if isinstance(v, datetime) else v 
         for k, v in row.asDict().items()}
        for row in processed_df.limit(10).collect()
    ],
    
    # Sentiment analysis samples
    "sentiment_samples": [
        {k: v.isoformat() if isinstance(v, datetime) else v 
         for k, v in row.asDict().items()}
        for row in sentiment_df.select(
            "username", "comment", "sentiment_score", "sentiment"
        ).limit(10).collect()
    ],
    
    # Sentiment distribution
    "sentiment_distribution": {
        row["sentiment"]: row["count"] 
        for row in sentiment_df.groupBy("sentiment").count().collect()
    },
    
    # Top words
    "top_words": [
        {"word": row["word"], "count": row["count"]}
        for row in words_df.limit(20).collect()
    ],
    
    # Hate speech metrics
    "hate_speech_metrics": {
        "total_comments": processed_df.count(),
        "hate_speech_count": hate_speech_df.filter(col("is_hate_speech") == True).count(),
        "hate_speech_percentage": round((hate_speech_df.filter(col("is_hate_speech") == True).count() / processed_df.count()) * 100, 2)
    },
    
    # Comment length statistics
    "comment_length_stats": {
        "count": processed_df.select(length(col("comment"))).count(),
        "mean": float(processed_df.select(length(col("comment"))).agg({"length(comment)": "avg"}).collect()[0][0]),
        "stddev": float(processed_df.select(length(col("comment"))).agg({"length(comment)": "stddev"}).collect()[0][0]),
        "min": int(processed_df.select(length(col("comment"))).agg({"length(comment)": "min"}).collect()[0][0]),
        "max": int(processed_df.select(length(col("comment"))).agg({"length(comment)": "max"}).collect()[0][0])
    }
}

# 8. Save JSON results
output_path = "/app/batch_processing/results.json"
with open(output_path, "w") as f:
    json.dump(results, f, indent=2, cls=DateTimeEncoder)

print("\n=== Processing Complete ===")
print(f"Results saved to {output_path}")

# 9. Print summary to console
print("\n=== Summary Statistics ===")
print(f"Total comments processed: {results['hate_speech_metrics']['total_comments']}")
print(f"Positive comments: {results['sentiment_distribution'].get('positive', 0)}")
print(f"Negative comments: {results['sentiment_distribution'].get('negative', 0)}")
print(f"Hate speech percentage: {results['hate_speech_metrics']['hate_speech_percentage']}%")
print("\nTop 5 words:")
for word in results['top_words'][:5]:
    print(f"{word['word']}: {word['count']}")

spark.stop()