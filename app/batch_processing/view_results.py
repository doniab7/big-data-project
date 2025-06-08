from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col
import json
from datetime import datetime

# Custom JSON encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

spark = SparkSession.builder \
    .appName("ViewResults") \
    .getOrCreate()

# Read all result datasets
processed_df = spark.read.parquet("hdfs://namenode:9000/data/youtube_comments/processed")
sentiment_df = spark.read.parquet("hdfs://namenode:9000/data/youtube_comments/sentiment_analysis")
wordcount_df = spark.read.parquet("hdfs://namenode:9000/data/youtube_comments/word_counts")

# Create a dictionary to hold all results
results = {}

# 1. Processed Comments Sample - Convert timestamps to strings
results["processed_comments"] = [
    {k: v.isoformat() if isinstance(v, datetime) else v 
     for k, v in row.asDict().items()}
    for row in processed_df.limit(10).collect()
]

# 2. Sentiment Analysis Results - Convert timestamps to strings
results["sentiment_samples"] = [
    {k: v.isoformat() if isinstance(v, datetime) else v 
     for k, v in row.asDict().items()}
    for row in sentiment_df.select(
        "username", "comment", "sentiment_score", "sentiment"
    ).limit(10).collect()
]

# Sentiment distribution
sentiment_dist = sentiment_df.groupBy("sentiment").count().collect()
results["sentiment_distribution"] = {
    row["sentiment"]: row["count"] for row in sentiment_dist
}

# 3. Word Frequency Analysis
results["top_words"] = [
    {"word": row["word"], "count": row["count"]}
    for row in wordcount_df.orderBy(desc("count")).limit(20).collect()
]

# 4. Hate Speech Percentage
total_comments = processed_df.count()
hate_speech_count = sentiment_df.filter(col("sentiment") == "negative").count()
hate_percentage = (hate_speech_count / total_comments) * 100

results["hate_speech_metrics"] = {
    "total_comments": total_comments,
    "hate_speech_count": hate_speech_count,
    "hate_speech_percentage": round(hate_percentage, 2)
}

# 5. Additional Metrics (comment length statistics)
from pyspark.sql.functions import length
comment_length_stats = processed_df.select(
    length(col("comment")).alias("comment_length")
).describe().collect()

results["comment_length_stats"] = {
    "count": comment_length_stats[0]["comment_length"],
    "mean": float(comment_length_stats[1]["comment_length"]),
    "stddev": float(comment_length_stats[2]["comment_length"]),
    "min": int(comment_length_stats[3]["comment_length"]),
    "max": int(comment_length_stats[4]["comment_length"])
}

# Write results to JSON file with custom encoder
output_path = "/app/batch_processing/results.json"
with open(output_path, "w") as f:
    json.dump(results, f, indent=2, cls=DateTimeEncoder)

print("Results saved to results.json")

spark.stop()

# docker exec spark-batch spark-submit /app/batch_processing/view_results.py