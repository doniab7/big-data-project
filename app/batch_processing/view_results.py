from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

spark = SparkSession.builder \
    .appName("ViewResults") \
    .getOrCreate()

# Read all result datasets
processed_df = spark.read.parquet("hdfs://namenode:9000/data/youtube_comments/processed")
sentiment_df = spark.read.parquet("hdfs://namenode:9000/data/youtube_comments/sentiment_analysis")
wordcount_df = spark.read.parquet("hdfs://namenode:9000/data/youtube_comments/word_counts")

print("=== Processed Comments Sample ===")
processed_df.show(n=10, vertical=True)

print("\n=== Sentiment Analysis Results ===")
sentiment_df.select("username", "comment", "sentiment_score", "sentiment").show(10)
sentiment_df.groupBy("sentiment").count().show()

print("\n=== Top 20 Most Frequent Words ===")
wordcount_df.orderBy(desc("count")).show(20, truncate=False)

print("\n=== Hate Speech Percentage ===")
total = processed_df.count()
hate_speech_count = sentiment_df.filter(col("sentiment") == "negative").count()
print(f"Hate speech percentage: {(hate_speech_count/total)*100:.2f}%")

spark.stop()

# docker exec spark-batch spark-submit /app/batch_processing/view_results.py