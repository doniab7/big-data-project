from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, to_timestamp
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("YouTubeCommentsBatchProcessing") \
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

# 4. Example batch analysis: Count comments by user
user_counts = processed_df.groupBy("username").count()
user_counts.show()

# Save batch results
user_counts.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(batch_output_path)

spark.stop()