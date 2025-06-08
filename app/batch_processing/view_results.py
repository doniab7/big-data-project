from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ViewResults") \
    .getOrCreate()

df = spark.read.parquet("hdfs://namenode:9000/data/youtube_comments/processed")
# Show vertically (better for wide columns)
df.show(n=20, vertical=True)

# Read the results
results_df = spark.read.parquet("hdfs://namenode:9000/data/youtube_comments/batch_results")

# Display with better formatting
results_df.show(n=50, truncate=False, vertical=True)

# Show schema
results_df.printSchema()

# Get statistics
results_df.describe().show()

spark.stop()

# docker exec spark-batch spark-submit /app/batch_processing/view_results.py