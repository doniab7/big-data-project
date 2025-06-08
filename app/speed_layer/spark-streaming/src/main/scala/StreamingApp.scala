/*
Purpose: Spark Streaming application to process comments in real-time

TODO:

Implement Kafka consumer

Add basic processing (sentiment analysis, word counts)

Windowed operations for trending analysis 

Write results to serving layer (Cassandra)

Add checkpointing for fault tolerance

*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object StreamingApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("RealTimeStreamingApp")
      .master("local[*]") 
      .config("spark.cassandra.connection.host", "cassandra") // hostname from docker
      .getOrCreate()

    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "weather") // Adjust topic name
      .option("startingOffsets", "latest")
      .load()

    val messages = kafkaDF.selectExpr("CAST(value AS STRING)", "timestamp")
      .as[(String, java.sql.Timestamp)]

    val words = messages
      .flatMap { case (text, ts) =>
        text.split("\\s+").filter(_.nonEmpty).map(word => (word.toLowerCase, ts))
      }.toDF("word", "timestamp")

    val trendingCounts = words
      .withWatermark("timestamp", "5 minutes")
      .groupBy(
        window($"timestamp", "5 minutes", "2 minutes"),
        $"word"
      )
      .count()
      .withColumn("window_start", $"window.start")
      .withColumn("window_end", $"window.end")
      .drop("window")

    val query = trendingCounts.writeStream
      .outputMode("append") // append is safe with watermark
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "streaming_ks")         // Make sure the keyspace exists
      .option("table", "trending_words")          // Make sure the table exists
      .option("checkpointLocation", "hdfs://namenode:9000/user/checkpoints/streaming-app")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    query.awaitTermination()
  }
}