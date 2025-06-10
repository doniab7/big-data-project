import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StreamingApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("YouTubeLiveStreamingProcessor")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Kafka source
    val kafkaBootstrap = "kafka:9092"
    val topic = "youtube-comments-raw"

    // Define schema of incoming messages
    val schema = new StructType()
      .add("datetime", StringType)
      .add("author", StringType)
      .add("message", StringType)

    // Read from Kafka
    val rawDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()

    // Parse messages
    val parsed = rawDF.selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", schema).alias("data"))
      .select("data.*")
      .withColumnRenamed("author", "username")
      .withColumnRenamed("message", "comment")
      .withColumn("timestamp", to_timestamp($"datetime", "yyyy-MM-dd HH:mm:ss"))
      .drop("datetime")

    // Basic sentiment logic (simulate)
    val sentimentDF = parsed.withColumn(
      "sentiment",
      when(lower($"comment").contains("love") || lower($"comment").contains("great"), "positive")
        .when(lower($"comment").contains("hate") || lower($"comment").contains("stupid"), "negative")
        .otherwise("neutral")
    )

    // Hate speech detection (basic keyword match)
    val hateKeywords = Seq("hate", "stupid", "idiot", "kill", "die", "dead", "ugly", "dumb")
    val isHateSpeech = hateKeywords.map(k => lower($"comment").contains(k)).reduce(_ || _)
    val hateDF = sentimentDF.withColumn("is_hate_speech", isHateSpeech)

    // Word count
    val words = hateDF.select(explode(split($"comment", "\\s+")).alias("word"))
      .filter(length($"word") > 2)
      .groupBy("word")
      .count()
      .orderBy(desc("count"))

    // Write streaming results to JSON (append mode)
    val query = hateDF.writeStream
      .outputMode("append")
      .format("json")
      .option("path", "/app/speed_layer/results/comments")
      .option("checkpointLocation", "/tmp/checkpoints/comments")
      .start()

    query.awaitTermination()
  }
}
