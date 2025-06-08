: '
Purpose: Submit Spark job to cluster

TODO:

Configure Spark submit parameters

Set appropriate executor memory

Add checkpoint directory
'
# === Configuration Parameters ===
APP_NAME="RealTimeStreamingApp"
MAIN_CLASS="StreamingApp"
JAR_PATH="/app/speed_layer/target/scala-2.12/spark-streaming-app_2.12-0.1.jar"  # TODO: Update if needed
CHECKPOINT_DIR="hdfs://namenode:9000/user/checkpoints/streaming-app"  # TODO: Update path if needed

# === Submit Spark Job ===
spark-submit \
  --class "$MAIN_CLASS" \
  --master local[*] \  # TODO: Change to yarn or spark://host:port in cluster mode
  --deploy-mode client \
  --executor-memory 2G \  # TODO: Adjust memory
  --total-executor-cores 2 \  # TODO: Adjust cores
  --conf "spark.sql.streaming.checkpointLocation=$CHECKPOINT_DIR" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  "$JAR_PATH"