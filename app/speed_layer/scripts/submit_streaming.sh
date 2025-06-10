#!/bin/bash

echo "Submitting Spark Streaming Job..."

docker exec spark-batch spark-submit \
  --class StreamingApp \
  --master local[*] \
  /app/speed_layer/spark-streaming/target/scala-2.12/spark-streaming_2.12-0.1.jar
