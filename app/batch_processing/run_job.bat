@echo off

docker exec spark-batch pip install textblob

docker exec spark-batch spark-submit ^
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 ^
    /app/batch_processing/batch_processing.py

pause