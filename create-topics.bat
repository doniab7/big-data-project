@echo off
REM Create topics with replication factor 1
docker exec kafka kafka-topics --create ^
  --topic youtube-comments-batch ^
  --partitions 1 ^
  --replication-factor 1 ^
  --config retention.ms=604800000 ^
  --bootstrap-server kafka:9092

docker exec kafka kafka-topics --create ^
  --topic youtube-comments-raw ^
  --partitions 3 ^
  --replication-factor 1 ^
  --config retention.ms=86400000 ^
  --bootstrap-server kafka:9092

echo Topics created successfully
pause