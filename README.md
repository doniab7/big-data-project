# YouTube Livestream Analytics

A complete system for collecting, processing, and analyzing YouTube livestream comments using:

- **Kafka** (real-time streaming)
- **Hadoop HDFS** (storage)
- **Spark** (batch and stream processing)
- **Docker** (containerization)

---

## Project Structure

```plaintext
app/
 └── local_test/
     ├── local_comment_listener.py     # Script to listen to livestream and save comments locally
     └── youtube_live_comments.txt     # Output file for collected comments
 ├── batch_processing/          # Spark batch processing job
 ├── data_source/               # Kafka producer script
hadoop_config/
 ├── core-site.xml                     # Core Hadoop configuration
 ├── hdfs-site.xml                     # HDFS settings (replication, paths)
 └── log4j.properties                  # Logging configuration for Hadoop
hadoop_datanode1/                      # DataNode 1 configuration and volumes
hadoop_datanode2/                      # DataNode 2 configuration and volumes
hadoop_namenode/                       # NameNode configuration and volumes
.env                                   # Environment variables (private)
.env.example                           # Example environment file to copy and fill
.gitignore                             # Git ignored files (.env, outputs, etc.)
docker-compose.yml                     # Docker setup for Hadoop cluster
init-datanode.sh                       # Helper script to initialize DataNodes
start-hdfs.sh                          # Helper script to start HDFS
```

---

## How to Run the Comment Listener Locally

1. **Set up your environment:**

- Copy `.env.example` to `.env` and fill your YouTube `VIDEO_ID`:

```bash
cp .env.example .env
```

2. **Install Python dependencies:**

```bash
pip install pytchat
```

3. **Run the listener script:**

```bash
cd app/local_test
python3 local_comment_listener.py
```

- Comments will be saved to `youtube_live_comments.txt`.

---

## How to Start the Cluster

1. **Make sure Docker and Docker Compose are installed.**

2. **Start the cluster:**

```bash
docker-compose up -d
```

- This launches:
  - Hadoop NameNode + 2 DataNodes
  - Zookeeper
  - Kafka
  - Spark master

4. **(Optional) Scripts:**

- `init-datanode.sh`: Prepares the datanodes.
- `start-hdfs.sh`: Starts HDFS manually if needed.

---

## Environment Variables

- **VIDEO_ID**: (required) ID of the YouTube livestream you want to listen to.

---

## Pipeline Execution

1. **Create Kafka Topics**

```bash
.\create-topics.bat
```

Creates:

- youtube-comments-raw (raw comments)

- youtube-comments-batch (processed data)

2. **Start Comment Producer**

```bash
python app/data_source/youtube_comment_producer.py
```

Collects comments in JSON format:

```json
{
  "datetime": "2025-06-09 23:28:29",
  "author": "username",
  "message": "comment text"
}
```

3. **Run Batch Processing**

```bash
.\app\batch_processing\run_job.bat
```

- Performs:

  - Sentiment analysis (using TextBlob)
  - Hate speech detection
  - Word frequency analysis

- Saves results to:
  - HDFS (in Parquet format)
