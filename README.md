# YouTube Livestream Comment Listener + Local Hadoop Cluster

This project allows you to **listen to YouTube livestream comments** and **store them locally**, with a setup to later process the data using a **Hadoop cluster** (running via Docker).

---

## Project Structure

```plaintext
app/
 └── local_test/
     ├── local_comment_listener.py     # Script to listen to livestream and save comments locally
     └── youtube_live_comments.txt     # Output file for collected comments
 └── youtube_comment_listener.py       # Another version of the script that will run on hdfs
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

## How to Start the Local Hadoop Cluster

1. **Make sure Docker and Docker Compose are installed.**

2. **Start the cluster:**

```bash
docker-compose up -d
```

- This launches:
  - 1 Hadoop NameNode
  - 2 Hadoop DataNodes

3. **Access Hadoop UI:**

- Namenode UI: [http://localhost:9870](http://localhost:9870)

4. **(Optional) Scripts:**

- `init-datanode.sh`: Prepares the datanodes.
- `start-hdfs.sh`: Starts HDFS manually if needed.

---

## Environment Variables

- **VIDEO_ID**: (required) ID of the YouTube livestream you want to listen to.

---