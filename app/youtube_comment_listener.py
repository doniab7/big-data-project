from typing import Optional
import pytchat
import time
import subprocess
import os
from dotenv import load_dotenv

def save_comment_hdfs(hdfs_file_path: str, comment: str) -> None:
    """
    Appends a comment into an HDFS file.

    Args:
        hdfs_file_path (str): Path to the HDFS file.
        comment (str): The comment text to save.
    """
    local_temp = "/tmp/temp_comment.txt"
    with open(local_temp, 'w', encoding='utf-8') as f:
        f.write(comment + '\n')

    # Use Docker to run HDFS command inside the namenode container
    try:
        result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-appendToFile", local_temp, hdfs_file_path],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        print(result.stdout.decode())
        print(result.stderr.decode())
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e}")
        print(f"stdout: {e.stdout.decode()}")
        print(f"stderr: {e.stderr.decode()}")

def listen_to_livestream(video_id: str, hdfs_file_path: str) -> None:
    """
    Listens to a YouTube livestream and saves each new comment into HDFS.

    Args:
        video_id (str): YouTube video ID of the livestream.
        hdfs_file_path (str): Path to the HDFS file.
    """
    chat = pytchat.create(video_id=video_id)
    print(f"Listening to livestream (Video ID: {video_id})...")

    while chat.is_alive():
        for c in chat.get().sync_items():
            formatted_comment = f"[{c.datetime}] {c.author.name}: {c.message}"
            print(formatted_comment)
            save_comment_hdfs(hdfs_file_path, formatted_comment)
        time.sleep(1)

if __name__ == "__main__":
    load_dotenv()

    VIDEO_ID = os.getenv('VIDEO_ID') 
    # Save into HDFS!
    HDFS_FILE_PATH = "/youtube_live_comments/comments.txt"

    listen_to_livestream(VIDEO_ID, HDFS_FILE_PATH)
