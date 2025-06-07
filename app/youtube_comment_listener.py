from typing import Optional
import pytchat
import time
import os
from dotenv import load_dotenv
from hdfs import InsecureClient


def save_comment_hdfs(client: InsecureClient, hdfs_path: str, comment: str) -> None:
    """
    Appends a comment to a file on HDFS using WebHDFS.

    Args:
        client (InsecureClient): WebHDFS client.
        hdfs_path (str): HDFS file path to write comments to.
        comment (str): The comment text to save.
    """
    # Read existing content if file exists (to preserve previous comments)
    try:
        with client.read(hdfs_path, encoding='utf-8') as reader:
            existing = reader.read()
    except FileNotFoundError:
        existing = ''

    # Write back existing content + new comment
    with client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
        writer.write(existing + comment + '\n')


def listen_to_livestream(video_id: str, client: InsecureClient, hdfs_output_path: str) -> None:
    """
    Listens to a YouTube livestream and saves comments to HDFS via WebHDFS.

    Args:
        video_id (str): YouTube video ID of the livestream.
        client (InsecureClient): WebHDFS client.
        hdfs_output_path (str): HDFS path to save comments.
    """
    chat = pytchat.create(video_id=video_id)
    print(f"Listening to livestream (Video ID: {video_id})...")

    while chat.is_alive():
        for c in chat.get().sync_items():
            formatted_comment = f"[{c.datetime}] {c.author.name}: {c.message}"
            print(formatted_comment)
            save_comment_hdfs(client, hdfs_output_path, formatted_comment)
        time.sleep(1)


if __name__ == "__main__":
    load_dotenv()

    VIDEO_ID = os.getenv('VIDEO_ID')
    HDFS_PATH = '/youtube_live_comments/comments.txt'

    if not VIDEO_ID:
        raise ValueError("VIDEO_ID is not set in the .env file.")

    client = InsecureClient('http://localhost:9870', user='root')

    listen_to_livestream(VIDEO_ID, client, HDFS_PATH)
