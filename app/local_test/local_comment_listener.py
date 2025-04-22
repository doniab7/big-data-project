from typing import Optional
import pytchat
import time
import os
from dotenv import load_dotenv


def save_comment(file_path: str, comment: str) -> None:
    """
    Appends a comment to the specified file.

    Args:
        file_path (str): Path to the output file.
        comment (str): The comment text to save.
    """
    with open(file_path, 'a', encoding='utf-8') as f:
        f.write(comment + '\n')


def listen_to_livestream(video_id: str, output_file: str) -> None:
    """
    Listens to a YouTube livestream and saves each new comment in real time.

    Args:
        video_id (str): YouTube video ID of the livestream.
        output_file (str): Path to the file where comments will be saved.
    """
    chat = pytchat.create(video_id=video_id)
    print(f"Listening to livestream (Video ID: {video_id})...")

    while chat.is_alive():
        for c in chat.get().sync_items():
            formatted_comment = f"[{c.datetime}] {c.author.name}: {c.message}"
            print(formatted_comment)
            save_comment(output_file, formatted_comment)
        time.sleep(1)


if __name__ == "__main__":
    load_dotenv()

    VIDEO_ID = os.getenv('VIDEO_ID') 
    OUTPUT_FILE = "youtube_live_comments.txt"

    if not VIDEO_ID:
        raise ValueError("VIDEO_ID environment variable is not set or missing in the .env file.")

    listen_to_livestream(VIDEO_ID, OUTPUT_FILE)
