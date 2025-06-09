from typing import Optional
import pytchat
import time
import os
from datetime import datetime
from confluent_kafka import Producer
import json
from dotenv import load_dotenv

KAFKA_CONFIG = {
    "bootstrap_servers": "127.0.0.1:9092", 
    "topic_raw": "youtube_comments_raw",
    "topic_batch": "youtube_comments_batch"
}

def create_kafka_producer():
    """Create a resilient Kafka producer with error handling"""
    try:
        conf = {
            'bootstrap.servers': KAFKA_CONFIG["bootstrap_servers"],
            'client.id': 'youtube-comment-producer',
            'request.timeout.ms': 10000,
            'message.timeout.ms': 10000
        }
        return Producer(conf)
    except Exception as e:
        print(f"Kafka connection failed: {e}")
        raise

def delivery_report(err, msg):
    """Delivery report callback"""
    if err is not None:
        print(f'✗ Message delivery failed: {err}')
    else:
        print(f'✓ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def send_to_kafka(producer: Producer, comment_data: dict):
    """Safely send data to both Kafka topics"""
    try:
        
        # Convert to JSON string
        json_data = json.dumps(comment_data).encode('utf-8')
        
        producer.produce(
            KAFKA_CONFIG["topic_raw"],
            value=json_data,
            callback=delivery_report
        )
        
        producer.produce(
            KAFKA_CONFIG["topic_batch"],
            value=json_data,
            callback=delivery_report
        )
        
        print("here3 - flushing messages")
        producer.flush(timeout=10)
        print("here4 - messages sent successfully")
        
    except Exception as e:
        print(f"Failed to send message: {type(e).__name__}: {e}")

def listen_to_livestream(video_id: str):
    """Stream YouTube comments to Kafka with reconnection logic"""
    producer = create_kafka_producer()
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        try:
            chat = pytchat.create(video_id=video_id)
            print(f"Connected to livestream (Video ID: {video_id})")
            
            while chat.is_alive():
                for comment in chat.get().sync_items():
                    comment_data = {
                        "datetime": comment.datetime,
                        "author": comment.author.name,
                        "message": comment.message
                        
                    }
                    print(f"[before being sent] {comment.datetime} {comment.author.name}: {comment.message}")
                    send_to_kafka(producer, comment_data)
                
                time.sleep(1)
                
            retry_count = 0  
            
        except Exception as e:
            retry_count += 1
            print(f"Error (Attempt {retry_count}/{max_retries}): {str(e)}")
            time.sleep(min(2 ** retry_count, 30))  # Exponential backoff
            
    # Clean up
    producer.flush(timeout=10)
    print("Max retries exceeded. Shutting down.")

def test_kafka_connection():
    """Test basic Kafka connectivity"""
    try:
        print("Testing Kafka connection...")
        producer = create_kafka_producer()
        
        # Test message
        test_data = {"test": "connection", "timestamp": str(datetime.now())}
        json_data = json.dumps(test_data).encode('utf-8')
        
        producer.produce(KAFKA_CONFIG["topic_raw"], value=json_data, callback=delivery_report)
        producer.flush(timeout=10)
        
        print("✓ Kafka connection successful!")
        return True
        
    except Exception as e:
        print(f"✗ Kafka connection failed: {e}")
        return False

if __name__ == "__main__":
    load_dotenv()
    VIDEO_ID = os.getenv('VIDEO_ID')
    
    if not VIDEO_ID:
        raise ValueError("Missing VIDEO_ID in .env file")
    
    # Test connection first
    if not test_kafka_connection():
        print("Exiting due to Kafka connection failure")
        exit(1)
    
    listen_to_livestream(VIDEO_ID)