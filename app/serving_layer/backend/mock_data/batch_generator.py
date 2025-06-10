import json
import time
import random
from datetime import datetime, timedelta
import threading

class BatchDataGenerator:
    def __init__(self, output_file="mock_data/data/batch_data.json"):
        self.output_file = output_file
        self.running = False
        
    def generate_analytics(self):
        base_time = datetime.now()
        return {
            "timestamp": base_time.isoformat(),
            "video_id": f"vid_{random.randint(1000,9999)}",
            "video_title": f"Live Stream {random.choice(['Gaming', 'Music', 'Talk Show'])}",
            "analytics": {
                "total_comments": random.randint(5000, 25000),
                "hateful_comments": random.randint(50, 500),
                "hate_percentage": round(random.uniform(0.5, 3.0), 2),
                "top_keywords": random.sample([
                    "amazing", "love", "hate", "awesome", "terrible", 
                    "great", "bad", "wonderful", "stupid", "brilliant"
                ], 5),
                "sentiment_distribution": {
                    "positive": round(random.uniform(0.4, 0.7), 2),
                    "neutral": round(random.uniform(0.2, 0.4), 2),
                    "negative": round(random.uniform(0.1, 0.3), 2)
                },
                "hourly_comment_volume": [random.randint(100, 300) for _ in range(24)],
                "user_engagement_stats": {
                    "unique_users": random.randint(3000, 15000),
                    "repeat_commenters": random.randint(500, 3000)
                }
            }
        }
    
    def start_generation(self, interval=30):  # Generate every 30 seconds
        self.running = True
        def generate_loop():
            while self.running:
                data = self.generate_analytics()
                with open(self.output_file, 'w') as f:
                    json.dump(data, f, indent=2)
                print(f"Generated batch data at {datetime.now()}")
                time.sleep(interval)
        
        thread = threading.Thread(target=generate_loop)
        thread.daemon = True
        thread.start()
        
    def stop_generation(self):
        self.running = False