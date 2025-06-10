import json
import time
import random
from datetime import datetime
import threading
import queue

class SpeedDataGenerator:
    def __init__(self, output_file="mock_data/data/speed_alerts.json"):
        self.output_file = output_file
        self.running = False
        self.alerts_queue = queue.Queue()
        
    def generate_alert(self):
        """Generate a more realistic alert with varied event types and more detailed information"""
        # Define possible event types and their configurations
        event_types = [
            {
                "type": "hateful_comment_detected",
                "categories": ["hate_speech", "profanity", "harassment", "discrimination", "threatening"],
                "actions": ["deleted", "flagged", "user_warned", "user_timeout", "escalated_to_moderator"],
                "confidence_range": (0.7, 0.99),
                "severity": ["high", "medium", "critical"],
                "weight": 0.4  # 40% chance of this event type
            },
            {
                "type": "spam_detected",
                "categories": ["commercial", "repetitive", "bot_activity", "phishing", "scam"],
                "actions": ["filtered", "hidden", "user_warned", "account_flagged"],
                "confidence_range": (0.65, 0.95),
                "severity": ["low", "medium"],
                "weight": 0.3  # 30% chance
            },
            {
                "type": "sentiment_shift",
                "categories": ["sudden_negative", "coordinated_activity", "topic_controversy"],
                "actions": ["monitored", "flagged_for_review", "trend_analysis"],
                "confidence_range": (0.6, 0.9),
                "severity": ["medium", "low"],
                "weight": 0.2  # 20% chance
            },
            {
                "type": "user_activity_spike",
                "categories": ["possible_bot", "raid", "trending_topic"],
                "actions": ["rate_limited", "monitored", "verification_required"],
                "confidence_range": (0.5, 0.85),
                "severity": ["low", "medium"],
                "weight": 0.1  # 10% chance
            }
        ]
        
        # Select event type based on weights
        selected_event = random.choices(
            event_types, 
            weights=[e["weight"] for e in event_types],
            k=1
        )[0]
        
        # Generate usernames that look more realistic
        username_patterns = [
            lambda: f"{random.choice(['cool', 'super', 'mega', 'ultra', 'epic'])}_{random.choice(['gamer', 'fan', 'user', 'viewer', 'streamer'])}{random.randint(1, 999)}",
            lambda: f"{random.choice(['the', 'real', 'not', 'original', 'only'])}{random.choice(['john', 'jane', 'alex', 'sam', 'max'])}{random.randint(10, 99)}",
            lambda: f"{random.choice(['gaming', 'streaming', 'watching', 'playing', 'chatting'])}_with_{random.choice(['friends', 'fans', 'viewers', 'everyone'])}"
        ]
        
        # Generate sample filtered text based on event type
        filtered_text_samples = {
            "hateful_comment_detected": [
                "[FILTERED: HATE SPEECH]",
                "[CONTENT REMOVED: VIOLATION OF COMMUNITY GUIDELINES]",
                "[REDACTED DUE TO HARMFUL CONTENT]"
            ],
            "spam_detected": [
                "[FILTERED: REPETITIVE SPAM]",
                "[HIDDEN: COMMERCIAL CONTENT]",
                "[REMOVED: UNSOLICITED PROMOTION]"
            ],
            "sentiment_shift": [
                "[FLAGGED: COORDINATED NEGATIVE ACTIVITY]",
                "[MONITORED: UNUSUAL SENTIMENT PATTERN]",
                "[REVIEW: TOPIC CONTROVERSY]"
            ],
            "user_activity_spike": [
                "[FLAGGED: UNUSUAL POSTING FREQUENCY]",
                "[MONITORED: POTENTIAL AUTOMATED ACTIVITY]",
                "[REVIEW: RATE LIMIT TRIGGERED]"
            ]
        }
        
        # Generate the alert with more detailed information
        event_type = selected_event["type"]
        categories = random.sample(selected_event["categories"], random.randint(1, min(2, len(selected_event["categories"]))))
        severity = random.choice(selected_event["severity"])
        confidence = round(random.uniform(*selected_event["confidence_range"]), 2)
        
        return {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "comment": {
                "id": f"comment_{random.randint(10000, 99999)}",
                "user": random.choice(username_patterns)(),
                "original_text": random.choice(filtered_text_samples[event_type]),
                "confidence_score": confidence,
                "detected_categories": categories,
                "severity": severity
            },
            "action_taken": random.choice(selected_event["actions"]),
            "notification_sent": True,
            "processing_time_ms": random.randint(50, 200),
            "message": f"{event_type.replace('_', ' ').title()} - {categories[0].replace('_', ' ').title()} detected with {int(confidence*100)}% confidence"
        }
    
    def start_generation(self, min_interval=5, max_interval=15):
        """Start generating alerts at random intervals"""
        self.running = True
        def generate_loop():
            while self.running:
                # Generate alert with random intervals
                alert = self.generate_alert()
                self.alerts_queue.put(alert)
                
                # Keep last 50 alerts
                alerts_list = []
                temp_queue = queue.Queue()
                count = 0
                
                while not self.alerts_queue.empty() and count < 50:
                    alert_item = self.alerts_queue.get()
                    alerts_list.append(alert_item)
                    temp_queue.put(alert_item)
                    count += 1
                
                self.alerts_queue = temp_queue

                with open(self.output_file, 'w') as f:
                    json.dump({"alerts": alerts_list}, f, indent=2)
                
                print(f"Generated speed alert: {alert['event_type']} at {datetime.now()}")
                
                # Use a more varied interval for more realistic timing
                interval = random.randint(min_interval, max_interval)
                if random.random() < 0.2:  # 20% chance of a burst of alerts
                    interval = random.randint(1, 3)  # Quick follow-up
                    print(f"Alert burst detected! Next alert in {interval} seconds")
                
                time.sleep(interval)
        
        thread = threading.Thread(target=generate_loop)
        thread.daemon = True
        thread.start()
        print(f"Speed data generator started with interval {min_interval}-{max_interval} seconds")
