import pymongo
import json
import os
from datetime import datetime
from typing import Dict, List, Optional

class MongoDataManager:
    def __init__(self, connection_string="mongodb://localhost:27017/", db_name="youtube_analytics"):
        """
        Initialize MongoDB connection
        """
        self.client = pymongo.MongoClient(connection_string)
        self.db = self.client[db_name]
        
        # Collections
        self.batch_results = self.db.batch_results
        self.processed_comments = self.db.processed_comments
        self.sentiment_analysis = self.db.sentiment_analysis
        self.hate_speech_logs = self.db.hate_speech_logs
        
        # Create indexes for better performance
        self._create_indexes()
    
    def _create_indexes(self):
        """Create database indexes for better query performance"""
        try:
            self.batch_results.create_index([("timestamp", -1)])
            self.processed_comments.create_index([("timestamp", -1)])
            self.sentiment_analysis.create_index([("batch_id", 1)])
            self.hate_speech_logs.create_index([("timestamp", -1)])
            print("MongoDB indexes created successfully")
        except Exception as e:
            print(f"Error creating indexes: {e}")
    
    def store_batch_results(self, batch_data: Dict) -> str:
        """
        Store batch processing results in MongoDB
        Returns the batch_id for reference
        """
        try:
            # Add metadata
            batch_document = {
                "batch_id": f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "timestamp": datetime.now(),
                "processed_at": datetime.now().isoformat(),
                "data": batch_data
            }
            
            result = self.batch_results.insert_one(batch_document)
            
            # Also store individual components for easier querying
            self._store_batch_components(batch_document["batch_id"], batch_data)
            
            print(f"Batch results stored with ID: {batch_document['batch_id']}")
            return batch_document["batch_id"]
            
        except Exception as e:
            print(f"Error storing batch results: {e}")
            return None
    
    def _store_batch_components(self, batch_id: str, batch_data: Dict):
        """Store individual components of batch data for easier access"""
        try:
            # Store processed comments
            if "processed_comments" in batch_data:
                for comment in batch_data["processed_comments"]:
                    comment_doc = {
                        "batch_id": batch_id,
                        "timestamp": datetime.fromisoformat(comment["timestamp"]),
                        "username": comment["username"],
                        "comment": comment["comment"],
                        "stored_at": datetime.now()
                    }
                    self.processed_comments.insert_one(comment_doc)
            
            # Store sentiment samples
            if "sentiment_samples" in batch_data:
                sentiment_docs = []
                for sentiment in batch_data["sentiment_samples"]:
                    sentiment_doc = {
                        "batch_id": batch_id,
                        "username": sentiment["username"],
                        "comment": sentiment["comment"],
                        "sentiment_score": sentiment["sentiment_score"],
                        "sentiment": sentiment["sentiment"],
                        "stored_at": datetime.now()
                    }
                    sentiment_docs.append(sentiment_doc)
                
                if sentiment_docs:
                    self.sentiment_analysis.insert_many(sentiment_docs)
            
            # Store hate speech detections
            if "hate_speech_metrics" in batch_data:
                hate_doc = {
                    "batch_id": batch_id,
                    "total_comments": batch_data["hate_speech_metrics"]["total_comments"],
                    "hate_speech_count": batch_data["hate_speech_metrics"]["hate_speech_count"],
                    "hate_speech_percentage": batch_data["hate_speech_metrics"]["hate_speech_percentage"],
                    "timestamp": datetime.now()
                }
                self.hate_speech_logs.insert_one(hate_doc)
                
        except Exception as e:
            print(f"Error storing batch components: {e}")
    
    def get_latest_batch_data(self) -> Optional[Dict]:
        """Get the most recent batch processing results"""
        try:
            latest = self.batch_results.find_one(
                sort=[("timestamp", -1)]
            )
            if latest:
                return latest["data"]
            return None
        except Exception as e:
            print(f"Error getting latest batch data: {e}")
            return None
    
    def get_dashboard_analytics(self) -> Dict:
        """
        Transform batch data into dashboard-compatible format
        """
        try:
            latest_batch = self.get_latest_batch_data()
            if not latest_batch:
                return {"error": "No batch data available"}
            
            # Transform the new format to match dashboard expectations
            analytics = {
                "timestamp": datetime.now().isoformat(),
                "video_id": "current_stream",  # You might want to get this from somewhere
                "video_title": "YouTube Live Stream Analysis",
                "total_comments": latest_batch.get("hate_speech_metrics", {}).get("total_comments", 0),
                "hateful_comments": latest_batch.get("hate_speech_metrics", {}).get("hate_speech_count", 0),
                "hate_speech_percentage": latest_batch.get("hate_speech_metrics", {}).get("hate_speech_percentage", 0),
                "hate_percentage": latest_batch.get("hate_speech_metrics", {}).get("hate_speech_percentage", 0),
                "top_keywords": [word["word"] for word in latest_batch.get("top_words", [])[:10]],
                "top_words": latest_batch.get("top_words", []),  # Add the original top_words array
                "sentiment_distribution": self._normalize_sentiment_distribution(
                    latest_batch.get("sentiment_distribution", {})
                ),
                "sentiment_samples": latest_batch.get("sentiment_samples", []),  # Add the sentiment samples
                "processed_comments": latest_batch.get("processed_comments", []),  # Add processed comments
                "hourly_comment_volume": self._generate_hourly_volume(
                    latest_batch.get("processed_comments", [])
                ),
                "user_engagement_stats": {
                    "unique_users": len(set(comment["username"] for comment in 
                                           latest_batch.get("processed_comments", []))),
                    "repeat_commenters": self._count_repeat_commenters(
                        latest_batch.get("processed_comments", [])
                    )
                },
                "comment_length_stats": latest_batch.get("comment_length_stats", {}),
                "comment_stats": latest_batch.get("comment_length_stats", {})
            }
            
            return analytics
            
        except Exception as e:
            print(f"Error getting dashboard analytics: {e}")
            return {"error": str(e)}
    
    def _normalize_sentiment_distribution(self, sentiment_dist: Dict) -> Dict:
        """Convert count-based sentiment to percentage-based"""
        total = sum(sentiment_dist.values()) if sentiment_dist else 1
        if total == 0:
            total = 1
            
        return {
            "positive": round(sentiment_dist.get("positive", 0) / total, 2),
            "neutral": round(sentiment_dist.get("neutral", 0) / total, 2),
            "negative": round(sentiment_dist.get("negative", 0) / total, 2)
        }
    
    def _generate_hourly_volume(self, comments: List[Dict]) -> List[int]:
        """Generate hourly comment volume from processed comments"""
        try:
            from collections import defaultdict
            
            hourly_counts = defaultdict(int)
            
            for comment in comments:
                try:
                    timestamp = datetime.fromisoformat(comment["timestamp"])
                    hour = timestamp.hour
                    hourly_counts[hour] += 1
                except:
                    continue
            
            # Create 24-hour array
            return [hourly_counts.get(hour, 0) for hour in range(24)]
            
        except Exception as e:
            print(f"Error generating hourly volume: {e}")
            return [0] * 24
    
    def _count_repeat_commenters(self, comments: List[Dict]) -> int:
        """Count users who commented more than once"""
        try:
            from collections import Counter
            
            user_counts = Counter(comment["username"] for comment in comments)
            return sum(1 for count in user_counts.values() if count > 1)
            
        except Exception as e:
            print(f"Error counting repeat commenters: {e}")
            return 0
    
    def get_recent_alerts(self, limit: int = 10) -> List[Dict]:
        """
        Generate alerts from hate speech detection
        """
        try:
            # Get recent hate speech logs
            recent_hate_logs = list(self.hate_speech_logs.find(
                sort=[("timestamp", -1)], 
                limit=limit
            ))
            
            alerts = []
            for log in recent_hate_logs:
                if log.get("hate_speech_count", 0) > 0:
                    alert = {
                        "timestamp": log["timestamp"].isoformat(),
                        "event_type": "hate_speech_detected",
                        "details": {
                            "total_comments_processed": log.get("total_comments", 0),
                            "hate_speech_detected": log.get("hate_speech_count", 0),
                            "percentage": log.get("hate_speech_percentage", 0)
                        },
                        "action_taken": "flagged_for_review",
                        "batch_id": log.get("batch_id", "unknown")
                    }
                    alerts.append(alert)
            
            return alerts
            
        except Exception as e:
            print(f"Error getting recent alerts: {e}")
            return []
    
    def load_batch_results_from_file(self, file_path: str = "/app/batch_processing/results.json"):
        """
        Load batch results from the file generated by batch processing
        """
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    batch_data = json.load(f)
                
                batch_id = self.store_batch_results(batch_data)
                print(f"Loaded batch results from {file_path} with ID: {batch_id}")
                return batch_id
            else:
                print(f"Batch results file not found: {file_path}")
                return None
                
        except Exception as e:
            print(f"Error loading batch results from file: {e}")
            return None
    
    def close_connection(self):
        """Close MongoDB connection"""
        self.client.close()
        print("MongoDB connection closed")
