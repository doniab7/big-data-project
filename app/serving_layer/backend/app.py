from flask import Flask, jsonify, render_template, request
import json
import os
from threading import Thread
import time
from datetime import datetime
import schedule
from flask_socketio import SocketIO, emit
import subprocess
import watchdog.observers
import watchdog.events
import sys

# Import MongoDataManager - try different import paths
try:
    from mongo_manager import MongoDataManager
except ImportError:
    try:
        from app.serving_layer.backend.mongo_manager import MongoDataManager
    except ImportError:
        from .mongo_manager import MongoDataManager

app = Flask(__name__, 
           static_folder='../frontend',
           static_url_path='',
           template_folder='../frontend')
app.config['SECRET_KEY'] = 'HelloWorld2025'

# Initialize SocketIO
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Initialize MongoDB manager
mongo_manager = MongoDataManager()

# File path for batch results - try multiple possible locations
possible_paths = [
    "/app/batch_processing/results.json",  # Docker container path
    "app/batch_processing/results.json",   # Relative from project root
    os.path.join(os.path.dirname(__file__), "../../batch_processing/results.json"),  # Relative from this file
    "batch_results.json"  # Fallback for local testing
]

BATCH_RESULTS_FILE = None
for path in possible_paths:
    if os.path.exists(path):
        BATCH_RESULTS_FILE = path
        print(f"Found batch results file at: {path}")
        break

if not BATCH_RESULTS_FILE:
    print("Warning: Could not find batch results file, using default path")
    BATCH_RESULTS_FILE = "app/batch_processing/results.json"

@app.route('/')
def dashboard():
    return render_template('index.html')

@app.route('/api/batch-data')
def get_batch_data():
    """Get batch processing data from MongoDB"""
    try:
        analytics = mongo_manager.get_dashboard_analytics()
        return jsonify(analytics)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
"""
@app.route('/api/speed-alerts')
def get_speed_alerts():
    try:
        alerts = mongo_manager.get_recent_alerts(limit=20)
        return jsonify({"alerts": alerts})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
"""
@app.route('/api/dashboard-stats')
def get_dashboard_stats():
    """Combined endpoint for dashboard overview"""
    try:
        analytics = mongo_manager.get_dashboard_analytics()
        alerts = mongo_manager.get_recent_alerts(limit=10)
        
        return jsonify({
            "batch_analytics": analytics,
            "recent_alerts": alerts,
            "system_status": "operational",
            "last_update": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/load-batch-results')
def load_batch_results():
    """Manually trigger loading of batch results from file"""
    try:
        batch_id = mongo_manager.load_batch_results_from_file(BATCH_RESULTS_FILE)
        if batch_id:
            return jsonify({
                "status": "success", 
                "message": f"Batch results loaded with ID: {batch_id}"
            })
        else:
            return jsonify({
                "status": "error", 
                "message": "No batch results file found or error loading"
            }), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/api/batch-metrics')
def get_batch_metrics():
    """Return parsed batch results for dashboard metrics"""
    try:
        # First try to read from MongoDB
        latest_batch = mongo_manager.get_latest_batch_data()
        if latest_batch:
            # Return the complete batch data structure that the frontend expects
            return jsonify({
                "hate_speech_metrics": latest_batch.get("hate_speech_metrics", {}),
                "sentiment_distribution": latest_batch.get("sentiment_distribution", {}),
                "top_words": latest_batch.get("top_words", []),
                "sentiment_samples": latest_batch.get("sentiment_samples", []),
                "processed_comments": latest_batch.get("processed_comments", []),
                "comment_length_stats": latest_batch.get("comment_length_stats", {})
            })
        
        # Fallback to file if MongoDB doesn't have data
        with open(BATCH_RESULTS_FILE, 'r') as f:
            data = json.load(f)
        
        return jsonify(data)
    except FileNotFoundError:
        return jsonify({"error": "Results file not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/sentiment-details')
def get_sentiment_details():
    """Get detailed sentiment analysis"""
    try:
        latest_batch = mongo_manager.get_latest_batch_data()
        if not latest_batch:
            return jsonify({"error": "No data available"}), 404
        
        sentiment_details = {
            "sentiment_samples": latest_batch.get("sentiment_samples", [])[:50],  # Limit for performance
            "sentiment_distribution": latest_batch.get("sentiment_distribution", {}),
            "total_analyzed": len(latest_batch.get("sentiment_samples", []))
        }
        
        return jsonify(sentiment_details)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/word-analysis')
def get_word_analysis():
    """Get top words analysis"""
    try:
        latest_batch = mongo_manager.get_latest_batch_data()
        if not latest_batch:
            return jsonify({"error": "No data available"}), 404
        
        word_analysis = {
            "top_words": latest_batch.get("top_words", []),
            "comment_stats": latest_batch.get("comment_length_stats", {})
        }
        
        return jsonify(word_analysis)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def check_for_new_batch_results():
    """Periodically check for new batch results and load them"""
    try:
        if os.path.exists(BATCH_RESULTS_FILE):
            # Check file modification time
            file_mod_time = os.path.getmtime(BATCH_RESULTS_FILE)
            current_time = time.time()
            
            # If file was modified in the last 5 minutes, load it
            if current_time - file_mod_time < 300:  # 5 minutes
                print("New batch results detected, loading...")
                batch_id = mongo_manager.load_batch_results_from_file(BATCH_RESULTS_FILE)
                if batch_id:
                    print(f"Successfully loaded new batch results: {batch_id}")
                    # Emit a socket event to notify clients
                    try:
                        latest_batch = mongo_manager.get_latest_batch_data()
                        if latest_batch:
                            socketio.emit('batch_update', {'analytics': latest_batch})
                            print("Emitted batch_update event via websocket")
                    except Exception as e:
                        print(f"Error emitting batch update: {e}")
                    
    except Exception as e:
        print(f"Error checking for new batch results: {e}")

# File watcher for batch_results.json
class BatchFileHandler(watchdog.events.FileSystemEventHandler):
    def __init__(self, file_path):
        self.file_path = os.path.abspath(file_path)
        print(f"Watching for changes to: {self.file_path}")
        
    def on_modified(self, event):
        if not event.is_directory and os.path.abspath(event.src_path) == self.file_path:
            print(f"Detected change to {event.src_path}")
            try:
                # Run the save_to_mongo.py script
                script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                                          "batch_processing", "save_to_mongo.py")
                
                if os.path.exists(script_path):
                    print(f"Running save_to_mongo.py at {script_path}")
                    subprocess.run([sys.executable, script_path, self.file_path], 
                                  check=True)
                    print("save_to_mongo.py executed successfully")
                else:
                    print(f"save_to_mongo.py not found at {script_path}")
            except Exception as e:
                print(f"Error running save_to_mongo.py: {e}")

def start_file_watcher(file_path):
    """Start a file watcher for batch_results.json"""
    observer = watchdog.observers.Observer()
    handler = BatchFileHandler(file_path)
    
    # Get the directory containing the file
    directory = os.path.dirname(file_path) or '.'
    
    observer.schedule(handler, directory, recursive=False)
    observer.daemon = True
    observer.start()
    print(f"File watcher started for {file_path} in directory {directory}")
    return observer

def scheduled_jobs():
    """Run scheduled tasks"""
    schedule.every(2).minutes.do(check_for_new_batch_results)
    
    while True:
        schedule.run_pending()
        time.sleep(30)

# Socket.IO event handlers
@socketio.on('connect')
def handle_connect():
    print(f"Client connected: {request.sid}")
    emit('status', {'msg': 'Connected to server'})

@socketio.on('disconnect')
def handle_disconnect():
    print(f"Client disconnected: {request.sid}")

# Import and initialize the speed data generator
from mock_data.speed_generator import SpeedDataGenerator

# Create a custom alert handler that emits socket events
class SocketAlertHandler:
    def __init__(self, socketio_instance):
        self.socketio = socketio_instance
        
    def handle_new_alert(self, alert):
        print(f"Emitting new alert: {alert['event_type']}")
        self.socketio.emit('new_alert', alert)

if __name__ == '__main__':
    # Ensure directories exist
    os.makedirs('logs', exist_ok=True)
    
    # Load initial batch results if available
    print("Loading initial batch results...")
    mongo_manager.load_batch_results_from_file(BATCH_RESULTS_FILE)
    
    # Start scheduled jobs thread
    scheduler_thread = Thread(target=scheduled_jobs)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    
    # Start file watcher for batch_results.json
    file_watcher = start_file_watcher(BATCH_RESULTS_FILE)
    
    # Initialize and start the speed data generator
    speed_generator = SpeedDataGenerator()
    
    # Create a thread to monitor the speed generator's queue and emit socket events
    def emit_speed_alerts():
        while True:
            try:
                if not speed_generator.alerts_queue.empty():
                    alert = speed_generator.alerts_queue.get()
                    print(f"Emitting real-time alert: {alert['event_type']}")
                    socketio.emit('new_alert', alert)
                time.sleep(1)
            except Exception as e:
                print(f"Error in emit_speed_alerts: {e}")
                time.sleep(5)  # Wait a bit longer if there's an error
    
    # Start the speed generator
    speed_generator.start_generation(min_interval=5, max_interval=15)
    
    # Start the alert emitter thread
    alert_thread = Thread(target=emit_speed_alerts)
    alert_thread.daemon = True
    alert_thread.start()
    
    print("Starting Flask-SocketIO server...")
    print(f"Monitoring batch results file: {BATCH_RESULTS_FILE}")
    
    try:
        # Use socketio.run instead of app.run
        socketio.run(app, debug=True, host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)
    finally:
        mongo_manager.close_connection()
        file_watcher.stop()
