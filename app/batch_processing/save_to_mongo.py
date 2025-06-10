import sys
import os
from pathlib import Path

# Get the project root directory (2 levels up from current script location)
current_script_path = Path(__file__).parent  # /app/batch_processing
app_path = current_script_path.parent        # /app
project_root = app_path.parent               # /project_root

# Add project root to Python path
sys.path.insert(0, str(project_root))

print(f"Script location: {current_script_path}")
print(f"App path: {app_path}")
print(f"Project root: {project_root}")
print(f"Added to Python path: {project_root}")

# Now import should work
from app.serving_layer.backend.mongo_manager import MongoDataManager

def main():
    # Allow optional file path argument
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        # Use the results.json in the same directory as this script
        file_path = current_script_path / "results.json"
    
    # Convert to string for compatibility
    file_path = str(file_path)
    
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        print(f"Current working directory: {os.getcwd()}")
        print(f"Looking for file at: {os.path.abspath(file_path)}")
        
        # Try to find the file in common locations relative to project root
        possible_paths = [
            str(current_script_path / "results.json"),  # Same directory as script
            str(project_root / "app" / "batch_processing" / "results.json"),
            str(project_root / "batch_processing" / "results.json"),
            "results.json"  # Current working directory
        ]
        
        print("\nChecking alternative paths:")
        for path in possible_paths:
            abs_path = os.path.abspath(path)
            exists = os.path.exists(path)
            print(f"  {path} -> {abs_path} (exists: {exists})")
            if exists:
                file_path = path
                print(f"Found file at: {file_path}")
                break
        else:
            print("Could not find results.json file!")
            sys.exit(1)

    print(f"Using file path: {file_path}")
    
    try:
        manager = MongoDataManager()
        batch_id = manager.load_batch_results_from_file(file_path)
        if batch_id:
            print(f"Batch results saved to MongoDB with batch_id: {batch_id}")
            # Optionally, print a summary of what was inserted
            latest = manager.get_latest_batch_data()
            if latest:
                print("Summary of inserted data:")
                print(f"- Processed comments: {len(latest.get('processed_comments', []))}")
                print(f"- Sentiment samples: {len(latest.get('sentiment_samples', []))}")
                print(f"- Top words: {len(latest.get('top_words', []))}")
                print(f"- Hate speech metrics: {latest.get('hate_speech_metrics', {})}")
                print(f"- Comment length stats: {latest.get('comment_length_stats', {})}")
        else:
            print("Failed to save batch results to MongoDB.")
            sys.exit(2)
        manager.close_connection()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(3)

if __name__ == "__main__":
    main()