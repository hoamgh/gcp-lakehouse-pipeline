"""
Continuously pulls messages from Google Cloud Pub/Sub and writes them to local JSON files.
This acts as a shim for local testing. In a real Dataproc environment, Spark would read 
from Pub/Sub directly using `format("pubsub")`.
"""
import os
import json
import time
import threading
from pathlib import Path
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from config import STAGING_DIR

PROJECT_ID = os.environ.get("PROJECT_ID")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID")
CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

if not STAGING_DIR.startswith("gs://"):
    os.makedirs(STAGING_DIR, exist_ok=True)

# --- Micro-batching Buffer Setup ---
# Dictionary to hold lists of JSON strings per entity
message_buffer = {}
buffer_lock = threading.Lock()
BATCH_SIZE = 50  # Very small batch size to intentionally create "Small File Problem"

def flush_buffer(entity):
    """Writes the buffered messages for an entity to a single NDJSON file."""
    with buffer_lock:
        if entity not in message_buffer or not message_buffer[entity]:
            return
        records = message_buffer[entity]
        message_buffer[entity] = []
    
    if not records:
        return
        
    file_name = f"batch_{int(time.time() * 1000)}.json"
    content = "\n".join(records) + "\n"

    entity_dir = f"{STAGING_DIR}/{entity}"
    if not STAGING_DIR.startswith("gs://"):
        os.makedirs(entity_dir, exist_ok=True)
        file_path = os.path.join(entity_dir, file_name)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
    else:
        # For GCS, use the python storage client
        from google.cloud import storage
        client = storage.Client()
        bucket_name = STAGING_DIR.split("/")[2]
        blob_path = f"staging/{entity}/{file_name}"
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(content)

        
    print(f"[Buffer] Flushed {len(records)} {entity} events to {file_name}")

def callback(message):
    try:
        data_str = message.data.decode("utf-8")
        record = json.loads(data_str)
        entity = record.pop("__entity", "unknown")
        
        # Add record to buffer
        with buffer_lock:
            if entity not in message_buffer:
                message_buffer[entity] = []
            message_buffer[entity].append(json.dumps(record, ensure_ascii=False))
            current_size = len(message_buffer[entity])
            
        message.ack()
        
        # If batch size reached, flush to disk
        if current_size >= BATCH_SIZE:
            flush_buffer(entity)
            
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

def flush_all_buffers():
    """Flushes any remaining messages in all buffers (useful on shutdown)."""
    with buffer_lock:
        entities = list(message_buffer.keys())
    for entity in entities:
        flush_buffer(entity)

def pull_data():
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    subscriber = pubsub_v1.SubscriberClient(credentials=creds)
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    
    print(f"Listening for messages on {subscription_path}...")
    print(f"Micro-batching enabled: {BATCH_SIZE} messages per file.")
    
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    
    with subscriber:
        try:
            # Block and listen indefinitely, checking every 5 seconds
            while True:
                time.sleep(5)
                # Time-based flush: flush any remaining data every 5 seconds
                # even if it hasn't reached BATCH_SIZE (Creates lots of small files!)
                flush_all_buffers() 
        except KeyboardInterrupt:
            print("\nStopping Pub/Sub listener...")
            streaming_pull_future.cancel()
            streaming_pull_future.result()
            print("Flushing remaining buffers to disk before exiting...")
            flush_all_buffers()
            print("Done.")

if __name__ == "__main__":
    pull_data()
