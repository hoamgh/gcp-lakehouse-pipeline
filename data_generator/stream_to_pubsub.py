"""
Continuous data generator that publishes events to GCP Pub/Sub.
Mimics a real-time stream of e-commerce transactions and CDC updates.
"""
import json
import time
import random
import os
from datetime import datetime
from google.cloud import pubsub_v1
from google.oauth2 import service_account
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "spark_jobs"))
from logger import log_ingestion
# Import from existing generators
from generators import (
    IDRegistry, generate_customers, generate_products, generate_sellers,
    generate_orders, generate_order_items, generate_payments,
    generate_reviews, generate_shipments
)

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

PROJECT_ID = os.environ.get("PROJECT_ID")
TOPIC_ID = os.environ.get("TOPIC_ID", "ecommerce-events")
CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

def default_json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def get_publisher():
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    return pubsub_v1.PublisherClient(credentials=creds)

def publish_records(publisher, topic_path, entity_name, records):
    for record in records:
        # Inject the entity type so Spark knows how to parse it
        record["__entity"] = entity_name
        data_str = json.dumps(record, default=default_json_serializer)
        data_bytes = data_str.encode("utf-8")
        publisher.publish(topic_path, data_bytes)
    
    count = len(records)
    print(f"Published {count} {entity_name} events.")
    if count > 0:
        log_ingestion(entity=entity_name, record_count=count)

def stream_data():
    publisher = get_publisher()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    
    registry = IDRegistry()
    
    print(f"Starting stream to Pub/Sub topic: {topic_path}")
    print("Press Ctrl+C to stop.")
    
    iteration = 1
    try:
        while True:
            print(f"\n--- Batch {iteration} ---")
            
            # Generate small batches to mimic a live stream
            customers = generate_customers(random.randint(1, 5), registry)
            products = generate_products(random.randint(1, 3), registry)
            sellers = generate_sellers(random.randint(0, 2), registry)
            
            # Ensure we have parents before generating children
            if registry.get_all("customers") and registry.get_all("products") and registry.get_all("sellers"):
                
                # Simulate Flash Sale Bursts (Velocity control)
                is_flash_sale = random.random() < 0.05  # 5% chance of a burst
                if is_flash_sale:
                    print("⚡ FLASH SALE BURST! Generating massive volume... ⚡")
                    n_orders = random.randint(50, 150) # Yields ~200-600 msgs/s
                else:
                    n_orders = random.randint(2, 6) # Baseline: Yields ~8-25 msgs/s
                    
                orders = generate_orders(n_orders, registry)
                order_items = generate_order_items(int(n_orders * 1.5), registry)
                payments = generate_payments(n_orders, registry)
                
                # ~30% of orders get a review
                reviews = generate_reviews(int(n_orders * 0.3), registry)
                
                # Shipments (some new, some CDC updates of existing)
                shipments = generate_shipments(n_orders, registry)
                
                publish_records(publisher, topic_path, "customers", customers)
                publish_records(publisher, topic_path, "products", products)
                publish_records(publisher, topic_path, "sellers", sellers)
                publish_records(publisher, topic_path, "orders", orders)
                publish_records(publisher, topic_path, "order_items", order_items)
                publish_records(publisher, topic_path, "payments", payments)
                publish_records(publisher, topic_path, "reviews", reviews)
                publish_records(publisher, topic_path, "shipments", shipments)
            
            iteration += 1
            # Frequency settings
            BATCH_INTERVAL = 1  # seconds between batches
            time.sleep(BATCH_INTERVAL)
            
    except KeyboardInterrupt:
        print("\nStreaming stopped.")

if __name__ == "__main__":
    stream_data()
