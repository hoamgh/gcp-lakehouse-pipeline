"""
Lambda Architecture Ingestion Pipeline (Apache Beam)
This pipeline implements the 'T-Branching' pattern:
1. Reads from Google Cloud Pub/Sub.
2. Branch A (Speed Layer): Aggregates revenue and prints to a live dashboard (every 30s).
3. Branch B (Batch Layer): Dumps raw JSON files to Staging for downstream Spark batch processing (every 30s).
"""
import argparse
import json
import logging
import os
import sys
import typing

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from apache_beam.transforms import window
from apache_beam.io.filesystems import FileSystems

sys.path.append(os.path.dirname(__file__))
from config import STAGING_DIR

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

PROJECT_ID = os.environ.get("PROJECT_ID", "hybrid-elt-lakehouse-pipeline")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID", "ecommerce-events-sub")

if not STAGING_DIR.startswith("gs://"):
    os.makedirs(STAGING_DIR, exist_ok=True)

class ParseAndBranchFn(beam.DoFn):
    """
    Parses JSON and branches the data.
    Yields (entity, json_string) for the Batch Layer.
    Also yields dict payload to a tagged output for the Speed Layer.
    """
    def process(self, element):
        try:
            data_str = element.decode('utf-8')
            record = json.loads(data_str)
            entity = record.pop("__entity", "unknown")
            
            # Main output: (entity, clean_json_string) for Batch dump
            yield (entity, json.dumps(record, ensure_ascii=False))
            
            # Tagged output for Speed Layer (only orders)
            if entity == "orders":
                yield beam.pvalue.TaggedOutput('speed_layer', record)
                
        except Exception as e:
            logging.error(f"Error parsing json: {e}")

class ExtractRevenueFn(beam.DoFn):
    def process(self, record):
        revenue = float(record.get("total_amount", 0.0))
        yield ("global", (revenue, 1))

class PrintDashboardFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        key, (total_revenue, order_count) = element
        window_start = window.start.to_utc_datetime().strftime('%H:%M:%S')
        window_end = window.end.to_utc_datetime().strftime('%H:%M:%S')
        
        print("\n" + "🔥"*25)
        print(f"📊 LIVE DASHBOARD UPDATE [{window_start} - {window_end}]")
        print("🔥"*25)
        print(f"💰 Total Revenue : ${total_revenue:,.2f}")
        print(f"📦 Orders Placed : {order_count:,}")
        print("🔥"*25 + "\n")
        yield element

class WriteBatchFn(beam.DoFn):
    """Write a batch of records (from a specific window) to a file"""
    def process(self, element, window=beam.DoFn.WindowParam):
        entity, records = element
        
        if entity == "unknown" or not records:
            return
            
        window_start = window.start.to_utc_datetime().strftime("%Y%m%d_%H%M%S")
        window_end = window.end.to_utc_datetime().strftime("%Y%m%d_%H%M%S")
        
        entity_dir = f"{STAGING_DIR}/{entity}"
        
        # In a real environment, this ensures the directory exists
        if not entity_dir.startswith("gs://"):
            os.makedirs(entity_dir, exist_ok=True)
            
        file_name = f"beam_batch_{window_start}_{window_end}.json"
        file_path = f"{entity_dir}/{file_name}"
        
        try:
            with FileSystems.create(file_path) as writer:
                for r in records:
                    writer.write(r.encode("utf-8") + b"\n")
            logging.info(f"Dumped {len(records)} records to {file_path}")
        except Exception as e:
            logging.error(f"Error writing to {file_path}: {e}")

def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    # We use DirectRunner by default, but it can be overridden by passing --runner=DataflowRunner
    
    subscription_path = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"
    
    print("🚀 Starting Lambda Ingestion Pipeline (Apache Beam)...")
    print("This pipeline uses T-Branching to split Data into Speed Layer and Batch Layer.")
    
    with beam.Pipeline(options=pipeline_options) as p:
        
        # 1. Read from Pub/Sub
        messages = p | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=subscription_path)
        
        # 2. Parse and Branch (T-Branching)
        branched_data = (
            messages
            | "Parse and Branch" >> beam.ParDo(ParseAndBranchFn()).with_outputs('speed_layer', main='batch_layer')
        )
        
        # ==========================================
        # BRANCH A: SPEED LAYER (Real-time Dashboard)
        # ==========================================
        (
            branched_data.speed_layer
            | "Speed Window 30s" >> beam.WindowInto(window.FixedWindows(30))
            | "Extract Revenue" >> beam.ParDo(ExtractRevenueFn())
            | "Sum Revenue" >> beam.CombinePerKey(lambda values: (sum(v[0] for v in values), sum(v[1] for v in values)))
            | "Update Live Dashboard" >> beam.ParDo(PrintDashboardFn())
        )
        
        # ==========================================
        # BRANCH B: BATCH LAYER (Raw Dump to Staging)
        # ==========================================
        (
            branched_data.batch_layer
            | "Enforce Type" >> beam.Map(lambda x: (x[0], x[1])).with_output_types(typing.Tuple[str, str])
            | "Batch Window 30s" >> beam.WindowInto(window.FixedWindows(30))
            | "Group by Entity" >> beam.GroupByKey()
            | "Dump to Staging" >> beam.ParDo(WriteBatchFn())
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
