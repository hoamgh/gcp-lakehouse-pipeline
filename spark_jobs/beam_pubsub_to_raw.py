

import argparse
import json
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from apache_beam.transforms import window
from apache_beam.io.filesystems import FileSystems

# Import STAGING_DIR from config.py
import sys
sys.path.append(os.path.dirname(__file__))
from config import STAGING_DIR

PROJECT_ID = os.environ.get("PROJECT_ID", "hybrid-elt-lakehouse-pipeline")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID", "ecommerce-events-sub")
CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
GCS_BUCKET = os.environ.get("GCS_BUCKET", f"{PROJECT_ID}-lakehouse")

class ParseJsonFn(beam.DoFn):
    """Parse JSON and extract __entity"""
    def process(self, element):
        try:
            data_str = element.decode('utf-8')
            record = json.loads(data_str)
            entity = record.pop("__entity", "unknown")
            yield (entity, json.dumps(record, ensure_ascii=False))
        except Exception as e:
            logging.error(f"Error parsing json: {e}")

class WriteBatchFn(beam.DoFn):
    """Write a batch of records (from a specific window) to a local file"""
    def process(self, element, window=beam.DoFn.WindowParam):
        entity, records = element
        
        if entity == "unknown" or not records:
            return
            
        # Format window timestamps for filename
        window_start = window.start.to_utc_datetime().strftime("%Y%m%d_%H%M%S")
        window_end = window.end.to_utc_datetime().strftime("%Y%m%d_%H%M%S")
        
        # Write to GCS (or local depending on GCS_BUCKET var, but we force GCS here for Dataflow)
        entity_dir = f"gs://{GCS_BUCKET}/staging/{entity}"
        
        file_name = f"dataflow_batch_{window_start}_{window_end}.json"
        file_path = f"{entity_dir}/{file_name}"
        
        # Write NDJSON using Beam FileSystems (supports GCS natively)
        with FileSystems.create(file_path, mime_type='application/json') as f:
            f.write(("\n".join(records) + "\n").encode('utf-8'))
            
        logging.info(f"[{window_start} - {window_end}] Wrote {len(records)} records for '{entity}' to {file_path}")

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--window_size', type=int, default=30, help='Window size in seconds')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True

    subscription_path = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"
    logging.info(f"Starting Apache Beam Pipeline. Reading from {subscription_path}")
    logging.info(f"Window size: {known_args.window_size} seconds")
    
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p 
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=subscription_path)
            | 'ParseJSON' >> beam.ParDo(ParseJsonFn())
            | 'WindowInto' >> beam.WindowInto(window.FixedWindows(known_args.window_size))
            | 'GroupByKey' >> beam.GroupByKey()
            | 'WriteBatches' >> beam.ParDo(WriteBatchFn())
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
