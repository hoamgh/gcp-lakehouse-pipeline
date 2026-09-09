"""
Lambda Architecture Ingestion Pipeline (Apache Beam)
This pipeline implements the 'T-Branching' pattern:
1. Reads from Google Cloud Pub/Sub.
2. Branch A (Speed Layer): Aggregates metrics and writes idempotently to Firestore.
3. Branch B (Batch Layer): Dumps raw JSON files to Staging for downstream Spark batch processing (every 30s).
"""

import argparse
import json
import logging
import os

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)
from apache_beam.transforms import window
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

PROJECT_ID = os.environ.get("PROJECT_ID")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID", "ecommerce-events-sub")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
PIPELINE_ENV = os.environ.get("PIPELINE_ENV", "gcs")

if PIPELINE_ENV == "gcs":
    if not GCS_BUCKET:
        raise RuntimeError("GCS_BUCKET is required when PIPELINE_ENV=gcs")
    STAGING_DIR = f"gs://{GCS_BUCKET}/staging"
else:
    STAGING_DIR = os.path.join(os.path.dirname(__file__), "..", "output", "staging")

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
            data_str = element.decode("utf-8")
            record = json.loads(data_str)
            entity = record.pop("__entity", "unknown")

            # Main output: (entity, clean_json_string) for Batch dump
            yield (entity, json.dumps(record, ensure_ascii=False))

            if entity in {"orders", "payments"}:
                yield beam.pvalue.TaggedOutput("speed_layer", {"entity": entity, **record})

        except Exception as e:
            logging.error(f"Error parsing json: {e}")
            yield (
                "_ingestion_dlq",
                json.dumps({"raw_payload": repr(element), "error": str(e)}),
            )


class ExtractRevenueFn(beam.DoFn):
    def process(self, record):
        entity = record["entity"]
        revenue = float(record.get("payment_value", 0.0)) if entity == "payments" else 0.0
        order_count = 1 if entity == "orders" else 0
        yield ("global", (revenue, order_count))


class PrintDashboardFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        key, (total_revenue, order_count) = element
        window_start = window.start.to_utc_datetime().strftime("%H:%M:%S")
        window_end = window.end.to_utc_datetime().strftime("%H:%M:%S")

        print("\n" + "🔥" * 25)
        print(f"📊 LIVE DASHBOARD UPDATE [{window_start} - {window_end}]")
        print("🔥" * 25)
        print(f"💰 Total Revenue : ${total_revenue:,.2f}")
        print(f"📦 Orders Placed : {order_count:,}")
        print("🔥" * 25 + "\n")
        yield element


class WriteToFirestoreFn(beam.DoFn):
    """Persist each window once so Dataflow retries cannot double-count totals."""

    def __init__(self, project_id):
        self.project_id = project_id

    def setup(self):
        from google.cloud import firestore

        self.db = firestore.Client(project=self.project_id)

    def process(self, element, window=beam.DoFn.WindowParam):
        from google.cloud import firestore

        _, (revenue, order_count) = element
        window_end = window.end.to_utc_datetime()
        window_id = window_end.strftime("%Y%m%dT%H%M%S")
        date_id = window_end.strftime("%Y-%m-%d")
        live_ref = self.db.collection("realtime_dashboard").document("live_metrics")
        daily_ref = self.db.collection("realtime_dashboard").document(f"daily_totals_{date_id}")
        window_ref = self.db.collection("realtime_windows").document(window_id)
        transaction = self.db.transaction()

        @firestore.transactional
        def commit_window(txn):
            if window_ref.get(transaction=txn).exists:
                return False
            txn.set(
                window_ref,
                {
                    "window_end": window_end,
                    "orders": order_count,
                    "revenue": round(revenue, 2),
                },
            )
            txn.set(
                daily_ref,
                {
                    "date": date_id,
                    "total_orders": firestore.Increment(order_count),
                    "total_revenue": firestore.Increment(round(revenue, 2)),
                    "last_updated": firestore.SERVER_TIMESTAMP,
                },
                merge=True,
            )
            txn.set(
                live_ref,
                {
                    "orders_in_window": order_count,
                    "revenue_in_window": round(revenue, 2),
                    "avg_order_value": round(revenue / order_count, 2) if order_count else 0.0,
                    "window_end": window_end,
                    "window_time": window_end.strftime("%H:%M:%S"),
                },
            )
            return True

        commit_window(transaction)
        yield element


class WriteBatchFn(beam.DoFn):
    """Write a batch of records (from a specific window) to a file"""

    def process(self, element, window=beam.DoFn.WindowParam):
        entity, records = element
        records = list(records)

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
            raise


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True
    project_id = pipeline_options.view_as(GoogleCloudOptions).project or PROJECT_ID
    if not project_id:
        raise RuntimeError("PROJECT_ID or Beam --project is required")
    # We use DirectRunner by default, but it can be overridden by passing --runner=DataflowRunner

    subscription_path = f"projects/{project_id}/subscriptions/{SUBSCRIPTION_ID}"

    print("🚀 Starting Lambda Ingestion Pipeline (Apache Beam)...")
    print("This pipeline uses T-Branching to split Data into Speed Layer and Batch Layer.")

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Read from Pub/Sub
        messages = p | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=subscription_path)

        # 2. Parse and Branch (T-Branching)
        branched_data = messages | "Parse and Branch" >> beam.ParDo(
            ParseAndBranchFn()
        ).with_outputs("speed_layer", main="batch_layer")

        # ==========================================
        # BRANCH A: SPEED LAYER (Real-time Dashboard)
        # ==========================================
        (
            branched_data.speed_layer
            | "Speed Window 30s" >> beam.WindowInto(window.FixedWindows(30))
            | "Extract Revenue" >> beam.ParDo(ExtractRevenueFn())
            | "Sum Revenue"
            >> beam.CombinePerKey(
                lambda values: (sum(v[0] for v in values), sum(v[1] for v in values))
            )
            | "Update Live Dashboard" >> beam.ParDo(PrintDashboardFn())
            | "Write Metrics to Firestore" >> beam.ParDo(WriteToFirestoreFn(project_id))
        )

        # ==========================================
        # BRANCH B: BATCH LAYER (Raw Dump to Staging)
        # ==========================================
        (
            branched_data.batch_layer
            | "Enforce Type" >> beam.Map(lambda x: (x[0], x[1])).with_output_types(tuple[str, str])
            | "Batch Window 30s" >> beam.WindowInto(window.FixedWindows(30))
            | "Group by Entity" >> beam.GroupByKey()
            | "Dump to Staging" >> beam.ParDo(WriteBatchFn())
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
