"""
Speed Layer (Real-time Aggregation) using Apache Beam.
Reads directly from Pub/Sub, applies a 30-second window, 
and aggregates live revenue and order counts for a Dashboard.
"""
import os
import json
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import apache_beam.transforms.window as window

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

PROJECT_ID = os.environ.get("PROJECT_ID")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID")
CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

# Use a separate subscription for the Speed Layer if possible. 
# For this demo, we reuse the existing one, but it means messages might be split 
# between pull_pubsub_to_raw.py and this script if both are running.
SUBSCRIPTION_PATH = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

class ParseEventsFn(beam.DoFn):
    """
    Parses each Pub/Sub message and emits aggregation tuples.
    - `orders`  → (key, (0.0, 1)) : contributes 1 to order count
    - `payments` → (key, (payment_value, 0)) : contributes revenue
    Both use the same key 'global' so CombinePerKey merges them in the same window.
    """
    def process(self, element):
        try:
            data = json.loads(element.decode("utf-8"))
            entity = data.get("__entity")

            if entity == "orders":
                yield ("global", (0.0, 1))

            elif entity == "payments":
                revenue = float(data.get("payment_value", 0.0))
                yield ("global", (revenue, 0))

        except Exception as e:
            logging.warning(f"Failed to parse message: {e}")

class PrintDashboardFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        key, (total_revenue, order_count) = element
        window_start = window.start.to_utc_datetime().strftime('%H:%M:%S')
        window_end = window.end.to_utc_datetime().strftime('%H:%M:%S')
        
        print("\n" + "="*50)
        print(f"📊 LIVE DASHBOARD UPDATE [{window_start} - {window_end}]")
        print("="*50)
        print(f"💰 Total Revenue : ${total_revenue:,.2f}")
        print(f"📦 Orders Placed : {order_count:,}")
        print("="*50 + "\n")
        yield element

class WriteToFirestoreFn(beam.DoFn):
    def __init__(self, project_id):
        self.project_id = project_id

    def setup(self):
        from google.cloud import firestore
        self.db = firestore.Client(project=self.project_id)

    def process(self, element, window=beam.DoFn.WindowParam):
        from google.cloud.firestore import SERVER_TIMESTAMP
        from google.cloud.firestore_v1 import transforms

        key, (total_revenue, order_count) = element
        window_end_dt = window.end.to_utc_datetime()
        today = window_end_dt.strftime("%Y-%m-%d")
        avg_order_value = (total_revenue / order_count) if order_count > 0 else 0.0

        try:
            # --- Document 1: live_metrics (overwrite every 30s) ---
            # Shows velocity: what happened in the LAST 30 seconds
            live_ref = self.db.collection("realtime_dashboard").document("live_metrics")
            live_ref.set({
                "orders_in_window": order_count,
                "revenue_in_window": round(total_revenue, 2),
                "avg_order_value": round(avg_order_value, 2),
                "window_end": window_end_dt,
                "window_time": window_end_dt.strftime("%H:%M:%S"),
            })

            # --- Document 2: daily_totals (atomic increment) ---
            # Shows running totals accumulated since the pipeline started today
            daily_ref = self.db.collection("realtime_dashboard").document(f"daily_totals_{today}")
            daily_ref.set({
                "date": today,
                "total_orders": transforms.Increment(order_count),
                "total_revenue": transforms.Increment(round(total_revenue, 2)),
                "last_updated": SERVER_TIMESTAMP,
            }, merge=True)

            logging.info(
                f"✅ Firestore updated | Window: Orders={order_count}, "
                f"Revenue={total_revenue:,.0f} VND, Avg={avg_order_value:,.0f} VND"
            )
        except Exception as e:
            logging.error(f"❌ Firestore write failed: {e}")

        yield element


def run():
    logging.getLogger().setLevel(logging.INFO)
    print("🚀 Starting Speed Layer (Apache Beam DirectRunner)...")
    print("Waiting for messages...")

    options = PipelineOptions(["--runner=DirectRunner"])
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION_PATH)
            | "Parse Events" >> beam.ParDo(ParseEventsFn())
            | "Window 30s" >> beam.WindowInto(window.FixedWindows(30))
            | "Sum Revenue & Count" >> beam.CombinePerKey(
                lambda values: (
                    sum(v[0] for v in values),
                    sum(v[1] for v in values)
                )
            )
            | "Print Dashboard" >> beam.ParDo(PrintDashboardFn())
            | "Write to Firestore" >> beam.ParDo(WriteToFirestoreFn(PROJECT_ID))
        )

if __name__ == "__main__":
    run()
