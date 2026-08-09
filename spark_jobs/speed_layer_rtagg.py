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

class ParseAndFilterOrdersFn(beam.DoFn):
    def process(self, element):
        try:
            # Parse the Pub/Sub message
            data = json.loads(element.decode("utf-8"))
            # Only process "orders"
            if data.get("__entity") == "orders":
                # We yield a tuple of (dummy_key, (revenue, 1))
                # Revenue is stored in 'total_amount'
                revenue = float(data.get("total_amount", 0.0))
                yield ("global", (revenue, 1))
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

def run():
    logging.getLogger().setLevel(logging.INFO)
    print("🚀 Starting Speed Layer (Apache Beam DirectRunner)...")
    print("Waiting for messages...")

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION_PATH)
            | "Parse & Filter Orders" >> beam.ParDo(ParseAndFilterOrdersFn())
            | "Window 30s" >> beam.WindowInto(window.FixedWindows(30))
            | "Sum Revenue & Count" >> beam.CombinePerKey(
                lambda values: (
                    sum(v[0] for v in values),
                    sum(v[1] for v in values)
                )
            )
            | "Print Dashboard" >> beam.ParDo(PrintDashboardFn())
        )

if __name__ == "__main__":
    run()
