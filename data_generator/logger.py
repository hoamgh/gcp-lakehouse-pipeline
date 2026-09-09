"""Structured ingestion logging with a local console fallback."""

import json
import logging
from datetime import datetime, timezone


def log_ingestion(entity: str, record_count: int) -> None:
    payload = {
        "event_type": "raw_data_ingestion",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "entity": entity,
        "record_count": record_count,
        "source": "stream_to_pubsub",
    }
    try:
        from google.cloud import logging as cloud_logging

        cloud_logging.Client().logger("lakehouse_pipeline").log_struct(payload, severity="INFO")
    except Exception as exc:
        logging.warning("Cloud Logging unavailable: %s", exc)

    logging.info("[raw_data_ingestion] %s", json.dumps(payload))
