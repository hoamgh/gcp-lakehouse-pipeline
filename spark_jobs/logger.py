import os
import json
import logging
from datetime import datetime

# Initialize basic logger for console fallback
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_cloud_logger(log_name="lakehouse_pipeline"):
    """
    Returns a Google Cloud Logger if credentials are set or running on GCP, otherwise returns a fallback console logger.
    """
    try:
        import google.cloud.logging
        # On GCP (e.g. Dataproc), this automatically uses the attached Service Account
        client = google.cloud.logging.Client()
        return client.logger(log_name)
    except ImportError:
        logging.warning("google-cloud-logging is not installed. Falling back to console logging.")
    except Exception as e:
        logging.warning(f"Failed to initialize Google Cloud Logging: {e}. Falling back to console logging.")
    
    return None

def log_event(event_type, payload, severity="INFO"):
    """
    Generic function to log structured JSON to Cloud Logging or Console.
    """
    cloud_logger = get_cloud_logger()
    
    # Prepare standard payload
    full_payload = {
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        **payload
    }
    
    if cloud_logger:
        cloud_logger.log_struct(full_payload, severity=severity)
    
    # Always log to console as well for local debugging
    log_msg = f"[{event_type}] {json.dumps(full_payload)}"
    if severity in ["ERROR", "CRITICAL"]:
        logging.error(log_msg)
    elif severity == "WARNING":
        logging.warning(log_msg)
    else:
        logging.info(log_msg)

def log_ingestion(entity, record_count, batch_id=None, source="stream_to_pubsub"):
    """MON1: Raw Data Monitor"""
    payload = {
        "entity": entity,
        "record_count": record_count,
        "source": source
    }
    if batch_id:
        payload["batch_id"] = batch_id
    log_event("raw_data_ingestion", payload)

def log_quality_check(entity, corrupt_records_count, total_records=None):
    """MON2: Bronze Quality Monitor"""
    payload = {
        "entity": entity,
        "corrupt_records_count": corrupt_records_count,
        "action": "sent_to_dlq"
    }
    if total_records is not None:
        payload["total_records"] = total_records
        
    severity = "ERROR" if corrupt_records_count > 0 else "INFO"
    log_event("bronze_quality_check", payload, severity=severity)

def log_performance(entity, duration_seconds, output_rows, layer="silver"):
    """MON3: Silver Performance Monitor"""
    payload = {
        "entity": entity,
        "duration_seconds": duration_seconds,
        "output_rows": output_rows,
        "layer": layer
    }
    log_event("performance_monitor", payload)
