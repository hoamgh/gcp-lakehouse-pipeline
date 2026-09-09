import importlib.util
import os
from pathlib import Path


def load_dag_config():
    os.environ.setdefault("PROJECT_ID", "test-project")
    os.environ.setdefault("GCS_BUCKET", "test-bucket")
    os.environ.setdefault("SERVICE_ACCOUNT_NAME", "test-sa")
    path = Path(__file__).parents[1] / "airflow" / "dags" / "dag_config.py"
    spec = importlib.util.spec_from_file_location("dag_config_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_batch_id_is_deterministic_and_gcp_safe():
    config = load_dag_config()
    first = config.make_batch_id("Raw_To_Bronze", "pipeline", "scheduled__2026-09-09T02:00:00+00:00")
    second = config.make_batch_id("Raw_To_Bronze", "pipeline", "scheduled__2026-09-09T02:00:00+00:00")

    assert first == second
    assert len(first) <= 63
    assert first[0].isalpha() and first[-1].isalnum()
    assert all(character.islower() or character.isdigit() or character == "-" for character in first)
