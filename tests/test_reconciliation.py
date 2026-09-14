import importlib.util
from pathlib import Path

import pytest


def load_reconciliation():
    path = Path(__file__).parents[1] / "airflow" / "dags" / "reconciliation.py"
    spec = importlib.util.spec_from_file_location("reconciliation_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_metric_thresholds_distinguish_pass_warning_and_failure():
    module = load_reconciliation()
    assert module.compare_metric("rows", "orders", 100, 100, 0.01, 0.05).status == "PASS"
    assert module.compare_metric("rows", "orders", 100, 103, 0.01, 0.05).status == "WARN"
    assert module.compare_metric("rows", "orders", 100, 94, 0.01, 0.05).status == "FAIL"


@pytest.mark.parametrize("check_name", ["duplicate_grain", "orphan_foreign_key"])
def test_exact_integrity_check_fails_on_any_violation(check_name):
    module = load_reconciliation()
    assert module.exact_zero_check(check_name, "fact_orders", 0).status == "PASS"
    assert module.exact_zero_check(check_name, "fact_orders", 1).status == "FAIL"


def test_missing_required_table_stops_reconciliation():
    module = load_reconciliation()

    class NotFound(Exception):
        pass

    class Client:
        def get_table(self, table):
            raise NotFound(table)

    with pytest.raises(RuntimeError, match="Missing required reconciliation tables"):
        class Logger:
            def log_struct(self, payload, severity):
                assert payload["dag_run_id"] == "run-1"
                assert payload["status"] == "FAIL"
                assert severity == "ERROR"

        module.run_reconciliation(Client(), Logger(), "project", "run-1")


def test_warning_does_not_fail_airflow_task_but_failure_does():
    from airflow.exceptions import AirflowException

    module = load_reconciliation()
    warning = module.compare_metric("rows", "orders", 100, 103, 0.01, 0.05)
    module.raise_for_reconciliation_failures([warning], AirflowException)

    failure = module.compare_metric("rows", "orders", 100, 90, 0.01, 0.05)
    with pytest.raises(AirflowException, match="orders.rows"):
        module.raise_for_reconciliation_failures([failure], AirflowException)


def test_logger_failure_is_not_swallowed():
    module = load_reconciliation()

    class NotFound(Exception):
        pass

    class Client:
        def get_table(self, table):
            raise NotFound(table)

    class Logger:
        def log_struct(self, payload, severity):
            raise ConnectionError("logging unavailable")

    with pytest.raises(ConnectionError, match="logging unavailable"):
        module.run_reconciliation(Client(), Logger(), "project", "run-1")


def test_bigquery_query_failure_is_not_swallowed():
    module = load_reconciliation()

    class Client:
        def get_table(self, table):
            return object()

        def query(self, sql):
            raise ConnectionError("bigquery unavailable")

    with pytest.raises(ConnectionError, match="bigquery unavailable"):
        module.run_reconciliation(Client(), object(), "project", "run-1")
