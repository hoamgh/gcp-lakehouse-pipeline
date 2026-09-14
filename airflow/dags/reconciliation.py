"""Gold reconciliation checks, kept separate from DAG construction for testing."""

import os
from dataclasses import asdict, dataclass

GOLD_GRAINS = {
    "fact_orders": "order_id",
    "fact_order_items": "order_item_sk",
    "fact_payments": "payment_id",
    "fact_reviews": "review_id",
    "fact_shipments": "shipment_id",
    "dim_customers": "customer_sk",
    "dim_products": "product_id",
    "dim_sellers": "seller_id",
    "dim_date": "date_id",
}
SILVER_TABLES = (
    "silver_customers",
    "silver_products",
    "silver_sellers",
    "silver_orders",
    "silver_order_items",
    "silver_payments",
    "silver_reviews",
    "silver_shipments",
)


@dataclass(frozen=True)
class Thresholds:
    count_warning: float = 0.01
    count_failure: float = 0.05
    value_warning: float = 0.001
    value_failure: float = 0.01

    @classmethod
    def from_env(cls):
        return cls(
            count_warning=float(os.environ.get("RECON_COUNT_WARN_PCT", "0.01")),
            count_failure=float(os.environ.get("RECON_COUNT_FAIL_PCT", "0.05")),
            value_warning=float(os.environ.get("RECON_VALUE_WARN_PCT", "0.001")),
            value_failure=float(os.environ.get("RECON_VALUE_FAIL_PCT", "0.01")),
        )


@dataclass
class CheckResult:
    check_name: str
    table: str
    expected: float
    actual: float
    difference: float
    relative_difference: float
    status: str


def compare_metric(check_name, table, expected, actual, warning, failure):
    expected = float(expected or 0)
    actual = float(actual or 0)
    difference = actual - expected
    denominator = max(abs(expected), 1.0)
    relative_difference = abs(difference) / denominator
    status = "FAIL" if relative_difference > failure else "WARN" if relative_difference > warning else "PASS"
    return CheckResult(
        check_name,
        table,
        expected,
        actual,
        difference,
        relative_difference,
        status,
    )


def exact_zero_check(check_name, table, actual):
    return compare_metric(check_name, table, 0, actual, 0, 0)


def raise_for_reconciliation_failures(results, exception_type=RuntimeError):
    """Raise the caller's orchestration exception only for hard failures."""
    failures = [result for result in results if result.status == "FAIL"]
    if failures:
        summary = ", ".join(
            f"{result.table}.{result.check_name}" for result in failures
        )
        raise exception_type(f"Gold reconciliation hard failures: {summary}")


def _scalar(client, sql):
    row = next(iter(client.query(sql).result()))
    return row[0]


def _qualified(project_id, dataset, table):
    return f"`{project_id}.{dataset}.{table}`"


def _source_relations(project_id):
    def silver(table):
        return _qualified(project_id, "lakehouse_silver", table)

    orders = silver("silver_orders")
    return {
        "fact_orders": f"(SELECT * FROM {orders} WHERE order_status != 'TEST_STATUS')",
        "fact_order_items": (
            f"(SELECT i.* FROM {silver('silver_order_items')} i WHERE EXISTS "
            f"(SELECT 1 FROM {orders} o WHERE o.order_id=i.order_id AND o.order_status!='TEST_STATUS'))"
        ),
        "fact_payments": (
            f"(SELECT p.* FROM {silver('silver_payments')} p WHERE p.payment_id IS NOT NULL "
            f"AND p.order_id IS NOT NULL AND p.payment_value >= 0 AND EXISTS "
            f"(SELECT 1 FROM {orders} o WHERE o.order_id=p.order_id AND o.order_status!='TEST_STATUS'))"
        ),
        "fact_reviews": (
            f"(SELECT r.* FROM {silver('silver_reviews')} r WHERE r.review_id IS NOT NULL "
            f"AND r.order_id IS NOT NULL AND r.review_score BETWEEN 1 AND 5 AND EXISTS "
            f"(SELECT 1 FROM {orders} o WHERE o.order_id=r.order_id AND o.order_status!='TEST_STATUS'))"
        ),
        "fact_shipments": (
            f"(SELECT s.* FROM {silver('silver_shipments')} s WHERE s.order_id IS NOT NULL AND EXISTS "
            f"(SELECT 1 FROM {orders} o WHERE o.order_id=s.order_id AND o.order_status!='TEST_STATUS'))"
        ),
        "dim_customers": f"(SELECT DISTINCT customer_id FROM {silver('silver_customers')})",
        "dim_products": f"(SELECT DISTINCT product_id FROM {silver('silver_products')})",
        "dim_sellers": f"(SELECT DISTINCT seller_id FROM {silver('silver_sellers')})",
    }


def run_reconciliation(client, logger, project_id, dag_run_id, thresholds=None):
    """Run control totals and return results; caller turns failures into AirflowException."""
    thresholds = thresholds or Thresholds.from_env()
    gold_dataset = "lakehouse_gold"
    silver_dataset = "lakehouse_silver"

    missing = []
    for table in GOLD_GRAINS:
        try:
            client.get_table(f"{project_id}.{gold_dataset}.{table}")
        except Exception as exc:
            if exc.__class__.__name__ == "NotFound":
                missing.append(f"{gold_dataset}.{table}")
            else:
                raise
    for table in SILVER_TABLES:
        try:
            client.get_table(f"{project_id}.{silver_dataset}.{table}")
        except Exception as exc:
            if exc.__class__.__name__ == "NotFound":
                missing.append(f"{silver_dataset}.{table}")
            else:
                raise
    if missing:
        logger.log_struct(
            {
                "event_type": "gold_reconciliation",
                "dag_run_id": dag_run_id,
                "check_name": "required_tables_exist",
                "missing_tables": missing,
                "status": "FAIL",
            },
            severity="ERROR",
        )
        raise RuntimeError("Missing required reconciliation tables: " + ", ".join(missing))

    results = []
    sources = _source_relations(project_id)
    source_grains = {
        "fact_orders": "order_id",
        "fact_order_items": "order_item_id",
        "fact_payments": "payment_id",
        "fact_reviews": "review_id",
        "fact_shipments": "shipment_id",
        "dim_customers": "customer_id",
        "dim_products": "product_id",
        "dim_sellers": "seller_id",
    }
    for table, source_relation in sources.items():
        gold = _qualified(project_id, gold_dataset, table)
        gold_control_relation = (
            f"(SELECT * FROM {gold} WHERE is_current AND NOT is_inferred)"
            if table == "dim_customers"
            else gold
        )
        results.append(
            compare_metric(
                "row_count",
                table,
                _scalar(client, f"SELECT COUNT(*) FROM {source_relation}"),
                _scalar(client, f"SELECT COUNT(*) FROM {gold_control_relation}"),
                thresholds.count_warning,
                thresholds.count_failure,
            )
        )
        results.append(
            compare_metric(
                "distinct_grain_count",
                table,
                _scalar(client, f"SELECT COUNT(DISTINCT {source_grains[table]}) FROM {source_relation}"),
                _scalar(client, f"SELECT COUNT(DISTINCT {GOLD_GRAINS[table]}) FROM {gold_control_relation}"),
                thresholds.count_warning,
                thresholds.count_failure,
            )
        )

    for table, grain in GOLD_GRAINS.items():
        gold = _qualified(project_id, gold_dataset, table)
        results.append(
            exact_zero_check(
                "duplicate_grain",
                table,
                _scalar(client, f"SELECT COUNT(*) - COUNT(DISTINCT {grain}) FROM {gold}"),
            )
        )

    dim_date_count = _scalar(
        client,
        f"SELECT COUNT(*) FROM {_qualified(project_id, gold_dataset, 'dim_date')}",
    )
    results.append(
        CheckResult(
            "row_count_nonzero",
            "dim_date",
            1.0,
            float(dim_date_count),
            float(dim_date_count) - 1.0,
            0.0 if dim_date_count else 1.0,
            "PASS" if dim_date_count else "FAIL",
        )
    )

    def gold(table):
        return _qualified(project_id, gold_dataset, table)

    orphan_queries = {
        "fact_orders.customer_sk": f"SELECT COUNT(*) FROM {gold('fact_orders')} f LEFT JOIN {gold('dim_customers')} d USING(customer_sk) WHERE d.customer_sk IS NULL",
        "fact_order_items.order_id": f"SELECT COUNT(*) FROM {gold('fact_order_items')} f LEFT JOIN {gold('fact_orders')} d USING(order_id) WHERE d.order_id IS NULL",
        "fact_order_items.product_id": f"SELECT COUNT(*) FROM {gold('fact_order_items')} f LEFT JOIN {gold('dim_products')} d USING(product_id) WHERE d.product_id IS NULL",
        "fact_order_items.seller_id": f"SELECT COUNT(*) FROM {gold('fact_order_items')} f LEFT JOIN {gold('dim_sellers')} d USING(seller_id) WHERE d.seller_id IS NULL",
        "fact_payments.order_id": f"SELECT COUNT(*) FROM {gold('fact_payments')} f LEFT JOIN {gold('fact_orders')} d USING(order_id) WHERE d.order_id IS NULL",
        "fact_reviews.order_id": f"SELECT COUNT(*) FROM {gold('fact_reviews')} f LEFT JOIN {gold('fact_orders')} d USING(order_id) WHERE d.order_id IS NULL",
        "fact_shipments.order_id": f"SELECT COUNT(*) FROM {gold('fact_shipments')} f LEFT JOIN {gold('fact_orders')} d USING(order_id) WHERE d.order_id IS NULL",
    }
    for relation, sql in orphan_queries.items():
        results.append(exact_zero_check("orphan_foreign_key", relation, _scalar(client, sql)))

    payment_source = sources["fact_payments"]
    source_payment_total = _scalar(client, f"SELECT COALESCE(SUM(payment_value), 0) FROM {payment_source}")
    for table, sql in {
        "fact_payments": f"SELECT COALESCE(SUM(payment_value), 0) FROM {gold('fact_payments')}",
        "fact_orders": f"SELECT COALESCE(SUM(total_payment_value), 0) FROM {gold('fact_orders')}",
    }.items():
        results.append(
            compare_metric(
                "payment_total",
                table,
                source_payment_total,
                _scalar(client, sql),
                thresholds.value_warning,
                thresholds.value_failure,
            )
        )

    for result in results:
        payload = {
            "event_type": "gold_reconciliation",
            "dag_run_id": dag_run_id,
            **asdict(result),
        }
        severity = "ERROR" if result.status == "FAIL" else "WARNING" if result.status == "WARN" else "INFO"
        logger.log_struct(payload, severity=severity)

    return results
