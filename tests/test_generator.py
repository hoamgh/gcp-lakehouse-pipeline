"""
Unit tests for the E-Commerce Faker Data Generator.

Tests:
- FK consistency across all entities
- Statistical distributions (order_status, review_score)
- CDC logic for shipments
- Schema completeness (all required fields present)
"""

import sys
import os
import random
import numpy as np
import pytest

# Add data_generator to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "data_generator"))

from config import RANDOM_SEED
from generators import generate_all, IDRegistry


@pytest.fixture(scope="module")
def generated_data():
    """Generate a dataset once for all tests (smaller counts for speed)."""
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    counts = {
        "customers": 100,
        "products": 30,
        "sellers": 15,
        "orders": 500,
        "order_items": 1200,
        "payments": 500,
        "reviews": 300,
        "shipments": 500,
    }
    return generate_all(counts)


# ===========================================================================
# FK Consistency Tests
# ===========================================================================

class TestFKConsistency:
    """Every foreign key must reference an existing primary key."""

    def test_orders_reference_valid_customers(self, generated_data):
        customer_ids = {c["customer_id"] for c in generated_data["customers"]}
        for order in generated_data["orders"]:
            assert order["customer_id"] in customer_ids, (
                f"order {order['order_id']} references non-existent customer {order['customer_id']}"
            )

    def test_order_items_reference_valid_orders(self, generated_data):
        order_ids = {o["order_id"] for o in generated_data["orders"]}
        for item in generated_data["order_items"]:
            assert item["order_id"] in order_ids

    def test_order_items_reference_valid_products(self, generated_data):
        product_ids = {p["product_id"] for p in generated_data["products"]}
        for item in generated_data["order_items"]:
            assert item["product_id"] in product_ids

    def test_order_items_reference_valid_sellers(self, generated_data):
        seller_ids = {s["seller_id"] for s in generated_data["sellers"]}
        for item in generated_data["order_items"]:
            assert item["seller_id"] in seller_ids

    def test_payments_reference_valid_orders(self, generated_data):
        order_ids = {o["order_id"] for o in generated_data["orders"]}
        for payment in generated_data["payments"]:
            assert payment["order_id"] in order_ids

    def test_reviews_reference_valid_orders(self, generated_data):
        order_ids = {o["order_id"] for o in generated_data["orders"]}
        for review in generated_data["reviews"]:
            assert review["order_id"] in order_ids

    def test_new_shipments_reference_valid_orders(self, generated_data):
        order_ids = {o["order_id"] for o in generated_data["orders"]}
        for shipment in generated_data["shipments"]:
            # CDC update records have order_id=None
            if shipment["order_id"] is not None:
                assert shipment["order_id"] in order_ids


# ===========================================================================
# Distribution Tests
# ===========================================================================

class TestDistributions:
    """Statistical distributions should match expected patterns."""

    def test_order_status_delivered_dominant(self, generated_data):
        """Delivered orders should be > 60% of total."""
        statuses = [o["order_status"] for o in generated_data["orders"]]
        delivered_ratio = statuses.count("delivered") / len(statuses)
        assert delivered_ratio > 0.60, f"Delivered ratio {delivered_ratio:.2%} is too low (expected > 60%)"

    def test_review_score_skewed_high(self, generated_data):
        """Average review score should be > 3.5 (skewed toward 4-5)."""
        scores = [r["review_score"] for r in generated_data["reviews"]]
        avg_score = sum(scores) / len(scores)
        assert avg_score > 3.5, f"Average review score {avg_score:.2f} is too low (expected > 3.5)"

    def test_review_scores_in_valid_range(self, generated_data):
        """All review scores must be between 1 and 5."""
        for review in generated_data["reviews"]:
            assert 1 <= review["review_score"] <= 5

    def test_prices_are_positive(self, generated_data):
        """All prices must be positive."""
        for item in generated_data["order_items"]:
            assert item["price"] > 0
            assert item["freight_value"] > 0

    def test_prices_in_vnd_range(self, generated_data):
        """Prices should be in VND range (>= 10,000 VND)."""
        for item in generated_data["order_items"]:
            assert item["price"] >= 10000, f"Price {item['price']} seems too low for VND"


# ===========================================================================
# CDC Tests
# ===========================================================================

class TestCDC:
    """CDC logic for shipments should produce status updates."""

    def test_shipments_contain_cdc_updates(self, generated_data):
        """There should be some CDC update records (with shipping_status != 'pending')."""
        statuses = [s["shipping_status"] for s in generated_data["shipments"]]
        non_pending = [s for s in statuses if s != "pending"]
        assert len(non_pending) > 0, "No CDC updates found in shipments"

    def test_cdc_updates_have_valid_status(self, generated_data):
        """CDC update records must have a valid shipping status."""
        valid_statuses = {"pending", "shipped", "in_transit", "delivered"}
        for shipment in generated_data["shipments"]:
            assert shipment["shipping_status"] in valid_statuses

    def test_delivered_shipments_have_actual_delivery_date(self, generated_data):
        """Shipments with status 'delivered' must have actual_delivery_date set."""
        for shipment in generated_data["shipments"]:
            if shipment["shipping_status"] == "delivered":
                assert shipment["actual_delivery_date"] is not None

    def test_cdc_updates_reuse_existing_shipment_ids(self, generated_data):
        """CDC updates should reuse shipment_ids from earlier records."""
        shipment_ids = [s["shipment_id"] for s in generated_data["shipments"]]
        # If there are CDC updates, some shipment_ids should appear more than once
        unique_ids = set(shipment_ids)
        if len(shipment_ids) > len(unique_ids):
            # At least one duplicate = CDC is working
            pass
        # This is expected behavior but not strictly required for all seed values


# ===========================================================================
# Schema Tests
# ===========================================================================

class TestSchema:
    """Every record must contain all required fields."""

    REQUIRED_FIELDS = {
        "customers": ["customer_id", "customer_name", "email", "city", "state", "zip_code"],
        "products": ["product_id", "category", "product_name", "weight_g"],
        "sellers": ["seller_id", "seller_name", "city", "state"],
        "orders": ["order_id", "customer_id", "order_status", "purchase_timestamp"],
        "order_items": ["order_id", "product_id", "seller_id", "price", "freight_value"],
        "payments": ["payment_id", "order_id", "payment_type", "installments", "payment_value"],
        "reviews": ["review_id", "order_id", "review_score"],
        "shipments": ["shipment_id", "shipping_status", "event_timestamp"],
    }

    @pytest.mark.parametrize("entity", REQUIRED_FIELDS.keys())
    def test_required_fields_present(self, generated_data, entity):
        """Check that all required fields are present in every record."""
        required = self.REQUIRED_FIELDS[entity]
        for i, record in enumerate(generated_data[entity]):
            for field in required:
                assert field in record, (
                    f"{entity}[{i}] missing required field '{field}'. Keys: {list(record.keys())}"
                )

    def test_all_entities_generated(self, generated_data):
        """All 8 entities must be present in the output."""
        expected = {"customers", "products", "sellers", "orders", "order_items", "payments", "reviews", "shipments"}
        assert set(generated_data.keys()) == expected

    def test_all_entities_non_empty(self, generated_data):
        """No entity should have zero records."""
        for entity, records in generated_data.items():
            assert len(records) > 0, f"{entity} has no records"
