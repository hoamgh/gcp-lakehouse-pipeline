"""
Data generators for all 8 Olist-style E-Commerce entities (Vietnam localized).

Contains:
- IDRegistry: manages generated IDs for FK consistency + CDC state for shipments
- 8 generator functions (one per entity)
- generate_all(): orchestrates generation in correct FK-dependency order
"""

import uuid
import random
from datetime import datetime, timedelta
from typing import Any

from faker import Faker

from config import (
    RECORD_COUNTS,
    ORDER_STATUS_OPTIONS, ORDER_STATUS_WEIGHTS,
    PAYMENT_TYPE_OPTIONS, PAYMENT_TYPE_WEIGHTS,
    SHIPPING_STATUS_TRANSITION,
    CDC_UPDATE_RATIO,
    CARRIERS,
    weighted_choice, log_normal_price, skewed_score,
    random_installments, pick_vietnam_location, pick_product_category,
    generate_product_name,
)

fake = Faker("vi_VN")  # Vietnamese locale


# ===========================================================================
# ID Registry — manages FK pools + CDC state
# ===========================================================================

class IDRegistry:
    """
    Keeps track of all generated IDs per entity so that downstream generators
    can reference valid foreign keys. Also tracks shipment statuses for CDC.
    """

    def __init__(self):
        self._ids: dict[str, list[str]] = {}
        # CDC state: shipment_id -> current shipping_status
        self._shipment_states: dict[str, str] = {}
        # Track order_id -> list of shipment_ids for referential integrity
        self._order_shipments: dict[str, list[str]] = {}

    def register(self, entity: str, entity_id: str) -> None:
        """Register a new ID for an entity."""
        self._ids.setdefault(entity, []).append(entity_id)

    def get_random(self, entity: str) -> str:
        """Get a random ID from the pool of a given entity."""
        pool = self._ids.get(entity, [])
        if not pool:
            raise ValueError(f"No IDs registered for entity '{entity}'. Generate it first.")
        return random.choice(pool)

    def get_all(self, entity: str) -> list[str]:
        """Get all IDs for a given entity."""
        return self._ids.get(entity, [])

    def count(self, entity: str) -> int:
        """Count registered IDs for an entity."""
        return len(self._ids.get(entity, []))

    # --- CDC helpers for shipments ---

    def register_shipment(self, shipment_id: str, order_id: str, status: str) -> None:
        """Register a shipment with its current status for CDC tracking."""
        self._shipment_states[shipment_id] = status
        self._order_shipments.setdefault(order_id, []).append(shipment_id)

    def get_updatable_shipments(self) -> list[tuple[str, str]]:
        """
        Get shipments that can be updated (not yet in a terminal state).
        Returns list of (shipment_id, current_status).
        """
        return [
            (sid, current_status)
            for sid, current_status in self._shipment_states.items()
            if SHIPPING_STATUS_TRANSITION.get(current_status) is not None
        ]

    def update_shipment_status(self, shipment_id: str, new_status: str) -> None:
        """Update the CDC state of a shipment."""
        self._shipment_states[shipment_id] = new_status


# ===========================================================================
# Generator functions
# ===========================================================================

# ---- Timestamps helpers ----

# Simulation window: orders span the last 12 months
_NOW = datetime(2026, 8, 5, 12, 0, 0)
_ORDER_START = _NOW - timedelta(days=365)


def _random_timestamp(start: datetime, end: datetime) -> str:
    """Generate a random ISO timestamp between start and end."""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    dt = start + timedelta(seconds=random_seconds)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _timestamp_after(base_ts: str, min_hours: int = 1, max_hours: int = 72) -> str:
    """Generate a timestamp some hours after the base timestamp."""
    base = datetime.fromisoformat(base_ts)
    offset = timedelta(hours=random.randint(min_hours, max_hours))
    return (base + offset).strftime("%Y-%m-%dT%H:%M:%S")


# ---- 1. Customers ----

def generate_customers(n: int, registry: IDRegistry) -> list[dict[str, Any]]:
    """Generate n customer records with Vietnamese locations."""
    records = []
    for _ in range(n):
        cid = str(uuid.uuid4())
        location = pick_vietnam_location()
        record = {
            "customer_id": cid,
            "customer_name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "city": location["city"],
            "state": location["state"],
        }
        # Inject Missing Value (Null) error for 2% of records (To be cleaned in Silver layer)
        if random.random() < 0.02:
            record["email"] = None
        # Inject schema error (type mismatch) for 1% of records (Realistic rate)
        if random.random() < 0.01:
            record["customer_name"] = {"first_name": "Lỗi", "last_name": "Cấu Trúc"} # Dict instead of String
            record["zip_code"] = [123, 456] # List instead of String
        records.append(record)
        registry.register("customers", cid)
    return records


# ---- 2. Products ----

def generate_products(n: int, registry: IDRegistry) -> list[dict[str, Any]]:
    """Generate n product records with realistic dimensions."""
    records = []
    for _ in range(n):
        pid = str(uuid.uuid4())
        category = pick_product_category()
        record = {
            "product_id": pid,
            "category": category,
            "product_name": generate_product_name(category),
            "weight_g": round(random.uniform(100, 30000), 1),
            "length_cm": round(random.uniform(5, 100), 1),
            "height_cm": round(random.uniform(2, 80), 1),
            "width_cm": round(random.uniform(2, 80), 1),
        }
        # Inject schema error (string instead of float) for 2% of records (Realistic rate)
        if random.random() < 0.02:
            record["weight_g"] = "5 kg" # String instead of Float
        records.append(record)
        registry.register("products", pid)
    return records


# ---- 3. Sellers ----

def generate_sellers(n: int, registry: IDRegistry) -> list[dict[str, Any]]:
    """Generate n seller records with Vietnamese locations."""
    records = []
    for _ in range(n):
        sid = str(uuid.uuid4())
        location = pick_vietnam_location()
        records.append({
            "seller_id": sid,
            "seller_name": fake.company(),
            "city": location["city"],
            "state": location["state"],
        })
        registry.register("sellers", sid)
    return records


# ---- 4. Orders ----

def generate_orders(n: int, registry: IDRegistry) -> list[dict[str, Any]]:
    """Generate n order records referencing existing customer_ids."""
    records = []
    for _ in range(n):
        oid = str(uuid.uuid4())
        status = str(weighted_choice(ORDER_STATUS_OPTIONS, ORDER_STATUS_WEIGHTS))
        purchase_ts = _random_timestamp(_ORDER_START, _NOW)
        approved_ts = _timestamp_after(purchase_ts, min_hours=0, max_hours=24) if status != "cancelled" else None

        delivered_ts = None
        if status == "delivered":
            delivered_ts = _timestamp_after(purchase_ts, min_hours=48, max_hours=720)  # 2-30 days

        records.append({
            "order_id": oid,
            "customer_id": registry.get_random("customers"),
            "order_status": status,
            "purchase_timestamp": purchase_ts,
            "approved_timestamp": approved_ts,
            "delivered_timestamp": delivered_ts,
        })
        registry.register("orders", oid)
    return records


# ---- 5. Order Items ----

def generate_order_items(n: int, registry: IDRegistry) -> list[dict[str, Any]]:
    """
    Generate n order_item records referencing order_id, product_id, seller_id.
    Multiple items can belong to the same order.
    """
    records = []
    order_ids = registry.get_all("orders")

    for _ in range(n):
        price = log_normal_price(mean=10.5, sigma=0.8, min_val=20000, max_val=30000000)
        freight = round(price * random.uniform(0.03, 0.15) / 1000) * 1000  # freight 3-15%, rounded to 1000 VND

        # Introduce dirty data: 2% chance of negative price
        if random.random() < 0.02:
            price = -999.99

        records.append({
            "order_id": random.choice(order_ids),
            "product_id": registry.get_random("products"),
            "seller_id": registry.get_random("sellers"),
            "price": price,
            "freight_value": max(freight, 10000),  # min 10,000 VND shipping
        })
    return records


# ---- 6. Payments ----

def generate_payments(n: int, registry: IDRegistry) -> list[dict[str, Any]]:
    """Generate n payment records referencing order_id."""
    records = []
    order_ids = registry.get_all("orders")

    for _ in range(n):
        payment_type = str(weighted_choice(PAYMENT_TYPE_OPTIONS, PAYMENT_TYPE_WEIGHTS))
        payment_value = log_normal_price(mean=10.8, sigma=0.9, min_val=30000, max_val=50000000)

        installments = random_installments() if payment_type == "credit_card" else 1

        records.append({
            "payment_id": str(uuid.uuid4()),
            "order_id": random.choice(order_ids),
            "payment_type": payment_type,
            "installments": installments,
            "payment_value": payment_value,
        })
    return records


# ---- 7. Reviews ----

def generate_reviews(n: int, registry: IDRegistry) -> list[dict[str, Any]]:
    """Generate n review records referencing order_id, with skewed scores."""
    records = []
    order_ids = registry.get_all("orders")

    positive_comments = [
        "San pham rat tot, giao hang nhanh!",
        "Chat luong tuyet voi, se mua lai!",
        "Dong goi can than, san pham dung mo ta.",
        "Giao hang nhanh, san pham dep lam!",
        "Rat hai long, gia ca hop ly.",
        "Shop uy tin, san pham chinh hang.",
        "Dung nhu hinh, chat luong tot.",
    ]
    negative_comments = [
        "San pham bi loi, khong dung mo ta.",
        "Giao hang qua cham, doi gan 2 tuan.",
        "Chat luong kem, khong dang gia tien.",
        "Dong goi so sai, san pham bi mop.",
        "Shop khong ho tro doi tra.",
    ]
    neutral_comments = [
        "San pham tam on, khong co gi dac biet.",
        "Binh thuong, co the tot hon.",
        "Duoc, nhung giao hoi cham.",
    ]

    for _ in range(n):
        score = skewed_score()

        if score >= 4:
            comment = random.choice(positive_comments) if random.random() < 0.7 else None
        elif score >= 3:
            comment = random.choice(neutral_comments) if random.random() < 0.5 else None
        else:
            comment = random.choice(negative_comments) if random.random() < 0.8 else None

        review_ts = _random_timestamp(_ORDER_START + timedelta(days=7), _NOW)

        records.append({
            "review_id": str(uuid.uuid4()),
            "order_id": random.choice(order_ids),
            "review_score": score,
            "comment": comment,
            "review_timestamp": review_ts,
        })
    return records


# ---- 8. Shipments (with CDC logic) ----

def generate_shipments(n: int, registry: IDRegistry) -> list[dict[str, Any]]:
    """
    Generate shipment records with CDC (Change Data Capture) logic.

    ~70% are new shipments (status = 'pending').
    ~30% are updates on existing shipments (status transitions).
    This mimics a real event stream where shipment statuses change over time.
    """
    records = []
    n_updates = int(n * CDC_UPDATE_RATIO)
    n_new = n - n_updates

    # --- New shipments ---
    order_ids = registry.get_all("orders")
    for _ in range(n_new):
        shipment_id = str(uuid.uuid4())
        order_id = random.choice(order_ids)
        shipped_date = _random_timestamp(_ORDER_START, _NOW)
        estimated_delivery = _timestamp_after(shipped_date, min_hours=72, max_hours=480)  # 3-20 days

        record = {
            "shipment_id": shipment_id,
            "order_id": order_id,
            "carrier": random.choice(CARRIERS),
            "tracking_number": f"VN{random.randint(100000000, 999999999)}",
            "shipping_status": "pending",
            "shipped_date": shipped_date,
            "estimated_delivery_date": estimated_delivery,
            "actual_delivery_date": None,
            "event_timestamp": shipped_date,
        }
        records.append(record)
        registry.register("shipments", shipment_id)
        registry.register_shipment(shipment_id, order_id, "pending")

    # --- CDC updates on existing shipments ---
    updatable = registry.get_updatable_shipments()
    if updatable:
        updates_to_make = min(n_updates, len(updatable))
        selected = random.sample(updatable, updates_to_make)

        for shipment_id, current_status in selected:
            new_status = SHIPPING_STATUS_TRANSITION[current_status]
            if new_status is None:
                continue  # already delivered, skip

            event_ts = _random_timestamp(
                datetime.fromisoformat(_random_timestamp(_ORDER_START + timedelta(days=30), _NOW)),
                _NOW,
            )

            actual_delivery = None
            if new_status == "delivered":
                actual_delivery = event_ts

            record = {
                "shipment_id": shipment_id,
                "order_id": None,  # not changing, but included for schema consistency
                "carrier": None,
                "tracking_number": None,
                "shipping_status": new_status,
                "shipped_date": None,
                "estimated_delivery_date": None,
                "actual_delivery_date": actual_delivery,
                "event_timestamp": event_ts,
            }
            records.append(record)
            registry.update_shipment_status(shipment_id, new_status)

    return records


# ===========================================================================
# Orchestrator
# ===========================================================================

def generate_all(counts: dict[str, int] | None = None) -> dict[str, list[dict[str, Any]]]:
    """
    Generate all 8 entities in FK-dependency order.

    Args:
        counts: Optional override for record counts per entity.
                Defaults to RECORD_COUNTS from config.

    Returns:
        Dict mapping entity name -> list of records.
    """
    counts = counts or RECORD_COUNTS
    registry = IDRegistry()

    data = {}

    # Order matters: independent entities first, then dependents
    generation_order = [
        ("customers", generate_customers),
        ("products", generate_products),
        ("sellers", generate_sellers),
        ("orders", generate_orders),
        ("order_items", generate_order_items),
        ("payments", generate_payments),
        ("reviews", generate_reviews),
        ("shipments", generate_shipments),
    ]

    for entity_name, generator_fn in generation_order:
        n = counts.get(entity_name, 100)
        data[entity_name] = generator_fn(n, registry)

    return data
