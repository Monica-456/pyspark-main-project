"""Reproducible synthetic order and product data generation."""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def generate_data(
    output_dir: str | Path, order_count: int = 100_000, seed: int = 42
) -> tuple[Path, Path, Path]:
    """Write orders.csv, products.csv, and customers.csv with deliberate edge cases.

    `order_count` counts valid order rows. The generated file also contains one duplicate,
    one invalid quantity, one unknown product, and one unknown customer.
    """
    if order_count < 1:
        raise ValueError("order_count must be at least 1")

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    orders_path = out / "orders.csv"
    products_path = out / "products.csv"
    customers_path = out / "customers.csv"
    rng = random.Random(seed)
    products = [(f"P{i:04d}", ["books", "home", "electronics", "clothing"][i % 4]) for i in range(1, 101)]
    base_time = datetime(2025, 1, 1, tzinfo=timezone.utc)

    with products_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "category"])
        writer.writerows(products)

    with customers_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id"])
        writer.writerows((f"C{i:05d}",) for i in range(1, 10_001))

    with orders_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "product_id", "quantity", "unit_price", "event_timestamp", "source_file"])
        for i in range(1, order_count + 1):
            product_id, _ = rng.choice(products)
            timestamp = base_time + timedelta(minutes=rng.randrange(0, 525_600))
            writer.writerow([
                f"O{i:09d}", f"C{rng.randrange(1, 10_001):05d}", product_id,
                rng.choice([1, 2, 3, 4, 5]), f"{rng.uniform(1, 500):.2f}",
                timestamp.isoformat().replace("+00:00", "Z"), "orders.csv",
            ])
        # Deliberate negative cases with stable, documented identifiers.
        if order_count:
            writer.writerow([f"O{1:09d}", "C00001", products[0][0], "1", "10.00", "2025-01-01T00:00:00Z", "orders.csv"])  # duplicate key
        writer.writerow([f"BAD{seed}", "C00002", products[1][0], "-2", "12.00", "2025-01-02T00:00:00Z", "orders.csv"])  # invalid quantity
        writer.writerow([f"REF{seed}", "C00003", "UNKNOWN", "1", "12.00", "2025-01-03T00:00:00Z", "orders.csv"])  # invalid reference
        writer.writerow([f"CUST{seed}", "UNKNOWN", products[2][0], "1", "12.00", "2025-01-04T00:00:00Z", "orders.csv"])  # invalid customer
    return orders_path, products_path, customers_path
