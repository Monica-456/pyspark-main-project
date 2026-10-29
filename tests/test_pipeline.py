import csv
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from src.generator import generate_data
from src import pipeline
from src.pipeline import reconciliation, run_pipeline


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pipeline-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def inputs(spark):
    schema = (
        "order_id string, customer_id string, product_id string, quantity string, "
        "unit_price string, event_timestamp string, source_file string"
    )
    rows = [
        ("O1", "C1", "P1", "2", "10.50", "2025-01-01T10:00:00Z", "a.csv"),
        ("O1", "C1", "P1", "2", "10.50", "2025-01-01T10:00:00Z", "a.csv"),
        ("O2", "C2", "P2", "-1", "8.00", "2025-01-01T11:00:00Z", "a.csv"),
        ("O3", "C3", "MISSING", "1", "5.00", "2025-01-01T12:00:00Z", "b.csv"),
        ("O4", "C4", "P1", "bad", "5.00", "2025-01-01T13:00:00Z", "b.csv"),
        ("O5", "C5", "P2", "1", "3.25", "2025-01-02T13:00:00Z", "b.csv"),
    ]
    products = spark.createDataFrame(
        [("P1", "books"), ("P2", "home")], "product_id string, category string"
    )
    customers = spark.createDataFrame(
        [(f"C{i}",) for i in range(1, 6)], "customer_id string"
    )
    return spark.createDataFrame(rows, schema), products, customers


def test_validation_dedup_and_reference_quarantine(inputs):
    orders, products, customers = inputs
    result = run_pipeline(orders, products, customers=customers, run_id="test-run")
    assert result["input_count"] == 6
    assert result["output_count"] == 2
    assert result["rejected_count"] == 3
    assert result["duplicate_rows_removed"] == 1
    assert {row.reject_reason for row in result["rejected"].select("reject_reason").collect()} == {
        "non_positive_quantity",
        "unknown_product_reference",
        "invalid_quantity",
    }
    revenue = {
        row.order_id: row.revenue
        for row in result["accepted"].select("order_id", "revenue").collect()
    }
    assert revenue == {"O1": Decimal("21.00"), "O5": Decimal("3.25")}
    rejection_counts = {
        row.reject_reason: row["count"] for row in result["rejection_summary"].collect()
    }
    assert rejection_counts == {
        "invalid_quantity": 1,
        "non_positive_quantity": 1,
        "unknown_product_reference": 1,
    }
    sales = {
        (row.order_date.isoformat(), row.category): (
            row.order_count,
            row.units_sold,
            row.gross_revenue,
        )
        for row in result["sales_summary"].collect()
    }
    assert sales == {
        ("2025-01-01", "books"): (1, 2, Decimal("21.00")),
        ("2025-01-02", "home"): (1, 1, Decimal("3.25")),
    }


def test_unknown_customer_is_quarantined(inputs, spark):
    orders, products, customers = inputs
    unknown = spark.createDataFrame(
        [("O6", "NOT_REGISTERED", "P1", "1", "4.00", "2025-01-03T00:00:00Z", "x.csv")],
        orders.schema,
    )
    result = run_pipeline(orders.unionByName(unknown), products, customers=customers)
    assert result["output_count"] == 2
    assert result["rejected"].filter(
        "reject_reason = 'unknown_customer_reference'"
    ).select("order_id").first().order_id == "O6"
    assert result["input_count"] == (
        result["output_count"]
        + result["rejected_count"]
        + result["duplicate_rows_removed"]
    )


def test_optimized_matches_baseline(inputs):
    orders, products, customers = inputs
    baseline = run_pipeline(orders, products, customers=customers, optimized=False)
    optimized = run_pipeline(orders, products, customers=customers, optimized=True)
    assert sorted(baseline["accepted"].drop("run_id").collect()) == sorted(
        optimized["accepted"].drop("run_id").collect()
    )
    assert sorted(baseline["rejected"].collect()) == sorted(optimized["rejected"].collect())


def test_empty_input(inputs):
    orders, products, customers = inputs
    result = run_pipeline(orders.limit(0), products, customers=customers)
    assert (result["input_count"], result["output_count"], result["rejected_count"]) == (0, 0, 0)
    assert result["duplicate_rows_removed"] == 0
    assert result["sales_summary"].count() == 0
    assert result["rejection_summary"].count() == 0


def test_reconciliation_accounts_for_duplicates_and_rejections(inputs):
    orders, products, customers = inputs
    result = run_pipeline(orders, products, customers=customers)
    row = reconciliation(result).first()
    assert row.unaccounted_count == 0
    assert row.duplicate_rows_removed == 1


@pytest.mark.parametrize(
    ("order_id", "customer_id", "product_id", "quantity", "price", "timestamp", "reason"),
    [
        ("", "C1", "P1", "1", "1.00", "2025-01-01T00:00:00Z", "missing_order_id"),
        ("O2", "", "P1", "1", "1.00", "2025-01-01T00:00:00Z", "missing_customer_id"),
        ("O3", "C1", "", "1", "1.00", "2025-01-01T00:00:00Z", "missing_product_id"),
        ("O4", "C1", "P1", "1.5", "1.00", "2025-01-01T00:00:00Z", "invalid_quantity"),
        ("O5", "C1", "P1", "0", "1.00", "2025-01-01T00:00:00Z", "non_positive_quantity"),
        ("O6", "C1", "P1", "1", "1.234", "2025-01-01T00:00:00Z", "invalid_unit_price"),
        ("O7", "C1", "P1", "1", "bad", "2025-01-01T00:00:00Z", "invalid_unit_price"),
        ("O8", "C1", "P1", "1", "-1.00", "2025-01-01T00:00:00Z", "negative_unit_price"),
        ("O9", "C1", "P1", "1", "1.00", "bad timestamp", "invalid_event_timestamp"),
    ],
)
def test_malformed_and_boundary_order_rows_are_quarantined(
    spark, order_id, customer_id, product_id, quantity, price, timestamp, reason
):
    schema = (
        "order_id string, customer_id string, product_id string, quantity string, "
        "unit_price string, event_timestamp string, source_file string"
    )
    orders = spark.createDataFrame(
        [(order_id, customer_id, product_id, quantity, price, timestamp, "x.csv")], schema
    )
    products = spark.createDataFrame([("P1", "books")], "product_id string, category string")
    customers = spark.createDataFrame([("C1",)], "customer_id string")
    result = run_pipeline(orders, products, customers=customers)
    assert result["output_count"] == 0
    assert result["rejected"].first().reject_reason == reason


def test_unit_price_documented_upper_boundary(spark):
    schema = (
        "order_id string, customer_id string, product_id string, quantity string, "
        "unit_price string, event_timestamp string, source_file string"
    )
    orders = spark.createDataFrame(
        [
            ("MAX", "C1", "P1", "1", "9999999999.99", "2025-01-01T00:00:00Z", "x.csv"),
            ("OVER", "C1", "P1", "1", "10000000000.00", "2025-01-01T00:00:00Z", "x.csv"),
        ],
        schema,
    )
    products = spark.createDataFrame([("P1", "books")], "product_id string, category string")
    customers = spark.createDataFrame([("C1",)], "customer_id string")

    result = run_pipeline(orders, products, customers=customers)

    accepted = result["accepted"].select("order_id", "revenue").first()
    rejected = result["rejected"].select("order_id", "reject_reason").first()
    assert (accepted.order_id, accepted.revenue) == ("MAX", Decimal("9999999999.99"))
    assert (rejected.order_id, rejected.reject_reason) == ("OVER", "invalid_unit_price")


def test_missing_order_schema_fails_before_processing(inputs, spark):
    _, products, customers = inputs
    malformed = spark.createDataFrame([("O1",)], "order_id string")
    with pytest.raises(ValueError, match="Invalid orders schema.*missing columns"):
        run_pipeline(malformed, products, customers=customers)


def test_missing_dimension_columns_fail_before_processing(spark):
    orders = spark.createDataFrame(
        [],
        "order_id string, customer_id string, product_id string, quantity string, "
        "unit_price string, event_timestamp string, source_file string",
    )
    products_without_category = spark.createDataFrame([("P1",)], "product_id string")
    customers = spark.createDataFrame([("C1",)], "customer_id string")
    with pytest.raises(ValueError, match="Invalid products schema.*missing columns: category"):
        run_pipeline(orders, products_without_category, customers=customers)

    products = spark.createDataFrame([("P1", "books")], "product_id string, category string")
    customers_without_key = spark.createDataFrame([("extra",)], "other_id string")
    with pytest.raises(ValueError, match="Invalid customers schema.*missing columns: customer_id"):
        run_pipeline(orders, products, customers=customers_without_key)


@pytest.mark.parametrize(
    ("product_rows", "customer_rows", "message"),
    [
        ([("P1", "books"), ("P1", "home")], [("C1",)], "duplicate product_id"),
        ([("P1", "books")], [("C1",), ("C1",)], "duplicate customer_id"),
        ([("", "books")], [("C1",)], "null or blank product_id"),
        ([("P1", " ")], [("C1",)], "null or blank category"),
        ([("P1", "books")], [(" ",)], "null or blank customer_id"),
    ],
)
def test_invalid_dimension_integrity_fails_fast(
    spark, product_rows, customer_rows, message
):
    orders = spark.createDataFrame(
        [],
        "order_id string, customer_id string, product_id string, quantity string, "
        "unit_price string, event_timestamp string, source_file string",
    )
    products = spark.createDataFrame(product_rows, "product_id string, category string")
    customers = spark.createDataFrame(customer_rows, "customer_id string")
    with pytest.raises(ValueError, match=message):
        run_pipeline(orders, products, customers=customers)


def test_duplicate_winner_is_stable_when_input_order_changes(spark):
    schema = (
        "order_id string, customer_id string, product_id string, quantity string, "
        "unit_price string, event_timestamp string, source_file string"
    )
    first = ("O1", "C2", "P1", "1", "2.00", "2025-01-01T00:00:00Z", "x.csv")
    second = ("O1", "C1", "P1", "3", "4.00", "2025-01-01T00:00:00Z", "x.csv")
    products = spark.createDataFrame([("P1", "books")], "product_id string, category string")
    customers = spark.createDataFrame([("C1",), ("C2",)], "customer_id string")
    forward = spark.createDataFrame([first, second], schema)
    reverse = spark.createDataFrame([second, first], schema)
    first_result = run_pipeline(forward, products, customers=customers, run_id="stable")
    second_result = run_pipeline(reverse, products, customers=customers, run_id="stable")
    assert first_result["accepted"].collect() == second_result["accepted"].collect()
    assert first_result["accepted"].first().customer_id == "C1"
    assert first_result["accepted"].first().revenue == Decimal("12.00")


def test_output_publish_overwrites_previous_successful_run(inputs, tmp_path, spark):
    orders, products, customers = inputs
    target = tmp_path / "orders"
    rejects = tmp_path / "quarantine"
    run_pipeline(
        orders,
        products,
        customers=customers,
        output_path=str(target),
        reject_path=str(rejects),
        run_id="repeat",
    )
    assert spark.read.parquet(str(target)).count() == 2
    assert spark.read.parquet(str(rejects)).count() == 3
    run_pipeline(
        orders.limit(1),
        products,
        customers=customers,
        output_path=str(target),
        reject_path=str(rejects),
        run_id="second",
    )
    assert spark.read.parquet(str(target)).count() == 1
    assert spark.read.parquet(str(rejects)).count() == 0


def test_partial_publish_failure_restores_outputs_and_retry_cleans_staging(
    inputs, tmp_path, spark, monkeypatch
):
    orders, products, customers = inputs
    target = tmp_path / "orders"
    rejects = tmp_path / "quarantine"
    run_pipeline(
        orders,
        products,
        customers=customers,
        output_path=str(target),
        reject_path=str(rejects),
        run_id="before-failure",
    )

    original_rename = pipeline._rename_path

    def fail_quarantine_promotion(filesystem, source, destination):
        if ".quarantine.staging-" in source.toString():
            return False
        return original_rename(filesystem, source, destination)

    monkeypatch.setattr(pipeline, "_rename_path", fail_quarantine_promotion)
    with pytest.raises(OSError, match="Could not promote staged output"):
        run_pipeline(
            orders.limit(0),
            products,
            customers=customers,
            output_path=str(target),
            reject_path=str(rejects),
            run_id="injected-promotion-failure",
        )

    assert spark.read.parquet(str(target)).count() == 2
    assert spark.read.parquet(str(rejects)).count() == 3
    assert not list(tmp_path.glob(".*.staging-*"))
    assert not list(tmp_path.glob(".*.backup-*"))

    monkeypatch.setattr(pipeline, "_rename_path", original_rename)
    result = run_pipeline(
        orders,
        products,
        customers=customers,
        output_path=str(target),
        reject_path=str(rejects),
        run_id="retry-after-failure",
    )
    assert result["output_count"] == 2
    assert result["rejected_count"] == 3
    assert spark.read.parquet(str(target)).count() == 2
    assert spark.read.parquet(str(rejects)).count() == 3
    assert not list(tmp_path.glob(".*.staging-*"))
    assert not list(tmp_path.glob(".*.backup-*"))


def test_generator_creates_customer_dimension(tmp_path):
    orders_path, products_path, customers_path = generate_data(tmp_path, order_count=2, seed=7)
    assert orders_path.exists()
    assert products_path.exists()
    with customers_path.open(newline="", encoding="utf-8") as file:
        customers = list(csv.DictReader(file))
    assert len(customers) == 10_000
    assert customers[0] == {"customer_id": "C00001"}
    assert customers[-1] == {"customer_id": "C10000"}


def test_generator_is_reproducible_for_fixed_seed(tmp_path):
    first_paths = generate_data(tmp_path / "first", order_count=100_000, seed=123)
    second_paths = generate_data(tmp_path / "second", order_count=100_000, seed=123)
    assert [path.read_bytes() for path in first_paths] == [
        path.read_bytes() for path in second_paths
    ]
