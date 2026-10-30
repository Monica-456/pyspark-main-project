"""Generate inputs, benchmark both join strategies, and publish validated outputs."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.generator import generate_data
from src.pipeline import reconciliation, run_pipeline


LOGGER = logging.getLogger("order_pipeline")
ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)
PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)
CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)
ORDERS_HEADER = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "event_timestamp",
    "source_file",
]
PRODUCTS_HEADER = ["product_id", "category"]
CUSTOMERS_HEADER = ["customer_id"]


def _assert_same_rows(left: DataFrame, right: DataFrame, name: str) -> None:
    if left.exceptAll(right).limit(1).count() or right.exceptAll(left).limit(1).count():
        raise RuntimeError(f"Baseline and optimized {name} outputs differ")


def _benchmark(
    orders: DataFrame,
    products: DataFrame,
    customers: DataFrame,
    repeats: int,
    warmups: int,
) -> tuple[dict[str, list[float]], dict[str, dict]]:
    samples: dict[str, list[float]] = {"baseline": [], "optimized": []}
    latest: dict[str, dict] = {}
    total_runs = warmups + repeats
    for iteration in range(total_runs):
        run_id = f"benchmark-{iteration}"
        modes = [("baseline", False), ("optimized", True)]
        if iteration % 2:
            modes.reverse()
        timed: dict[str, float] = {}
        run_results = {}
        for label, optimized in modes:
            started = time.perf_counter()
            result = run_pipeline(
                orders,
                products,
                customers=customers,
                optimized=optimized,
                run_id=run_id,
            )
            timed[label] = time.perf_counter() - started
            run_results[label] = result
        _assert_same_rows(
            run_results["baseline"]["accepted"].drop("run_id"),
            run_results["optimized"]["accepted"].drop("run_id"),
            "accepted",
        )
        _assert_same_rows(
            run_results["baseline"]["rejected"],
            run_results["optimized"]["rejected"],
            "quarantine",
        )
        if iteration >= warmups:
            for label in samples:
                samples[label].append(timed[label])
            latest = run_results
            LOGGER.info(
                "Benchmark iteration %d/%d: baseline=%.3fs optimized=%.3fs",
                iteration - warmups + 1,
                repeats,
                timed["baseline"],
                timed["optimized"],
            )
    return samples, latest


def _write_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, suffix=".tmp", delete=False
        ) as temporary:
            temporary_path = temporary.name
            json.dump(report, temporary, indent=2, default=str)
            temporary.write("\n")
        os.replace(temporary_path, path)
    finally:
        if temporary_path and os.path.exists(temporary_path):
            os.unlink(temporary_path)


def _read_csv(
    spark: SparkSession,
    path: Path,
    schema: StructType,
    expected_header: list[str],
) -> DataFrame:
    with path.open(newline="", encoding="utf-8-sig") as file:
        try:
            header = next(csv.reader(file))
        except StopIteration as error:
            raise ValueError(f"CSV file is empty: {path}") from error
    if len(header) != len(set(header)):
        raise ValueError(f"Invalid CSV header in {path}: duplicate header names")
    if header != expected_header:
        raise ValueError(
            f"Invalid CSV header in {path}; expected {expected_header}, got {header}"
        )
    return (
        spark.read.schema(schema)
        .option("header", True)
        .option("enforceSchema", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(str(path))
        .withColumn("_source_path", F.input_file_name())
    )


def _publish_generation(
    output_dir: Path,
    staging_dir: Path,
    run_id: str,
    report: dict,
) -> Path:
    runs_dir = output_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    generation_dir = runs_dir / run_id
    if generation_dir.exists():
        raise FileExistsError(f"Run generation already exists: {generation_dir}")

    report_path = staging_dir / "reconciliation.json"
    _write_report(report_path, report)
    staging_dir.replace(generation_dir)
    _write_report(
        output_dir / "active-run.json",
        {
            "status": "SUCCESS",
            "run_id": run_id,
            "generation": str(generation_dir),
            "report": str(generation_dir / "reconciliation.json"),
        },
    )
    return generation_dir


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=100_000, help="number of valid synthetic orders")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--orders-path", default=None)
    parser.add_argument("--products-path", default=None)
    parser.add_argument("--customers-path", default=None)
    parser.add_argument(
        "--generate-demo",
        action="store_true",
        help="explicitly generate demo input files before processing",
    )
    parser.add_argument("--output-dir", default="data/output")
    parser.add_argument("--timezone", default="UTC", help="Spark reporting timezone")
    parser.add_argument("--benchmark-repeats", type=int, default=3)
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--output-partitions", type=int, default=8)
    args = parser.parse_args()
    if args.records < 1:
        parser.error("--records must be at least 1")
    if args.benchmark_repeats < 1:
        parser.error("--benchmark-repeats must be at least 1")
    if args.warmup_runs < 0:
        parser.error("--warmup-runs cannot be negative")
    if args.shuffle_partitions < 1 or args.output_partitions < 1:
        parser.error("partition counts must be at least 1")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    spark = None
    orders = products = customers = None
    staging_dir: Path | None = None
    try:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        if args.generate_demo:
            orders_path, products_path, customers_path = generate_data(
                args.data_dir, args.records, args.seed
            )
        else:
            data_dir = Path(args.data_dir)
            orders_path = Path(args.orders_path) if args.orders_path else data_dir / "orders.csv"
            products_path = (
                Path(args.products_path) if args.products_path else data_dir / "products.csv"
            )
            customers_path = (
                Path(args.customers_path)
                if args.customers_path
                else data_dir / "customers.csv"
            )
        spark = (
            SparkSession.builder.appName("pyspark-order-pipeline")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
            .config("spark.sql.session.timeZone", args.timezone)
            .getOrCreate()
        )
        spark.conf.set("spark.sql.session.timeZone", args.timezone)
        spark.sparkContext.setLogLevel("WARN")
        orders = _read_csv(spark, orders_path, ORDERS_SCHEMA, ORDERS_HEADER).cache()
        products = _read_csv(spark, products_path, PRODUCTS_SCHEMA, PRODUCTS_HEADER)
        customers = _read_csv(spark, customers_path, CUSTOMERS_SCHEMA, CUSTOMERS_HEADER)
        for name, frame in (("products", products), ("customers", customers)):
            if frame.filter(F.col("_corrupt_record").isNotNull()).select(
                *frame.columns
            ).limit(1).count():
                raise ValueError(f"{name} CSV contains a malformed structural record")
        products = products.drop("_corrupt_record", "_source_path").cache()
        customers = customers.drop("_corrupt_record", "_source_path").cache()
        input_count = orders.count()
        product_count = products.count()
        customer_count = customers.count()
        LOGGER.info(
            "Loaded %d order rows, %d products, and %d customers; benchmark inputs are cached",
            input_count,
            product_count,
            customer_count,
        )

        samples, benchmark_results = _benchmark(
            orders, products, customers, args.benchmark_repeats, args.warmup_runs
        )
        baseline_median = statistics.median(samples["baseline"])
        optimized_median = statistics.median(samples["optimized"])

        output_dir = Path(args.output_dir)
        run_id = str(uuid4())
        staging_dir = output_dir / "runs" / f".staging-{run_id}"
        staging_dir.mkdir(parents=True, exist_ok=False)
        final_result = run_pipeline(
            orders,
            products,
            customers=customers,
            optimized=True,
            output_path=str(staging_dir / "orders"),
            reject_path=str(staging_dir / "quarantine"),
            run_id=run_id,
            output_partitions=args.output_partitions,
        )
        recon = reconciliation(final_result).first().asDict()
        final_baseline = benchmark_results["baseline"]
        _assert_same_rows(
            final_baseline["accepted"].drop("run_id"),
            final_result["accepted"].drop("run_id"),
            "published accepted",
        )
        _assert_same_rows(
            final_baseline["rejected"].drop("run_id"),
            final_result["rejected"].drop("run_id"),
            "published quarantine",
        )

        report = {
            "run_id": run_id,
            "status": "SUCCESS",
            "generated_at_utc": final_result["publish_ended_at"],
            "timezone": args.timezone,
            "timings": {
                "started_at": final_result["started_at"],
                "transform_ended_at": final_result["transform_ended_at"],
                "publish_ended_at": final_result["publish_ended_at"],
                "transform_seconds": final_result["transform_elapsed_seconds"],
                "pipeline_seconds_including_dataset_publish": final_result["elapsed_seconds"],
            },
            "input_files": {
                "orders": str(orders_path),
                "products": str(products_path),
                "customers": str(customers_path),
            },
            "counts": {
                "input": final_result["input_count"],
                "accepted": final_result["output_count"],
                "rejected": final_result["rejected_count"],
                "duplicate_rows_removed": final_result["duplicate_rows_removed"],
            },
            "reconciliation": recon,
            "rejections_by_reason": [
                row.asDict() for row in final_result["rejection_summary"].collect()
            ],
            "sales_by_date_and_category": [
                row.asDict() for row in final_result["sales_summary"].collect()
            ],
            "benchmark": {
                "scope": "pipeline transformations and count actions; cached input, excluding CSV reads and output publishing",
                "warmup_runs_per_mode": args.warmup_runs,
                "measured_repeats_per_mode": args.benchmark_repeats,
                "baseline_seconds": samples["baseline"],
                "optimized_seconds": samples["optimized"],
                "baseline_median_seconds": baseline_median,
                "optimized_median_seconds": optimized_median,
                "median_speedup": baseline_median / optimized_median
                if optimized_median
                else None,
                "accepted_and_quarantine_outputs_equal": True,
            },
            "outputs": {
                "orders": str(output_dir / "runs" / run_id / "orders"),
                "quarantine": str(output_dir / "runs" / run_id / "quarantine"),
                "duplicates": str(output_dir / "runs" / run_id / "duplicates"),
            },
        }
        final_result["duplicates"].write.mode("errorifexists").parquet(
            str(staging_dir / "duplicates")
        )
        for name, result_frame in (
            ("orders", final_result["accepted"]),
            ("quarantine", final_result["rejected"]),
            ("duplicates", final_result["duplicates"]),
        ):
            persisted = spark.read.parquet(str(staging_dir / name)).select(
                *result_frame.columns
            )
            if persisted.count() != result_frame.count():
                raise RuntimeError(f"Persisted {name} row count does not match the run result")
            _assert_same_rows(result_frame, persisted, f"persisted {name}")
        generation_dir = _publish_generation(output_dir, staging_dir, run_id, report)
        staging_dir = None
        report_path = generation_dir / "reconciliation.json"
        LOGGER.info(
            "Run %s complete: accepted=%d rejected=%d duplicates_removed=%d; "
            "median baseline=%.3fs optimized=%.3fs; report=%s",
            run_id,
            final_result["output_count"],
            final_result["rejected_count"],
            final_result["duplicate_rows_removed"],
            baseline_median,
            optimized_median,
            report_path,
        )
    except Exception:
        LOGGER.exception("Order pipeline execution failed")
        raise SystemExit(1) from None
    finally:
        if staging_dir is not None and staging_dir.exists():
            shutil.rmtree(staging_dir)
        for frame in (orders, products, customers):
            if frame is not None:
                frame.unpersist()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
