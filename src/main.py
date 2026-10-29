"""Generate inputs, benchmark both join strategies, and publish validated outputs."""

from __future__ import annotations

import argparse
import json
import logging
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
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
    ]
)
PRODUCTS_SCHEMA = StructType(
    [StructField("product_id", StringType(), True), StructField("category", StringType(), True)]
)
CUSTOMERS_SCHEMA = StructType([StructField("customer_id", StringType(), True)])


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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=100_000, help="number of valid synthetic orders")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--output-dir", default="data/output")
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
    try:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        orders_path, products_path, customers_path = generate_data(
            args.data_dir, args.records, args.seed
        )
        spark = (
            SparkSession.builder.appName("pyspark-order-pipeline")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        orders = (
            spark.read.schema(ORDERS_SCHEMA)
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .csv(str(orders_path))
            .cache()
        )
        products = (
            spark.read.schema(PRODUCTS_SCHEMA)
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .csv(str(products_path))
            .cache()
        )
        customers = (
            spark.read.schema(CUSTOMERS_SCHEMA)
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .csv(str(customers_path))
            .cache()
        )
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
        final_result = run_pipeline(
            orders,
            products,
            customers=customers,
            optimized=True,
            output_path=str(output_dir / "orders"),
            reject_path=str(output_dir / "quarantine"),
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
            final_baseline["rejected"], final_result["rejected"], "published quarantine"
        )

        report = {
            "run_id": run_id,
            "generated_at_utc": final_result["ended_at"],
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
                "orders": str(output_dir / "orders"),
                "quarantine": str(output_dir / "quarantine"),
            },
        }
        report_path = output_dir / "reconciliation.json"
        _write_report(report_path, report)
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
        for frame in (orders, products, customers):
            if frame is not None:
                frame.unpersist()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
