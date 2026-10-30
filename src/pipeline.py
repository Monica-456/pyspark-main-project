"""Validation, deterministic transformations, and recoverable local publishing."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Sequence
from uuid import uuid4

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


LOGGER = logging.getLogger(__name__)
ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "event_timestamp",
    "source_file",
]
ORDER_INPUT_COLUMNS = ORDER_COLUMNS
PRODUCT_INPUT_COLUMNS = ["product_id", "category"]
CUSTOMER_INPUT_COLUMNS = ["customer_id"]
_PRICE_PATTERN = r"^[+-]?(?:\d+(?:\.\d{1,2})?|\.\d{1,2})$"


def _rename_path(filesystem: Any, source: Any, target: Any) -> bool:
    """Rename a Hadoop path; isolated so publish recovery can be fault-tested."""
    return filesystem.rename(source, target)


def _validate_columns(frame: DataFrame, required: Sequence[str], name: str) -> None:
    columns = frame.columns
    missing = sorted(set(required) - set(columns))
    duplicates = sorted({column for column in columns if columns.count(column) > 1})
    if missing or duplicates:
        details = []
        if missing:
            details.append(f"missing columns: {', '.join(missing)}")
        if duplicates:
            details.append(f"duplicate columns: {', '.join(duplicates)}")
        raise ValueError(f"Invalid {name} schema ({'; '.join(details)})")


def _validate_dimension(
    frame: DataFrame,
    required: Sequence[str],
    key: str,
    name: str,
) -> DataFrame:
    _validate_columns(frame, required, name)
    clean = frame.select(
        *[F.trim(F.col(column).cast("string")).alias(column) for column in required]
    )
    for column in required:
        if clean.filter(F.col(column).isNull() | (F.col(column) == "")).limit(1).count():
            raise ValueError(f"{name} contains a null or blank {column}")
    if clean.groupBy(key).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"{name} contains duplicate {key} values")
    return clean


def _clean_orders(
    raw_orders: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Apply the raw order contract and split malformed rows from valid rows."""
    _validate_columns(raw_orders, ORDER_INPUT_COLUMNS, "orders")
    raw_record = F.to_json(F.struct(*[F.col(column) for column in raw_orders.columns]))
    payload_record = F.to_json(F.struct(*[F.col(column) for column in ORDER_COLUMNS]))
    source_path = (
        F.col("_source_path").cast("string")
        if "_source_path" in raw_orders.columns
        else F.lit(None).cast("string")
    )
    identified = raw_orders.withColumn("_raw_record", raw_record).withColumn(
        "_source_path", source_path
    )
    identity_window = Window.partitionBy("_raw_record").orderBy(F.lit(1))
    identified = identified.withColumn(
        "_source_record_id",
        F.concat(
            F.sha2(F.col("_raw_record"), 256),
            F.lit("-"),
            F.row_number().over(identity_window).cast("string"),
        ),
    )
    quantity_text = F.trim(F.col("quantity").cast("string"))
    price_text = F.trim(F.col("unit_price").cast("string"))
    typed = identified.select(
        F.trim(F.col("order_id").cast("string")).alias("order_id"),
        F.trim(F.col("customer_id").cast("string")).alias("customer_id"),
        F.trim(F.col("product_id").cast("string")).alias("product_id"),
        F.expr("try_cast(trim(cast(quantity as string)) as int)").alias("quantity"),
        F.when(
            price_text.rlike(_PRICE_PATTERN),
            F.expr("try_cast(trim(cast(unit_price as string)) as decimal(12, 2))"),
        )
        .otherwise(F.lit(None).cast(DecimalType(12, 2)))
        .alias("unit_price"),
        F.expr(
            "try_to_timestamp(trim(cast(event_timestamp as string)))"
        ).alias("event_timestamp"),
        F.coalesce(
            F.trim(F.col("source_file").cast("string")),
            F.lit("unknown"),
        ).alias("source_file"),
        F.col("_source_record_id").alias("source_record_id"),
        F.col("_source_path").alias("source_path"),
        F.col("_raw_record").alias("raw_record"),
        payload_record.alias("_payload_record"),
        (
            F.col("_corrupt_record").cast("string")
            if "_corrupt_record" in raw_orders.columns
            else F.lit(None).cast("string")
        ).alias("_corrupt_record"),
        quantity_text.alias("_raw_quantity"),
        price_text.alias("_raw_unit_price"),
        F.trim(F.col("event_timestamp").cast("string")).alias("_raw_event_timestamp"),
    )
    reason = (
        F.when(F.col("_corrupt_record").isNotNull(), "malformed_csv_record")
        .when(F.col("order_id").isNull() | (F.col("order_id") == ""), "missing_order_id")
        .when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "missing_customer_id")
        .when(F.col("product_id").isNull() | (F.col("product_id") == ""), "missing_product_id")
        .when(F.col("_raw_quantity").isNull() | (F.col("_raw_quantity") == ""), "invalid_quantity")
        .when(F.col("quantity").isNull(), "invalid_quantity")
        .when(F.col("quantity") <= 0, "non_positive_quantity")
        .when(F.col("_raw_unit_price").isNull() | (F.col("_raw_unit_price") == ""), "invalid_unit_price")
        .when(F.col("unit_price").isNull(), "invalid_unit_price")
        .when(F.col("unit_price") < 0, "negative_unit_price")
        .when(
            F.col("_raw_event_timestamp").isNull() | (F.col("_raw_event_timestamp") == ""),
            "invalid_event_timestamp",
        )
        .when(F.col("event_timestamp").isNull(), "invalid_event_timestamp")
    )
    assessed = typed.withColumn("reject_reason", reason)
    rejected = assessed.filter(F.col("reject_reason").isNotNull()).select(
        *[F.col(column) for column in ORDER_COLUMNS],
        "source_record_id",
        "source_path",
        "raw_record",
        "reject_reason",
    )
    valid = assessed.filter(F.col("reject_reason").isNull()).drop(
        "reject_reason",
        "_raw_quantity",
        "_raw_unit_price",
        "_raw_event_timestamp",
        "_corrupt_record",
    )

    # All output-affecting fields participate, so even timestamp ties have a stable winner.
    window = Window.partitionBy("order_id").orderBy(
        "event_timestamp",
        "source_file",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "raw_record",
        "source_record_id",
    )
    winner_window = window.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    ranked = (
        valid.withColumn("_row_number", F.row_number().over(window))
        .withColumn(
            "_winner_source_record_id",
            F.first("source_record_id").over(winner_window),
        )
        .withColumn(
            "_winner_payload_record",
            F.first("_payload_record").over(winner_window),
        )
    )
    duplicates = ranked.filter(F.col("_row_number") > 1).select(
        *[F.col(column) for column in ORDER_COLUMNS],
        "source_record_id",
        "source_path",
        "raw_record",
        F.col("_winner_source_record_id").alias("winner_source_record_id"),
        F.when(
            F.col("_payload_record") == F.col("_winner_payload_record"),
            "exact_duplicate",
        )
        .otherwise("conflicting_duplicate")
        .alias("duplicate_reason"),
    )
    winners = ranked.filter(F.col("_row_number") == 1).drop(
        "_row_number",
        "_winner_source_record_id",
        "_winner_payload_record",
        "_payload_record",
    )
    source_ids = identified.select("_source_record_id").withColumnRenamed(
        "_source_record_id", "source_record_id"
    )
    return winners, rejected, duplicates, source_ids


def _enrich(
    orders: DataFrame,
    products: DataFrame,
    customers: DataFrame,
    optimized: bool = False,
) -> DataFrame:
    product_side = F.broadcast(products) if optimized else products
    customer_side = F.broadcast(customers) if optimized else customers
    joined = orders.join(product_side, "product_id", "left").join(
        customer_side, "customer_id", "left"
    )
    return joined.withColumn(
        "revenue", (F.col("quantity") * F.col("unit_price")).cast(DecimalType(22, 2))
    )


def _publish_transactionally(
    frames_and_targets: Sequence[tuple[DataFrame, str, bool]],
    run_id: str,
    output_partitions: int,
) -> None:
    """Stage every output, then promote them together with rollback on promotion failure."""
    if not frames_and_targets:
        return
    jvm = frames_and_targets[0][0].sparkSession.sparkContext._jvm
    conf = frames_and_targets[0][0].sparkSession.sparkContext._jsc.hadoopConfiguration()
    token = uuid4().hex
    target_names = [target for _, target, _ in frames_and_targets]
    if len(set(target_names)) != len(target_names):
        raise ValueError("Output targets must be different paths")
    entries = []
    for frame, target_text, partition_by_category in frames_and_targets:
        target = jvm.org.apache.hadoop.fs.Path(target_text)
        filesystem = target.getFileSystem(conf)
        parent = target.getParent()
        stage = jvm.org.apache.hadoop.fs.Path(parent, f".{target.getName()}.staging-{token}")
        backup = jvm.org.apache.hadoop.fs.Path(parent, f".{target.getName()}.backup-{token}")
        entries.append((frame, target, stage, backup, filesystem, partition_by_category))

    try:
        for frame, _, stage, _, _, partition_by_category in entries:
            if partition_by_category:
                frame = frame.coalesce(output_partitions)
            writer = frame.write.mode("errorifexists")
            if partition_by_category:
                writer = writer.partitionBy("category")
            writer.parquet(stage.toString())

        backed_up = []
        promoted = []
        try:
            for _, target, _, backup, filesystem, _ in entries:
                if filesystem.exists(target):
                    if not _rename_path(filesystem, target, backup):
                        raise OSError(f"Could not move existing output {target} to backup {backup}")
                    backed_up.append((target, backup, filesystem))
            for _, target, stage, _, filesystem, _ in entries:
                if not _rename_path(filesystem, stage, target):
                    raise OSError(f"Could not promote staged output {stage} to {target}")
                promoted.append((target, filesystem))
        except Exception:
            for target, filesystem in reversed(promoted):
                if filesystem.exists(target) and not filesystem.delete(target, True):
                    LOGGER.error("Could not remove partially published output %s", target)
            for target, backup, filesystem in reversed(backed_up):
                if filesystem.exists(backup) and not _rename_path(filesystem, backup, target):
                    LOGGER.error("Could not restore output backup %s to %s", backup, target)
            raise

        for _, _, _, backup, filesystem, _ in entries:
            if filesystem.exists(backup) and not filesystem.delete(backup, True):
                LOGGER.warning("Published successfully but could not remove backup %s", backup)
    except Exception:
        LOGGER.exception("Output publish failed for run %s", run_id)
        for _, _, stage, _, filesystem, _ in entries:
            if filesystem.exists(stage) and not filesystem.delete(stage, True):
                LOGGER.error("Could not remove incomplete staging path %s", stage)
        raise


def run_pipeline(
    raw_orders: DataFrame,
    products: DataFrame,
    *,
    customers: DataFrame | None = None,
    optimized: bool = False,
    output_path: str | None = None,
    reject_path: str | None = None,
    run_id: str | None = None,
    output_partitions: int = 8,
) -> dict[str, Any]:
    """Validate, enrich, reconcile, and optionally publish one deterministic run.

    A customer dimension is optional for API compatibility. When supplied, customer
    IDs are checked as foreign keys and invalid references are quarantined.
    """
    run_id = run_id or str(uuid4())
    started_at = datetime.now(timezone.utc)
    started_clock = time.perf_counter()
    if output_partitions < 1:
        raise ValueError("output_partitions must be at least 1")
    if output_path is not None and not output_path.strip():
        raise ValueError("output_path must not be blank")
    if reject_path is not None and not reject_path.strip():
        raise ValueError("reject_path must not be blank")
    try:
        _validate_columns(raw_orders, ORDER_INPUT_COLUMNS, "orders")
        clean_products = _validate_dimension(
            products, PRODUCT_INPUT_COLUMNS, "product_id", "products"
        )
        clean_customers = (
            _validate_dimension(customers, CUSTOMER_INPUT_COLUMNS, "customer_id", "customers")
            if customers is not None
            else raw_orders.sparkSession.createDataFrame([], "customer_id string")
        )
        clean_customers = clean_customers.withColumn("_customer_exists", F.lit(True))
        valid, rejected, duplicates, source_ids = _clean_orders(raw_orders)
        matched = _enrich(valid, clean_products, clean_customers, optimized=optimized)
        if customers is None:
            matched = matched.withColumn("_customer_exists", F.lit(True))
        else:
            matched = matched.withColumn(
                "_customer_exists", F.coalesce(F.col("_customer_exists"), F.lit(False))
            )

        product_rejects = matched.filter(F.col("category").isNull()).select(
            *[F.col(column) for column in ORDER_COLUMNS],
            "source_record_id",
            "source_path",
            "raw_record",
            F.lit("unknown_product_reference").alias("reject_reason"),
        )
        customer_rejects = matched.filter(
            F.col("category").isNotNull() & ~F.col("_customer_exists")
        ).select(
            *[F.col(column) for column in ORDER_COLUMNS],
            "source_record_id",
            "source_path",
            "raw_record",
            F.lit("unknown_customer_reference").alias("reject_reason"),
        )
        revenue_rejects = matched.filter(
            F.col("category").isNotNull()
            & F.col("_customer_exists")
            & F.col("revenue").isNull()
        ).select(
            *[F.col(column) for column in ORDER_COLUMNS],
            "source_record_id",
            "source_path",
            "raw_record",
            F.lit("revenue_overflow").alias("reject_reason"),
        )
        accepted = (
            matched.filter(
                F.col("category").isNotNull()
                & F.col("_customer_exists")
                & F.col("revenue").isNotNull()
            )
            .drop("_customer_exists")
            .withColumn("run_id", F.lit(run_id))
        )
        quarantine = (
            rejected.unionByName(product_rejects)
            .unionByName(customer_rejects)
            .unionByName(revenue_rejects)
            .withColumn("run_id", F.lit(run_id))
        )
        duplicates = duplicates.withColumn("run_id", F.lit(run_id))

        disposition_rows = (
            source_ids.select("source_record_id", F.lit("input").alias("_disposition"))
            .unionByName(
                accepted.select(
                    "source_record_id", F.lit("accepted").alias("_disposition")
                )
            )
            .unionByName(
                quarantine.select(
                    "source_record_id", F.lit("rejected").alias("_disposition")
                )
            )
            .unionByName(
                duplicates.select(
                    "source_record_id", F.lit("duplicate").alias("_disposition")
                )
            )
        )
        by_source = disposition_rows.groupBy("source_record_id").agg(
            F.sum(F.when(F.col("_disposition") == "input", 1).otherwise(0)).alias(
                "_input_count"
            ),
            F.sum(F.when(F.col("_disposition") == "accepted", 1).otherwise(0)).alias(
                "_accepted_count"
            ),
            F.sum(F.when(F.col("_disposition") == "rejected", 1).otherwise(0)).alias(
                "_rejected_count"
            ),
            F.sum(F.when(F.col("_disposition") == "duplicate", 1).otherwise(0)).alias(
                "_duplicate_count"
            ),
        )
        disposition_stats = by_source.agg(
            F.sum("_input_count").alias("input_count"),
            F.sum("_accepted_count").alias("output_count"),
            F.sum("_rejected_count").alias("rejected_count"),
            F.sum("_duplicate_count").alias("duplicate_rows_removed"),
            F.sum(
                F.when(
                    (F.col("_input_count") != 1)
                    | (
                        F.col("_accepted_count")
                        + F.col("_rejected_count")
                        + F.col("_duplicate_count")
                        != 1
                    ),
                    1,
                ).otherwise(0)
            ).alias("coverage_errors"),
        ).first()
        input_count = disposition_stats.input_count or 0
        output_count = disposition_stats.output_count or 0
        rejected_count = disposition_stats.rejected_count or 0
        duplicate_rows_removed = disposition_stats.duplicate_rows_removed or 0
        if disposition_stats.coverage_errors:
            raise RuntimeError(
                "Pipeline source coverage failed: each input record must have exactly "
                "one accepted, rejected, or duplicate disposition"
            )

        sales_summary = accepted.groupBy(
            F.to_date("event_timestamp").alias("order_date"), "category"
        ).agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("revenue").alias("gross_revenue"),
        ).orderBy("order_date", "category")
        if sales_summary.filter(
            F.col("gross_revenue").isNull() | F.col("units_sold").isNull()
        ).limit(1).count():
            raise RuntimeError("Sales aggregate overflowed its supported numeric range")
        rejection_summary = quarantine.groupBy("reject_reason").count().orderBy("reject_reason")
        result = {
            "run_id": run_id,
            "started_at": started_at.isoformat(),
            "input_count": input_count,
            "output_count": output_count,
            "rejected_count": rejected_count,
            "duplicate_rows_removed": duplicate_rows_removed,
            "accepted": accepted,
            "rejected": quarantine,
            "duplicates": duplicates,
            "sales_summary": sales_summary,
            "rejection_summary": rejection_summary,
        }
        reconciliation(result).first()
        result["transform_ended_at"] = datetime.now(timezone.utc).isoformat()
        result["transform_elapsed_seconds"] = time.perf_counter() - started_clock

        outputs = []
        if output_path is not None:
            outputs.append((accepted, output_path, True))
        if reject_path is not None:
            outputs.append((quarantine, reject_path, False))
        _publish_transactionally(outputs, run_id, output_partitions)
        result["publish_ended_at"] = datetime.now(timezone.utc).isoformat()
        result["elapsed_seconds"] = time.perf_counter() - started_clock
        LOGGER.info(
            "Pipeline run %s complete: input=%d output=%d rejected=%d duplicates_removed=%d",
            run_id,
            input_count,
            output_count,
            rejected_count,
            duplicate_rows_removed,
        )
        return result
    except Exception:
        LOGGER.exception("Pipeline run %s failed", run_id)
        raise


def reconciliation(result: dict[str, Any]) -> DataFrame:
    """Return a one-row count reconciliation report."""
    spark = result["accepted"].sparkSession
    unaccounted = (
        result["input_count"]
        - result["output_count"]
        - result["rejected_count"]
        - result["duplicate_rows_removed"]
    )
    report = spark.createDataFrame(
        [(
            result["input_count"],
            result["output_count"],
            result["rejected_count"],
            result["duplicate_rows_removed"],
            unaccounted,
        )],
        [
            "input_count",
            "output_count",
            "rejected_count",
            "duplicate_rows_removed",
            "unaccounted_count",
        ],
    )
    if unaccounted != 0:
        raise RuntimeError(
            f"Pipeline reconciliation failed: {unaccounted} unaccounted rows"
        )
    return report
