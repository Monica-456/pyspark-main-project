# Verification report

Run on 2026-10-29 against the local Windows checkout. Timings are machine-specific;
the join benchmark measures cached pipeline transformations and count actions, not
CSV generation/loading, parity checks, or output publishing.

## Environment and benchmark

| Setting | Value |
|---|---|
| OS | Windows 10, build 26300 |
| Logical CPUs | 14 |
| Python | 3.11.9, 64-bit |
| Java | Eclipse Temurin 17.0.20.1 |
| PySpark | 3.5.9 |
| Local Hadoop Windows tools | 3.3.6 (`winutils.exe`, `hadoop.dll`); Spark JARs bundle Hadoop 3.3.4 |
| Spark master | `local[*]` |
| Shuffle partitions | 8 |
| Generated valid orders | 10,000 |
| Total input rows | 10,004, including one duplicate and three deliberate rejected rows |
| Warmups | 1 per join mode |
| Measured repeats | 3 per join mode |

| Join mode | Samples (seconds) | Median |
|---|---|---:|
| Baseline | 7.0013, 7.2795, 6.8331 | 7.0013 |
| Broadcast optimized | 7.3896, 6.9713, 7.1457 | 7.1457 |

The broadcast path was about 2.1% slower by median on this run (speedup ratio
0.9798). Accepted and quarantine outputs matched row for row. This small local result
does not support claiming a speedup; benchmark results depend on data size, cluster,
cache state, and machine load.

### 100,000-order acceptance run

The end-to-end CLI was also run with seed 42, one warmup, and two measured repeats on
100,000 generated valid orders (100,004 input rows after the deliberate duplicate and
three invalid examples). Baseline samples were 7.5749 and 7.6583 seconds (median
7.6166); broadcast samples were 7.8352 and 7.4036 seconds (median 7.6194). The
median speedup ratio was 0.9996, effectively equal within run-to-run variation. The
CLI verified accepted/quarantine parity and published successfully. Spark emitted a
non-fatal Parquet memory-manager warning and reduced row-group allocations; output
publishing still completed.

The CLI reconciliation was 10,004 input = 10,000 accepted + 3 rejected + 1 duplicate
removed, with zero unaccounted rows. The three rejection reasons each occurred once:
non-positive quantity, unknown customer, and unknown product. The 10,000-order report
is `data/verification_20261029_output/reconciliation.json`; the 100,000-order report
is `data/acceptance_100k_output/reconciliation.json`.

## Automated tests

- Expanded full suite: 27 passed in 469.65 seconds. This includes the schema,
  dimension-integrity, price-limit, 100,000-row deterministic-generator, overwrite,
  rollback, cleanup, and retry cases.
- Coverage includes malformed and boundary orders, invalid dimensions, duplicate
  winner stability, reconciliation, baseline/broadcast parity, and output replacement.
- The recovery test injects a failure promoting the second dataset after the first
  has been promoted. It verifies that old outputs are restored, staging and backup
  directories are removed, and a subsequent retry publishes successfully.

## Publishing, reruns, and backfills

The CLI published accepted and quarantine Parquet outputs and its JSON report
successfully in the verification run. The existing-output test reruns the same input
and confirms row counts do not grow, then publishes a smaller input and confirms that
the previous dataset was replaced. The injected mid-promotion failure test verifies
rollback and successful retry. The CLI report's accepted and quarantine data also
passed row-for-row parity checks before publication.

This project performs full refreshes. A backfill is supported by supplying the
historical input set to a new run; it replaces the output paths rather than merging
only a date range. Incremental merge/backfill semantics are not implemented.

## Documentation and limits

The README documents Python/Java setup, Windows Hadoop configuration, CLI arguments,
input validation, deterministic deduplication, output publishing/recovery, benchmark
scope, and the accepted/quarantine field types. Hadoop's local Windows binaries are
version 3.3.6 while this PySpark distribution bundles Hadoop 3.3.4; the smoke and
10,000-order local publishing runs succeeded with this pair, but matching versions is
preferable for a separate production environment.
