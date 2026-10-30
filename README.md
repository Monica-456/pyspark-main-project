# PySpark Order Pipeline

A local PySpark example for validating order data, enforcing product and customer
referential integrity, deterministically resolving duplicate order IDs, quarantining
bad rows, and comparing baseline and broadcast-join execution.

## Requirements and setup

- Python 3.9+
- Java 11 or 17 available on `PATH` (required by Spark)
- On Windows, Hadoop local-file output may require `winutils.exe` and the matching
  `hadoop.dll` under `%HADOOP_HOME%\bin`; Spark worker processes use the active
  Python interpreter.

From PowerShell in the project directory:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Verify Java is installed before running Spark:

```powershell
java -version
```

If Spark reports `HADOOP_HOME and hadoop.home.dir are unset`, or a native
`NativeIO$Windows.access0` error while writing output, install both `winutils.exe`
and its matching `hadoop.dll` under `%HADOOP_HOME%\bin`, then configure the current
PowerShell session before running Spark. This checkout has a locally downloaded
Hadoop 3.3.6 pair under `tools\hadoop-3.3.6`; the PySpark Hadoop JARs are version
3.3.4, so the binaries are not an exact version match.

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
$env:HADOOP_HOME = (Resolve-Path .\tools\hadoop-3.3.6).Path
$env:Path = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:Path"
$env:PYSPARK_PYTHON = (Resolve-Path .\.venv\Scripts\python.exe).Path
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
$env:HADOOP_HOME
Test-Path "$env:HADOOP_HOME\bin\winutils.exe"
Test-Path "$env:HADOOP_HOME\bin\hadoop.dll"
```

Run a short example against the existing inputs and the test suite:

```powershell
.\.venv\Scripts\python.exe -m src.main --benchmark-repeats 1 --warmup-runs 0
.\.venv\Scripts\python.exe -m pytest -q
```

Demo inputs are generated only when explicitly requested. Generation writes into
`--data-dir` and replaces its three CSV files:

```powershell
.\.venv\Scripts\python.exe -m src.main --generate-demo --records 10 --data-dir data\demo --benchmark-repeats 1 --warmup-runs 0
```

For prepared or historical inputs, pass `--orders-path`, `--products-path`, and
`--customers-path` (or use the corresponding default files under `--data-dir`).
Without `--generate-demo`, the CLI does not modify inputs.

Both `Test-Path` commands should return `True`. If your Hadoop binaries are installed
elsewhere, set `HADOOP_HOME` to that directory instead. Use binaries from a trusted
source that match the Hadoop version bundled with PySpark.

The default run reads the existing CSVs under `data`, performs one warm-up and three
measured runs of each join strategy, and publishes a new immutable generation under
`data/output/runs/`. Set `--records` and `--generate-demo` to generate demo inputs;
`--records` counts valid generated orders, before deliberate invalid/duplicate rows.
A fixed seed reproduces generated inputs.

## CLI options

| Option | Default | Description |
|---|---:|---|
| `--records` | `100000` | Number of generated valid order records (must be positive) |
| `--seed` | `42` | Random seed for reproducible orders |
| `--data-dir` | `data` | Directory for generated CSV inputs |
| `--orders-path` | `data/orders.csv` | Existing order CSV to ingest |
| `--products-path` | `data/products.csv` | Existing product CSV to ingest |
| `--customers-path` | `data/customers.csv` | Existing customer CSV to ingest |
| `--generate-demo` | disabled | Explicitly generate demo input files before processing |
| `--output-dir` | `data/output` | Directory for published outputs and report |
| `--timezone` | `UTC` | Spark timezone used to derive sales report dates |
| `--benchmark-repeats` | `3` | Measured runs per execution strategy |
| `--warmup-runs` | `1` | Unmeasured runs per strategy |
| `--shuffle-partitions` | `8` | Spark SQL shuffle partition setting |
| `--output-partitions` | `8` | Maximum coalesced tasks used for accepted output |

## Input contracts and validation

The CLI validates exact CSV header names and order before loading explicit all-string
Spark schemas. Reordered, duplicate, missing, and additional headers fail visibly.
Malformed order records are retained in quarantine with their corrupt source text and
input path; malformed dimension records fail the run. Dimension keys must be
nonblank and unique.

### Orders

`orders.csv` must contain:

| Column | Validation |
|---|---|
| `order_id` | Required and nonblank |
| `customer_id` | Required and nonblank |
| `product_id` | Required and nonblank |
| `quantity` | Integer greater than zero |
| `unit_price` | Decimal with at most two fractional digits, in `[0, 9999999999.99]` |
| `event_timestamp` | Must parse as a Spark timestamp |
| `source_file` | Preserved as provenance; null values become `unknown` |

Orders with malformed or invalid values are retained in quarantine with their source
fields, a serialized `raw_record`, `source_record_id`, source path, `run_id`, and the
first applicable `reject_reason`.

### Dimensions

- `products.csv`: unique, nonblank `product_id` and nonblank `category`.
- `customers.csv`: unique, nonblank `customer_id`.
- A missing product or customer reference is quarantined as
  `unknown_product_reference` or `unknown_customer_reference`, respectively.
- For compatibility with existing direct Python callers, `run_pipeline` accepts an
  omitted customer DataFrame; in that mode the customer reference check is skipped.
  The CLI always loads and validates the supplied customer dimension.

The generator writes 10,000 customers. `--records N` means N valid base order rows;
the generated orders file also has four deliberate extra rows: one repeated order
ID, one negative quantity, one unknown product, and one unknown customer (N + 4
input rows total).

## Processing and deterministic duplicates

The flow is:

```text
CSV -> required-column checks -> cast/row validation -> deterministic deduplication
    -> product/customer left joins -> accepted and quarantine datasets
    -> sales and rejection summaries -> reconciliation and JSON report
```

Only structurally valid rows participate in order-ID deduplication. The retained
record is selected by ascending event timestamp, source file, customer ID, product
ID, quantity, unit price, and serialized raw record. This fully orders records that
differ in output-affecting fields; exact duplicate rows are interchangeable.
Late event timestamps are retained.

The business grain is one retained accepted record per `order_id`; each input row
represents a whole order, not an order line. Structurally valid versions are ranked
by event timestamp and stable tie-breakers before reference checks. The earliest
candidate wins even if its product/customer reference is invalid; later versions
are recorded as duplicates rather than promoted. Exact duplicate payloads and
conflicting versions are distinguished in the duplicate audit dataset.

The accepted dataset adds `category`, decimal `revenue` (`Decimal(22,2)`), and
`run_id`. Accepted Parquet is partitioned by category. The quarantine includes the
original order fields, `source_record_id`, `source_path`, `raw_record`, and
`reject_reason`. Revenue supports the maximum positive signed 32-bit quantity and
maximum documented unit price without row-level overflow. Sales reporting dates use
the configured Spark timezone (UTC by default) for timestamp-to-date conversion.

### Output data dictionary

The accepted Parquet dataset contains the validated order fields below, plus the
derived fields shown here. Parquet readers reconstruct `category` from the partition
directory name.

| Dataset | Field | Type | Meaning |
|---|---|---|---|
| Accepted | `order_id` | string | Retained unique order key |
| Accepted | `customer_id` | string | Validated customer key |
| Accepted | `product_id` | string | Validated product key |
| Accepted | `quantity` | integer | Positive units ordered |
| Accepted | `unit_price` | decimal(12,2) | Validated price per unit |
| Accepted | `event_timestamp` | timestamp | Parsed event time; naive values are interpreted in the configured Spark timezone |
| Accepted | `source_file` | string | Input provenance; null becomes `unknown` |
| Accepted | `source_path` | string | Actual input path when loaded by the CLI |
| Accepted | `category` | string | Product category and Parquet partition key |
| Accepted | `revenue` | decimal(22,2) | `quantity * unit_price` |
| Accepted | `run_id` | string | Identifier for this pipeline run |
| Accepted | `source_record_id` | string | Source payload and occurrence identity |
| Quarantine | Order fields above | Same types | Parsed values where valid; malformed numeric/time values are null |
| Quarantine | `source_record_id` | string | Source payload and occurrence identity |
| Quarantine | `source_path` | string | Actual input path when loaded by the CLI |
| Quarantine | `run_id` | string | Identifier for this pipeline run |
| Quarantine | `raw_record` | string (JSON) | Original row values serialized before casting |
| Quarantine | `reject_reason` | string | First applicable validation or reference failure |
| Duplicate audit | `winner_source_record_id` | string | Retained source record for this order ID |
| Duplicate audit | `duplicate_reason` | string | `exact_duplicate` or `conflicting_duplicate` |

The reconciliation JSON stores input/accepted/rejected/duplicate counts, rejection
counts by reason, sales totals by date and category, benchmark samples, timezone,
timings, and output paths. Each successful CLI run is an immutable generation under
`data/output/runs/<run_id>/`, containing accepted, quarantine, duplicate audit, and
the report. `data/output/active-run.json` is atomically updated after all artifacts
are written and verified; it identifies the active successful run. A backfill
reprocesses the complete supplied input set as a full refresh; incremental date-range
merges are not supported.

## Reconciliation and recovery

The pipeline independently measures duplicates from its ranking stage and checks
source-record identity coverage across accepted, rejected, and duplicate
dispositions. It accounts for every input row using:

```text
input = accepted + rejected + duplicate_rows_removed
```

It raises an error if reconciliation leaves any unaccounted rows. On a successful
CLI run, the active generation's `reconciliation.json` contains count reconciliation,
rejections and sales summaries, paths, timezone, timings, and benchmark metadata.
Persisted Parquet is read back and checked before the active pointer changes.

The Python API's optional output paths use sibling staging and backup paths to
recover from dataset promotion failures. The CLI writes immutable run generations
and publishes the active pointer last. A report-write or validation failure leaves
the previous active generation unchanged. An incomplete or unactivated generation
may remain after process termination; it is not active and can be inspected
separately.

Each run receives a new `run_id`; a fixed seed reproduces explicitly generated demo
inputs. Historical generations are retained rather than overwritten.

## Benchmark and optimization

Both variants execute the same validation, deduplication, joins, and count actions.
The benchmark caches the same input DataFrames, alternates which strategy runs first,
discards configured warm-ups, measures multiple repetitions, and reports each sample
and the median. It also checks accepted and quarantine row-for-row equality before
publishing. The measured time is for transformations and count actions; CSV reads,
input generation, parity checks, and output publishing are excluded. Results are
machine- and workload-dependent; a broadcast join is not guaranteed to be faster.

The baseline performs regular left joins. The optimized path broadcasts the small
product and customer dimensions to reduce join shuffles. Accepted output is
coalesced to the configured task count before category-partitioned writing to limit
small output files. Tune the partition settings for the available machine and data
size.

## Project structure

```text
my_pyspark_pipeline/
├── README.md
├── requirements.txt
├── src/
│   ├── generator.py
│   ├── main.py
│   └── pipeline.py
├── tests/
│   └── test_pipeline.py
└── data/                 # generated inputs and outputs
```

See [`docs/acceptance_checklist.md`](docs/acceptance_checklist.md) for the verification
matrix and [`docs/verification_report.md`](docs/verification_report.md) for benchmark
environment and measured results.
