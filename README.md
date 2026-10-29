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

Run a short example and the test suite:

```powershell
.\.venv\Scripts\python.exe -m src.main --records 10 --benchmark-repeats 1 --warmup-runs 0
.\.venv\Scripts\python.exe -m pytest -q
```

Both `Test-Path` commands should return `True`. If your Hadoop binaries are installed
elsewhere, set `HADOOP_HOME` to that directory instead. Use binaries from a trusted
source that match the Hadoop version bundled with PySpark.

The default run generates 100,000 valid synthetic orders, performs one warm-up and
three measured runs of each join strategy, writes Parquet outputs under
`data/output/`, and saves `data/output/reconciliation.json`. Set `--records` lower
for a quick smoke test. Inputs are regenerated reproducibly for a fixed seed.

## CLI options

| Option | Default | Description |
|---|---:|---|
| `--records` | `100000` | Number of generated valid order records (must be positive) |
| `--seed` | `42` | Random seed for reproducible orders |
| `--data-dir` | `data` | Directory for generated CSV inputs |
| `--output-dir` | `data/output` | Directory for published outputs and report |
| `--benchmark-repeats` | `3` | Measured runs per execution strategy |
| `--warmup-runs` | `1` | Unmeasured runs per strategy |
| `--shuffle-partitions` | `8` | Spark SQL shuffle partition setting |
| `--output-partitions` | `8` | Maximum coalesced tasks used for accepted output |

## Input contracts and validation

The CLI supplies explicit all-string Spark schemas for the CSV inputs, preserving
malformed values for row-level validation. The pipeline checks that required fields
exist and that dimension keys are nonblank and unique. Invalid dimension structure
fails the run with a clear error rather than producing ambiguous joins.

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
fields, a serialized `raw_record`, and the first applicable `reject_reason`.

### Dimensions

- `products.csv`: unique, nonblank `product_id` and nonblank `category`.
- `customers.csv`: unique, nonblank `customer_id`.
- A missing product or customer reference is quarantined as
  `unknown_product_reference` or `unknown_customer_reference`, respectively.
- For compatibility with existing direct Python callers, `run_pipeline` accepts an
  omitted customer DataFrame; in that mode the customer reference check is skipped.
  The CLI always loads and validates the generated customer dimension.

The generator writes 10,000 customers and includes deliberate examples of a repeated
order ID, a negative quantity, an unknown product, and an unknown customer.

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

The accepted dataset adds `category`, decimal `revenue` (`Decimal(18,2)`), and
`run_id`. Accepted Parquet is partitioned by category. The quarantine includes the
original order fields, `raw_record`, and `reject_reason`. Sales summaries are grouped
by event date and category; rejection counts are grouped by reason.

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
| Accepted | `event_timestamp` | timestamp | Parsed event time |
| Accepted | `source_file` | string | Input provenance; null becomes `unknown` |
| Accepted | `category` | string | Product category and Parquet partition key |
| Accepted | `revenue` | decimal(18,2) | `quantity * unit_price` |
| Accepted | `run_id` | string | Identifier for this pipeline run |
| Quarantine | Order fields above | Same types | Parsed values where valid; malformed numeric/time values are null |
| Quarantine | `raw_record` | string (JSON) | Original row values serialized before casting |
| Quarantine | `reject_reason` | string | First applicable validation or reference failure |

The reconciliation JSON stores input/accepted/rejected/duplicate counts, rejection
counts by reason, sales totals by date and category, benchmark samples, and output
paths. A rerun replaces the output datasets. A backfill therefore reprocesses the
input files supplied for that run as a full refresh; the pipeline does not merge
incremental date ranges into existing Parquet data.

## Reconciliation and recovery

The pipeline accounts for every input row using:

```text
input = accepted + rejected + duplicate_rows_removed
```

It raises an error if reconciliation leaves any unaccounted rows. On a successful
CLI run, `data/output/reconciliation.json` contains the count reconciliation,
rejections by reason, sales summaries, input/output paths, and benchmark metadata.
The report is written through a temporary file and atomic replacement.

When output paths are provided, the pipeline writes both datasets to unique sibling
staging paths before publishing either. Existing outputs are renamed to backups,
then staged outputs are promoted. If promotion fails, already-promoted outputs are
removed and previous outputs are restored; failures are logged and re-raised. This
protects the prior successful run from partial replacement on the local filesystem
and Hadoop-compatible filesystems. It is not a distributed transaction across
storage systems, and process termination during promotion may require manual
inspection of staging or backup directories.

Rerunning the same command replaces the output datasets rather than appending
duplicates. A fixed seed reproduces the input data; each run receives a new `run_id`.

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
