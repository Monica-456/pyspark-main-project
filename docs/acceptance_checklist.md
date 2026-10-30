# Acceptance checklist

Earlier verification was performed on 2026-10-29. Benchmark details are in
[`verification_report.md`](verification_report.md).

| Requirement | Result | Evidence |
|---|---|---|
| Baseline and optimized outputs match | Verified | 100,000 order CLI run compared accepted and quarantine rows; its report records parity as `true`. |
| Fixed-seed generator supports 100,000+ rows | Verified | Generator test writes 100,000 rows twice with seed 123 and compares all generated CSVs byte for byte; CLI also completed a seed 42 run with 100,000 generated valid orders. |
| Schema, key, and referential integrity checks | Verified | Tests cover explicit CSV header contracts, missing order/dimension columns, blank and duplicate dimension keys, malformed fields, unknown references, and numeric boundaries. |
| Independent row reconciliation | Verified | Duplicate count comes from the ranking stage; source-record coverage asserts each input has exactly one accepted, rejected, or duplicate disposition. |
| Revenue and ANSI-safe parsing | Verified | Tests cover maximum quantity/price revenue and malformed integer/timestamp quarantine with ANSI mode enabled. |
| Generation publication | Verified | CLI writes immutable run generations, reads back Parquet, and changes the active-run pointer only after report creation and validation. |
| Input preservation | Verified | Demo generation requires `--generate-demo`; ingest-only CLI mode reads existing inputs and does not regenerate them. |
| Traceability and reporting timezone | Verified | Accepted, quarantine, and duplicate audit rows carry run/source identities; reports record the configured timezone and pipeline timings. |

Backfills replay the supplied historical inputs as a full refresh. Incremental
date-range merges are outside the current pipeline contract. The 2026-10-29 benchmark
evidence predates these correctness changes and is not new performance verification.
