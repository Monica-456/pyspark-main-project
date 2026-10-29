# Acceptance checklist

Verification was performed on 2026-10-29. Benchmark details are in
[`verification_report.md`](verification_report.md).

| Requirement | Result | Evidence |
|---|---|---|
| Baseline and optimized outputs match | Verified | 100,000 order CLI run compared accepted and quarantine rows; its report records parity as `true`. |
| Fixed-seed generator supports 100,000+ rows | Verified | Generator test writes 100,000 rows twice with seed 123 and compares all generated CSVs byte for byte; CLI also completed a seed 42 run with 100,000 generated valid orders. |
| Schema, key, and referential integrity checks | Verified | Tests cover missing order/dimension columns, blank and duplicate dimension keys, blank product category, malformed fields, unknown product/customer references, and the maximum price boundary. |
| Idempotency, retry, and recovery | Verified | Same-input rerun does not append rows; injected failure during second promotion restores prior datasets, removes staging/backup directories, and a retry succeeds. |
| Edge and failure cases | Verified | Updated suite covers invalid quantities/prices/timestamps, unknown references, empty input, duplicate selection, and failed publication recovery. |
| README, data dictionary, design trade-offs | Verified | README describes setup, input/output fields, validation, benchmark scope, broadcast trade-offs, full-refresh backfills, and transactional limitations. |

Backfills replay the supplied historical inputs as a full refresh. Incremental
date-range merges are outside the current pipeline contract.
