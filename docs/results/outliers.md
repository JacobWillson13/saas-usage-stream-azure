# Outlier detection evaluation (task 3.5)

Generated 2026-10-06 20:36 UTC by `make evaluate-outliers`.
Recall: injected anomalies whose expected flag was raised on the injected row key.
Precision: flags of that type on injected rows of that anomaly, out of all flags of
that type in the same scope. **Precision is measured against injected labels only**:
flags on rows that were not injected count as false positives even when they are
real oddities in the source, so precision here is a lower bound.

## Baseline history (random injection, about 1 per 2,000 rows)

Delay is in source time from the anomaly row to the flag (history is scored in bulk).

| Anomaly | Flag | Injected | Detected | Recall | Flags | Precision | Median delay |
|---|---|---:|---:|---:|---:|---:|---:|
| activity_spike | activity_spike | 132 | 106 | 0.803 | 256 | 0.414 | 0.0 s |
| duplicate_payment | duplicate_row | 136 | 136 | 1.000 | 136 | 1.000 | 0.0 s |
| failure_burst | failure_rate | 150 | 123 | 0.820 | 67 | 0.493 | 0.0 s |
| negative_invoice | invalid_amount | 99 | 99 | 1.000 | 99 | 1.000 | 0.0 s |

## Live demo take (scripted at 60, 120, 180, 240 s)

Delay is wall time from insert to flag; consumers poll every 5 s.

| Anomaly | Flag | Injected | Detected | Recall | Flags | Precision | Median delay |
|---|---|---:|---:|---:|---:|---:|---:|
| duplicate_payment | duplicate_row | 1 | 1 | 1.000 | 1 | 1.000 | 2.5 s |
| failure_burst | failure_rate | 1 | 1 | 1.000 | 1 | 1.000 | 2.6 s |
| negative_invoice | invalid_amount | 1 | 1 | 1.000 | 1 | 1.000 | 2.6 s |
| session_spike | session_rate | 1 | 1 | 1.000 | 1 | 1.000 | 2.5 s |

## Activity spike: threshold choice

Robust z of an account's active_users vs its trailing 28 days, on all eligible
history rows. Precision is against injected labels only.

| z threshold | Flags | On injected rows | Recall | Precision | F1 |
|---:|---:|---:|---:|---:|---:|
| 3.5 | 2985 | 127 | 0.962 | 0.043 | 0.081 |
| 5 | 775 | 124 | 0.939 | 0.160 | 0.273 |
| 7 (default) | 256 | 106 | 0.803 | 0.414 | 0.546 |

Best F1 is at z > 7; the scorer's default is z > 7. The tables above use the default.

## Naturally occurring hits

Flags on rows that were not injected.

| Flag | Baseline | Live take |
|---|---:|---:|
| activity_spike | 150 | 0 |
| failure_rate | 34 | 0 |
| orphan_payment | 105 | 0 |
| test_mode_payment | 18 | 0 |

## Notes

- `activity_spike` (history) uses z > 7, the best F1 of the thresholds tested (table
  above). At 3.5 it caught 96% of injected spikes but raised 2,858 other flags.
- `failure_rate` has one flag per source hour. Natural payment failures in the source
  are monthly retry runs (up to 23 failures in one hour), which look like injected
  bursts and are flagged as natural hits. Two bursts in one hour share one flag.
- Missed bursts land on the 2nd of a month, right after the monthly retry run on the
  1st: that run is nearly all of the trailing week's few payments (pooled failure rate
  about 0.9), so 20 failures in a busy hour look normal against it.
- `session_rate` and `ingest_volume` only run in the live demo.
- Rule checks (`test_mode_payment`, `orphan_payment`, `license_overuse`) have no injected
  counterpart; their counts are natural data errors kept in the prepared data.
