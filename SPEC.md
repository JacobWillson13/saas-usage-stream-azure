# SPEC: saas-usage-stream-azure

Status: v1, 2026-10-06. Due 2026-10-08. Big Data Project 1: Streaming Analytics Pipeline on Azure PostgreSQL.

## 1. Purpose

Simulate a live B2B SaaS product where usage and billing records arrive continuously in Azure Database for PostgreSQL, and build three components on top:

1. A live dashboard in Azure Managed Grafana.
2. Outlier detection that flags data errors and unusual behavior as rows arrive.
3. An online model (River) that predicts trial conversion and updates as activity arrives, evaluated prequentially.

Success: during a live replay, the Grafana dashboard moves every 5 seconds, injected anomalies get flagged with measured precision and recall, and the model's rolling AUC is visible next to a fixed baseline.

The repo is public as an Azure showcase. Keep it simple and good-looking.

## 2. The product

A generic B2B SaaS product for connecting devices. Free accounts for individuals, paid team plans per license, annual business contracts. Business signups start a 14-day trial and either convert to paid or fall back to free. No company name.

Data is synthetic, derived from a separate personal simulation project. The README and report say this in one sentence.

## 3. Data

### 3.1 Prepared data (the public starting point)

The committed pipeline starts from `data/prepared/*.parquet`: generic names, re-keyed IDs, no personal fields. These files are committed to the repo. Publishing them as a GitHub Release asset is optional.

The step that builds them from the original simulation export lives in `local/` (gitignored) and is run once by Jake. Source table names, column names, and dedupe details are kept there too, not in tracked files. Its rules:

- Re-key every ID: `<prefix>_` + first 12 hex chars of `md5(RENAME_SALT || source_id)`. Prefixes: `acc_`, `usr_`, `dev_`, `inv_`, `pay_`, `chg_`, `lic_`.
- Drop names, emails, raw domains, metadata blobs, and sync bookkeeping columns after using them for filtering. Drop soft-deleted rows.
- Where the export holds several versions of a record, keep the latest version.
- Keep test-mode payments and orphan payments. They are real data errors the outlier layer should catch.
- Map every coded value through `config/mappings.yml`. An unmapped value is a hard failure.
- Money: `numeric(12,2)` in the original currency plus `*_usd` using fixed illustrative rates (EUR 1.08, GBP 1.27) from `config/settings.yml`.
- Accounts carry signup attributes only. Never carry the final plan or the actual trial end: that leaks the label.

### 3.2 Prepared tables

| Table | Key | Event timestamp | Notes |
|---|---|---|---|
| accounts | account_id | created_at | `account_type` business (trial or business signup) or personal. `signup_plan`, `price_version`, `trial_started_at`, `trial_ends_at` (scheduled: start + 14 days), currency, `is_nonprofit`, `is_internal`. |
| users | user_id | invited_at | role. |
| devices | device_id | registered_at | `device_fingerprint` (salted hash), os, `is_tagged`. |
| usage_daily | account_id, activity_date | activity_date + jitter | active_users, user_devices, tagged_resources, ephemeral_minutes. |
| feature_usage_daily | account_id, activity_date, feature | activity_date + jitter | attempts, gated_blocked. |
| license_events | license_event_id | occurred_at | event_type, licenses_held_after, licenses_used_after. |
| plan_changes | plan_change_id | changed_at | from/to plan and price version, change_source. |
| invoices | invoice_id | issued_at | latest status, money columns, service period, due date, paid_at. |
| payments | payment_id | created | status, amount, refunds, `is_test`, nullable `account_id` and `invoice_id`. |

Daily-grain rows get a deterministic jittered time within the day (seeded by the row key), clamped to be no earlier than the parent account's `created_at`. Every child row's `event_ts` is at or after its parent's.

### 3.3 Generic values

```
plan: free, basic, team, pro, business      price_version: legacy, current
signup_plan: free, pro, business, trial     change_source: trial_resolution, self_service,
                                                           assisted_sale, voluntary_migration,
                                                           collections
```

Features, license event types, and statuses are listed in `config/mappings.yml`.

## 4. Architecture

```
data/prepared/*.parquet
   |                       \
make baseline (COPY,       make demo: live emitter (events generated from DEMO_DATE's
 shifted, injection)        source records, DEMO_RATE/min, scripted + random injection)
   \                       /
 Azure Database for PostgreSQL Flexible Server (dsa508, B1ms, PG 18)
   |                    |                       |
Azure Managed Grafana   outlier scorer          trial model (River)
(5 s refresh, dash.*)   -> ops.outlier_flags    -> ops.predictions, ops.trial_outcomes,
                                                   ops.model_metrics
```

The emitter, scorer, and model run on Jake's MacBook as one `make demo` process. Only the database and Grafana are on Azure.

### 4.1 Time and live demo

- `source_event_ts`: the original event time (2023 to 2026-10-01) for history, and the take clock for live events (below). The scorer and model use it for every window and for label timing.
- `event_ts`: display time. History is shifted forward by `offset = DEMO_DATE - SOURCE_CUTOFF` days (defaults: today and 2026-09-04), so it ends the day before `DEMO_DATE`; every other date and timestamp column in `app` moves by the same offset. Live events get `event_ts = now()` at insert. The offset is logged in `ops.replay_state` and disclosed in the report and README, not on the dashboard. Prepared Parquet stays unshifted.
- `ingested_at`: wall time at insert, default `now()`.
- `make baseline`: load every prepared row before `SOURCE_CUTOFF` into the daily and single-row tables (`is_backfill = true`) with random injection at the normal rate, run the scorer and model over it in fast mode with the live code paths (the model's warm start), then save a checkpoint in `data/checkpoints/`: the model pickle and `checkpoint.json` (consumer offsets and ops id high-water marks).
- `make demo`: the live emitter, scorer, and model in one process; Ctrl-C stops all three cleanly. The emitter simulates real-time events from the source records of `DEMO_DATE` (source day `SOURCE_CUTOFF`):
  - each `usage_daily` row becomes `active_users` rows in `app.sessions`, each assigned to a real user and device of that account
  - each `feature_usage_daily` row becomes `attempts` rows in `app.feature_events`
  - that day's signups, user invites, devices, license events, plan changes, invoices, and payments are emitted as single rows
  - generated payments: real non-test payments from the 30 days before `DEMO_DATE`, re-keyed, about one every 10 seconds, succeeded and failed in their historical ratio (`is_generated = true`)
  - everything is interleaved in a seeded random order (a child never before a same-day parent) and emitted continuously at `DEMO_RATE` events/min (default 500) with exponential gaps. Due events are inserted every second in one transaction with the ingest log, injected anomalies, and the emitter's cursor, with `event_ts = now()` and `is_demo = true`.
  - the take clock: the source day's midnight plus the scheduled seconds into the take. It is each live event's `source_event_ts`. Display timestamps on live rows (`created_at`, `registered_at`, `started_at`, ...) equal `event_ts`, or `event_ts` plus their original offset (for example `trial_ends_at` = `event_ts` + 14 days); source times live only in `source_event_ts`.
  - scripted anomalies at 60, 120, 180, and 240 seconds (session spike, duplicate payment, negative invoice, failure burst), plus random injection at a low rate (session spikes, duplicate payments, negative invoices; a take spans one source hour, so failure bursts are scripted only).
  - the take is a pure function of the prepared data and `DEMO_SEED`, so reset then demo reproduces the same events, anomalies, flags, and predictions. One day yields about 7,300 events, about 15 minutes at the default rate; the take ends there.
- `make demo-reset`: refuse while a demo is running; otherwise delete `is_demo` rows and every ops row written after the checkpoint, and restore the consumer offsets and the emitter cursor. The model reloads the checkpoint pickle on start. Idempotent, under 30 seconds.
- Grafana live panels cover the last 15 minutes by second and minute. Daily trend panels combine history daily rows with today's live events rolled up to the day.

### 4.2 Database

Schemas: `app` (streamed tables), `ops` (component outputs and state), `dash` (views for Grafana).

Every `app` table has `event_ts timestamptz`, `source_event_ts timestamptz`, `ingested_at timestamptz default now()`, `ingest_id bigint unique` from one sequence shared by all app tables (so consumers read every table in insert order), `is_backfill boolean`, `is_demo boolean`, `is_injected boolean` (created or modified by anomaly injection; business KPI views exclude these rows, outlier views include them), a BRIN index on `ingested_at`, btrees on `ingest_id`, `event_ts`, and `source_event_ts`, and a partial index on demo rows.

Live-only tables: `app.sessions(session_id, account_id, user_id, device_id, started_at, duration_s)` and `app.feature_events(event_id, account_id, user_id, feature, gated_blocked, occurred_at)`, with the standard ingest columns. History for the same activity stays in `usage_daily` and `feature_usage_daily`. `payments.is_generated` marks generated live payments.

FKs: `account_id` on every child table references `app.accounts`, nullable on `payments`. `devices.user_id`, `sessions.user_id`, and `feature_events.user_id` reference `app.users`; `sessions.device_id` references `app.devices`. `payments.invoice_id` references `app.invoices`, nullable.

`ops` tables: `replay_state` (offset, demo date, emitter cursor, flush count), `ingest_log` (one row per batch) with `ingest_log_tables` (rows per table per batch), `consumer_offsets`, and the insert-only outputs `injected_anomalies`, `outlier_flags`, `predictions`, `trial_outcomes`, `model_metrics`.

Roles: admin for DDL only; `stream_rw` for the local apps; `grafana_ro` (SELECT on `dash` only) for Grafana.

## 5. Dashboard (deliverable iii)

Azure Managed Grafana, Standard tier, PostgreSQL data source with `grafana_ro`, 5 s refresh. Dashboard JSON committed at `grafana/dashboard.json`. Screenshots and a GIF in `docs/img/` because the live instance is deleted after grading.

Rows:
1. Live (last 15 minutes, by second and minute): events per second, events per minute by table, sessions and active users per minute, feature events by feature, signups and trials, payments and failures, flags per minute by type, the latest 20 flags with reason, demo status, and time into the current take (mm:ss, or idle).
2. Usage trends (daily, history plus today's live events): active accounts and active users (last 7 days), signups and trials per day, devices registered, feature attempts.
3. Billing (daily): invoiced USD, payments succeeded vs failed, failure rate.
4. Outliers: precision and recall against injected anomalies.
5. Model: rolling AUC and log loss vs baseline, trials resolved, top 10 open trials by predicted probability.

## 6. Outlier detection (deliverable iv)

All flags go to `ops.outlier_flags(flag_id, table_name, row_key, flag_type, method, score, reason, source_event_ts, event_ts, flagged_at)`, unique on `(table_name, row_key, flag_type)`. Windows use `source_event_ts`.

**Rules (data errors):**
- `test_mode_payment`: `is_test = true`.
- `orphan_payment`: payment with no invoice.
- `invalid_amount`: invoice total < 0, or payment amount <= 0.
- `license_overuse`: licenses_used_after > licenses_held_after.
- `duplicate_row`: a payment with the same account, invoice, amount, status, and source time as an earlier one (other tables have natural primary keys).

**Statistical (robust z-score, MAD):** `|x - median| / max(1.4826 * MAD, floor) > 3.5` unless stated otherwise. The floor stops a flat history (MAD 0) from flagging every small change.
- `activity_spike`: an account's active_users vs its trailing 28 days (minimum 7 days of history), floor 1 user, threshold z > 7 (best F1 against injected spikes among 3.5, 5, and 7; see `docs/results/outliers.md`).
- `failure_rate`: the hour's payment failure rate vs hourly rates over the trailing 7 days, once the hour has 5 payments. The floor is the binomial noise of the hour's rate at the trailing 7-day pooled failure rate (at least 2%), so one failure among a few payments does not flag but a burst does. Natural failures in the source are monthly retry runs (up to 23 failures in one hour), so about 40 natural hours are flagged in the baseline; none fall after the cutoff.
- `session_rate` (live): an account's sessions in the current minute of the take vs its previous minutes of the take (up to 10, minimum 1), floor `max(2, sqrt(median))` so busy accounts' normal variation does not flag; flags increases only.
- `ingest_volume`: demo rows per wall minute vs the trailing 30 minutes (minimum 10), floor 20% of the median. Catches stalls and bursts.

**Injected anomalies** (logged in `ops.injected_anomalies` with the row key their flag should carry):
- activity spike (baseline): active_users x10 for one account-day with at least 7 days of history (flag `activity_spike`)
- session spike (live): 10x the account's daily active users in sessions (20 to 60) within one minute (flag `session_rate`)
- duplicate payment row with a new id (flag `duplicate_row`)
- negative invoice total (flag `invalid_amount`)
- failure burst: 20 failed payments within one source hour (flag `failure_rate`)
- ingest stall: no inserts for 90 seconds (cut with task 3.4; the `ingest_volume` check remains)

The baseline injects about 1 anomaly per 2,000 rows, split evenly across activity spikes, duplicate payments, negative invoices, and failure bursts. The demo adds the four scripted anomalies and random injection at about 1 per 5,000 events (session spikes instead of activity spikes). Scripted invoice and payment anomalies are synthetic rows templated on the latest real one, because invoices and payments arrive in daily runs and a given batch may have none. Every choice is a hash of a fixed seed (`DEMO_SEED` for the demo) and the row key.

**Evaluation:** precision, recall, and median detection delay per anomaly type against `ops.injected_anomalies`; counts for naturally occurring hits. Precision is measured against injected labels only, so natural oddities count as false positives.

## 7. Trial conversion model (deliverable v)

**Population:** business accounts with a trial start.

**Label:** the first plan change on or after trial start with `change_source = trial_resolution`. Converted (1) unless the destination plan is `free`, which means fallback (0). The label becomes known at that change's source time. A trial with no such change by trial end + 3 days (source time) is unlabeled. Trials whose trial end + 3 days falls after the last event in the prepared data (`DATA_END`, 2026-10-01) are excluded from training and evaluation.

**Time:** features, the trial window, and label timing use source time: `source_event_ts`, and for other columns `source_event_ts` plus the column's offset from `event_ts`. `event_ts` is copied onto outputs for display only.

**Live updates:** during the demo, open-trial features update from `sessions` (distinct users and devices per day feed the same active-user, active-day, and devices-per-user features as `usage_daily`) and `feature_events` (distinct features, gated attempts), and labels come from live plan changes.

**Features (built incrementally from `app` rows only):**
- days into trial
- max active users, active days, average devices per active user, max tagged resources
- gated feature attempts, distinct features used
- users invited, devices registered, distinct OS count, license adds
- `prior_personal_device`: a device fingerprint previously registered on a personal account
- price version (legacy or current), currency, is_nonprofit

**Loop:** on each new usage, feature, session, or feature event row for an open trial, update features and write a prediction to `ops.predictions` (at most one per account per source day). When a label arrives, score the trial at its day-7 prediction (the latest prediction on or before day 7 of the trial; the running base rate if it has none) into `ops.trial_outcomes`, then `learn_one` with the same day-7 vector. Outputs are insert-only, so a demo reset can roll them back.

**Models:**
- River `StandardScaler | LogisticRegression` (primary): log1p on count features, days into trial as a fraction of 14, Adam (learning rate 0.003), L2 0.001
- Fixed points baseline, no learning: one point each for 3+ peak active users, 5+ active days, 3+ users invited, 5+ devices, 2+ features, a gated attempt, a license add, and a prior personal device; probability = points / 8, clipped to [0.05, 0.95]

**Warm start:** `make baseline` runs the same loop over every trial before the cutoff (phase `warm`) and pickles the model as the demo checkpoint. Demo outputs are phase `live`. Report the two separately.

**Metrics:** rolling ROC AUC and log loss over the last 100 resolved trials, cumulative AUC, and top-decile conversion lift. Write to `ops.model_metrics` every 10 resolved trials per model. Note any change around 2026-04-08, when new signups move to the current price version. Scoring at a fixed day keeps label timing out of the evaluation: scoring at the last prediction before the label (the earlier rule) let it leak in through days into trial (see `docs/results/model.md`). The Hoeffding tree comparison was cut with task 4.4.

## 8. Repo layout

```
config/settings.yml   config/mappings.yml   .env.example
sql/ddl/              sql/views/
src/saas_stream/      settings.py rows.py db.py backfill.py replay.py inject.py consumer.py
                      outliers.py model.py demo.py evaluate.py
grafana/dashboard.json
scripts/check_names.py
docs/REPORT.md        docs/img/
local/                (gitignored) source preparation
data/source/          (gitignored)
data/prepared/        committed prepared Parquet
data/checkpoints/     (gitignored) demo checkpoint from make baseline
```

`scripts/check_names.py` fails if any tracked file contains a source-system or original-project term (list kept in the script, which itself only stores hashes of the terms).

## 9. Deliverables

1. Schema DDL and an ERD (i)
2. `make baseline`, `make demo`, `make demo-reset` (ii)
3. Grafana dashboard JSON, screenshots, GIF (iii)
4. `make outliers`, outlier evaluation table (iv)
5. `make model`, model evaluation (v)
6. `docs/REPORT.md` exported to PDF, and a 5-minute video (vi)

## 10. Out of scope

Kafka or Event Hubs, hosting the Python apps on Azure, dbt, the original project's finance logic, deep learning.
