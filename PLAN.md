# PLAN: saas-usage-stream-azure

Due 2026-10-08. Work top to bottom. A task is done when its acceptance criteria pass. **[J]** = Jake does it by hand in the Azure portal or terminal. **[M]** must ship, **[S]** should ship, **[C]** could ship.

**Cut order if behind:** 4.4 Hoeffding tree, 3.4 stall injection, 5.3 GIF, 4.3 warm start. Never cut [M].

## Day 1 (Oct 6): data, database, stream, dashboard

- [ ] 0.1 [J][M] Azure networking on `dsa508`: add your current IP, and enable "Allow public access from any Azure service" so Grafana can connect.
- [ ] 0.2 [J][M] Create `.env` from `.env.example` (host, admin user, password, `sslmode=require`, `RENAME_SALT`). Never commit it.
- [x] 1.1 [M] Profile the source: distinct values of every coded column and the payment metadata format. Write `config/mappings.yml` and fill gaps in SPEC 3.3.
  - Accept: every distinct value is mapped; the profile output is saved to `local/profile.txt`, not committed.
- [x] 1.2 [M] `local/prepare.py` (gitignored): source to `data/prepared/*.parquet` per SPEC 3.
  - Accept: row counts printed per table; no ID, column, or value from the source naming survives (`scripts/check_names.py --data data/prepared` passes); a trial account's prepared row has no final plan or trial outcome.
- [x] 1.3 [M] Scaffold: uv project (Python 3.12, psycopg[binary,pool], duckdb, river, pyyaml, python-dotenv), ruff, Makefile, `scripts/check_names.py`, pre-commit hook running it.
- [x] 1.4 [M] DDL and roles per SPEC 4.2. `make db-init` is idempotent.
  - Accept: `\dt app.*` shows 9 tables with FKs; `grafana_ro` can read `dash` and nothing else.
- [x] 1.5 [M] `make backfill`: COPY all rows before the cutoff, parents first, shifted by DEMO_DATE - SOURCE_CUTOFF days (originals kept in `source_event_ts`).
  - Accept: counts match prepared files filtered by date plus injected rows; runs in under 15 minutes.
- [x] 1.6 [M] Live demo (SPEC 4.1). `make baseline`: history before SOURCE_CUTOFF shifted to end the day before DEMO_DATE, random injection at the normal rate, scorer and model in fast mode, checkpoint in `data/checkpoints/`. `make demo`: live emitter generating sessions, feature events, and single rows from DEMO_DATE's source records in a seeded random order at DEMO_RATE events/min with exponential gaps and `event_ts = now()`, plus the scorer and model in one process; scripted anomalies at 60, 120, 180, 240 s plus low-rate random injection; Ctrl-C stops all three. `make demo-reset`: refuse while running; delete demo rows and post-checkpoint ops rows, restore offsets and cursor.
  - Accept: a 5-minute demo produces about 2,500 events with event_ts within the take; all four scripted anomalies are flagged; trial predictions update during the take; reset (idempotent, under 30 s) then demo reproduces the same events.
- [x] 1.7 [M] `dash` views for SPEC 5 rows 1 to 3: live views over the last 15 minutes by second and minute, daily trend views that union history with today's live events, billing views, and the demo state (status and time into the take).
- [ ] 1.8 [J][M] Create Azure Managed Grafana (Standard, same region), add the PostgreSQL data source with `grafana_ro`, build rows 1 to 3 from the `dash` views, 5 s refresh. Export JSON to `grafana/dashboard.json`.
  - Accept: panels move while the replayer runs.

## Day 2 (Oct 7): outliers, model, write-up

- [x] 3.1 [M] Consumer framework: watermark per consumer in `ops.consumer_offsets`, poll every 5 s, idempotent writes.
- [x] 3.2 [M] Rule checks and MAD checks (SPEC 6) in `make outliers`.
- [x] 3.3 [M] Anomaly injection in the replayer with `ops.injected_anomalies`.
- [ ] ~~3.4 [S] Ingest stall injection and the volume check.~~ **Cut** (2026-10-06). The `ingest_volume` check exists; stall injection was not built.
- [x] 3.5 [M] `make evaluate-outliers`: precision, recall, median delay per type; output a markdown table to `docs/results/outliers.md`.
- [x] 4.1 [M] Incremental features and label logic (SPEC 7); state rebuilt from Postgres on restart.
- [x] 4.2 [M] Logistic regression and points baseline, prequential loop, `ops.predictions`, `ops.model_metrics`.
- [x] 4.3 [S] Warm start on backfill trials.
- [ ] ~~4.4 [C] Hoeffding adaptive tree comparison.~~ **Cut** (2026-10-06).
- [x] 4.5 [M] `make evaluate-model`: warm-start and live AUC, log loss, top-decile lift, a chart of rolling AUC with 2026-04-08 marked; output to `docs/results/model.md` and `docs/img/`.
- [x] 4.6 [M] `dash` views and Grafana rows 4 and 5; re-export the dashboard JSON.

## Day 3 morning (Oct 8): ship

- [ ] 5.1 [M] `docs/REPORT.md`: problem, architecture, schema and ERD, ingestion, dashboard, outliers, model, limitations, data provenance sentence. Export to PDF.
- [ ] 5.2 [M] README: one architecture image, one dashboard screenshot, quick start from the Release asset, Azure resources used, cost note.
- [ ] 5.3 [S] Dashboard GIF.
- [ ] 5.4 [J][M] 5-minute video during a live replay (script below).
- [ ] 5.5 [M] Publish `data/prepared/` as a Release asset; secrets scan (`gitleaks` or `git log -p | grep`); crop subscription IDs from screenshots; set the repo public.
- [ ] 5.6 [J][M] After grading: stop the server, delete Managed Grafana.

### Video script (5 minutes)

Before recording: `make baseline` on the recording day (or `make demo-reset` if it already ran), Grafana open on the dashboard with the 15-minute range and 5 s refresh, and a terminal ready. Start the take at 0:15 so the scripted anomalies land at 1:15, 2:15, 3:15, and 4:15.

- 0:00 The product and the three questions: what is happening now, what looks wrong, which trials will convert. Say it once: synthetic data, shifted so history ends yesterday.
- 0:15 Run `make demo`. "Demo elapsed" starts counting and the live row starts moving: events per second, sessions, feature events, signups and payments.
- 0:30 Architecture and the ERD while the stream warms up: baseline history plus live events, one sequence for insert order, consumers with watermarks.
- 1:15 Session spike (scripted at 60 s): a flag appears in the Outliers row about 3 seconds later; read its reason in the latest-flags table.
- 1:35 Usage trends row: history from the daily tables plus today's live sessions.
- 2:15 Duplicate payment (120 s) flagged. Billing row: business panels exclude injected rows (7-day failure rate 4.0%, not 24.8%).
- 2:40 Trial model row: top open trials update as sessions arrive. State the result plainly: scored at day 7, the model is well calibrated but ranks only slightly better than chance (holdout AUC 0.54).
- 3:15 Negative invoice (180 s) flagged. Outlier evaluation: recall and precision table, and the activity-spike threshold choice (z > 7, best F1).
- 3:45 How the AUC of 0.71 turned out to be label timing, and why scoring at day 7 fixes it.
- 4:15 Failure burst (240 s) flagged; the failed-payments stat jumps.
- 4:30 Limitations: synthetic data, a small holdout, precision against injected labels only.
- 4:50 Ctrl-C (all three components stop), then `make demo-reset` in about 3 seconds: the next take replays exactly.
