# saas-usage-stream-azure

A streaming analytics pipeline for a synthetic B2B SaaS product, built on Azure Database for PostgreSQL and Azure Managed Grafana. A baseline loads about 1.25 million rows of history. A live simulator then streams real-time sessions, feature events, signups, and payments, at about 500 events per minute. Alongside the stream run an outlier detector that flags data errors and unusual behavior within seconds, and an online model that predicts which free trials will convert. A demo take is seeded and can be reset in a few seconds, so the same five minutes can be replayed for a recording. The data is derived from a separate synthetic simulation, with IDs re-keyed and timestamps shifted; no real customer data is involved. The full write-up is in [docs/REPORT.md](docs/REPORT.md).

## Architecture

```
data/prepared/*.parquet  (committed, re-keyed synthetic data)
   |                         \
make baseline                 make demo: live emitter
(COPY history, shifted,       (events generated from one source day,
 injected anomalies)           ~500/min, scripted + random anomalies)
   \                         /
 Azure Database for PostgreSQL Flexible Server (B1ms, PostgreSQL 18)
   app (streamed tables)   ops (state and outputs)   dash (views)
   |                         |                          |
Azure Managed Grafana      outlier scorer             trial model (River)
(dash views, grafana_ro,   -> ops.outlier_flags       -> ops.predictions,
 5 s refresh)                                            ops.trial_outcomes
```

The emitter, scorer, and model run locally as one process. Only the database and Grafana run on Azure.

Schema: [ERD](docs/img/erd.png), [DDL](sql/ddl/schema.sql), [dashboard views](sql/views/dash.sql).

## Quick start

Requirements: Python 3.12, [uv](https://docs.astral.sh/uv/), and an Azure Database for PostgreSQL server you can reach.

```sh
cp .env.example .env        # fill in host, admin user, passwords; keep sslmode=require
make setup                  # install dependencies
make db-init                # create the database, schemas, tables, roles, and views
make baseline               # load history and score it, save the demo checkpoint (~3.5 min)
make demo                   # stream live events with the scorer and model; Ctrl-C stops all three
make demo-reset             # remove the take and restore the checkpoint (~3 s)
```

Set `DEMO_DATE` in `.env` to the recording day (default: today) and rerun `make baseline`, so history ends the day before. `make evaluate-outliers` and `make evaluate-model` write the results to [docs/results/](docs/results/). Import `grafana/dashboard.json` into Grafana with a PostgreSQL data source that logs in as `grafana_ro`.

## Azure resources

| Resource | Configuration |
|---|---|
| Azure Database for PostgreSQL Flexible Server | Burstable B1ms (1 vCore, 2 GiB), PostgreSQL 18, 32 GiB storage, Canada Central, no high availability, 7-day backups |
| Azure Managed Grafana | Standard tier, Grafana 13, Canada Central |

Grafana connects to the server through the "Allow public access from any Azure service" setting. The local apps connect from an allowed client IP over TLS.

## Cost

Both resources bill by the hour while they exist. The B1ms server is the smallest burstable tier: compute bills only while the server is running, but storage and backups bill even when it is stopped. Managed Grafana Standard bills per instance hour, plus active users. To keep costs down, stop the server between sessions (`az postgres flexible-server stop`), and delete the Grafana instance after grading. The dashboard JSON and the screenshots in `docs/img/` preserve the dashboard. Check current prices in the Azure pricing calculator; this project ran on a student subscription.
