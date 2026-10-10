# AGENTS.md

Project: saas-usage-stream-azure. A streaming analytics pipeline on Azure Database for PostgreSQL with Azure Managed Grafana, an outlier scorer, and an online trial-conversion model. Due 2026-10-08, so favor working and simple over clever.

This file is the single source of agent instructions. CLAUDE.md imports it; Codex reads it directly.

## Workflow

- Read SPEC.md and PLAN.md before starting.
- Do one PLAN task at a time. Stop when its acceptance criteria pass, tick the box in PLAN.md, and report what changed and how you verified it.
- Skip tasks marked [J] (Jake does them by hand). Say so when one is blocking you.
- If SPEC and the data disagree, stop and report the conflict instead of guessing.

## Rules

- Python 3.12, uv, ruff. psycopg 3 with a pool. Bulk loads use COPY. Read Parquet with DuckDB.
- Credentials only from `.env`. Never print, log, or commit them. Always `sslmode=require`.
- Raw source files in `data/source/` are read only by code in `local/` (gitignored). Everything committed reads `data/prepared/`.
- No source-system or original-project terms in any tracked file, including comments, docs, and commit messages. `scripts/check_names.py` must pass before every commit.
- The model never sees a label before its simulated time, and accounts never carry a final plan or trial outcome.
- Every consumer is restartable from `ops.consumer_offsets` and writes idempotently.
- The server is a B1ms. Keep queries indexed, batches small, and connections pooled (max 5 per app).
- Grafana reads only `dash` views through the `grafana_ro` role.
- Never commit `.env`, `data/source/`, `local/`, or `.DS_Store`.
- No em dashes in prose.

## Commands

- `make setup`: install dependencies with uv
- `make check`: ruff and check_names
- `make db-init`: create schemas, tables, roles, views (idempotent)
- `make backfill`: load shifted history before SOURCE_CUTOFF (no scoring)
- `make baseline`: load history, score it with the outlier scorer and model, save the demo checkpoint
- `make demo`: run the live emitter, scorer, and model together (Ctrl-C stops all three)
- `make demo-reset`: remove demo rows and outputs, restore the checkpoint
- `make outliers`: run the outlier scorer
- `make model`: run the online model
- `make evaluate-outliers`, `make evaluate-model`: write results to `docs/results/`
- `make reset`: drop and recreate `app` and `ops` (asks for confirmation)
