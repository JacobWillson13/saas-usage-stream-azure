"""Load the baseline: every prepared row before SOURCE_CUTOFF, shifted and with injection.

Every date and timestamp moves forward by offset = DEMO_DATE - SOURCE_CUTOFF days, so
history ends the day before DEMO_DATE. The original event time is kept in
source_event_ts. Tables load parents first with COPY.
"""

from __future__ import annotations

import time
from datetime import datetime

import duckdb
import psycopg

from saas_stream.db import stream_pool
from saas_stream.inject import BASELINE_KINDS, Injector, injection_rates
from saas_stream.rows import (
    APP_TABLES,
    DATA_COLUMNS,
    LOAD_COLUMNS,
    TABLE_NAMES,
    shift_row,
    source_rows,
)
from saas_stream.settings import BASELINE_INJECT_RATE, INJECT_SEED, Settings, parquet_path, settings

APP_TRUNCATE = ", ".join(f"app.{table}" for table in reversed(APP_TABLES))
OPS_TRUNCATE = (
    "ops.ingest_log, ops.ingest_log_tables, ops.consumer_offsets, ops.injected_anomalies, "
    "ops.outlier_flags, ops.predictions, ops.trial_outcomes, ops.model_metrics"
)


def cutoff_ts(config: Settings) -> datetime:
    return datetime.combine(config.source_cutoff, datetime.min.time())


def source_counts(end: datetime) -> dict[str, int]:
    con = duckdb.connect()
    return {
        table: con.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE event_ts < ?",
            [str(parquet_path(table)), end],
        ).fetchone()[0]
        for table in TABLE_NAMES
    }


def copy_table(
    cur: psycopg.Cursor, table: str, end: datetime, offset: int, injector: Injector
) -> tuple[int, list[dict]]:
    columns = LOAD_COLUMNS[table]
    data_columns = DATA_COLUMNS[table]
    anomalies: list[dict] = []
    copied = 0
    with cur.copy(f"COPY app.{table} ({', '.join(columns)}) FROM STDIN") as copy:
        for _, source in source_rows(table, end=end):
            row = shift_row(source, offset)
            extra, injected = injector.random(table, row)
            anomalies += injected
            for out in [row, *extra]:
                copy.write_row(
                    [out[c] for c in data_columns]
                    + [out["event_ts"], out["source_event_ts"], True, False]
                    + [out.get("is_injected", False)]
                )
                copied += 1
    cur.execute(
        """
        WITH batch AS (
            INSERT INTO ops.ingest_log
                (started_at, finished_at, rows_inserted, min_source_ts, max_source_ts,
                 is_backfill)
            SELECT now(), clock_timestamp(), count(*), min(source_event_ts),
                   max(source_event_ts), true
            FROM app.""" + table + """
            RETURNING batch_id, rows_inserted
        )
        INSERT INTO ops.ingest_log_tables (batch_id, table_name, rows_inserted)
        SELECT batch_id, %s, rows_inserted FROM batch
        """,
        (table,),
    )
    return copied, anomalies


def load_baseline(pool, config: Settings) -> dict[str, int]:
    """Wipe app and ops data and load the shifted baseline. Returns rows per table."""
    end = cutoff_ts(config)
    counts = source_counts(end)
    rates = injection_rates(counts, BASELINE_INJECT_RATE, BASELINE_KINDS)
    injector = Injector(rates, INJECT_SEED)
    copied: dict[str, int] = {}
    anomalies: list[dict] = []
    with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute(f"TRUNCATE {APP_TRUNCATE}, {OPS_TRUNCATE} CASCADE")
        for table in TABLE_NAMES:
            started = time.monotonic()
            copied[table], injected = copy_table(cur, table, end, config.offset_days, injector)
            anomalies += injected
            print(
                f"  {table}: source={counts[table]} loaded={copied[table]} "
                f"injected={len(injected)} ({time.monotonic() - started:.0f} s)",
                flush=True,
            )
        cur.executemany(
            """
            INSERT INTO ops.injected_anomalies
                (anomaly_type, table_name, row_key, source_event_ts, event_ts, scripted)
            VALUES (%(anomaly_type)s, %(table_name)s, %(row_key)s, %(source_event_ts)s,
                    %(event_ts)s, %(scripted)s)
            """,
            anomalies,
        )
        cur.execute(
            """
            INSERT INTO ops.replay_state
                (singleton, status, demo_date, source_cutoff, offset_days, demo_batches)
            VALUES (true, 'baseline', %s, %s, %s, 0)
            ON CONFLICT (singleton) DO UPDATE
            SET status = EXCLUDED.status, demo_date = EXCLUDED.demo_date,
                source_cutoff = EXCLUDED.source_cutoff, offset_days = EXCLUDED.offset_days,
                cursor_index = 0, cursor_ts = NULL, demo_batches = 0,
                updated_at = now()
            """,
            (config.demo_date, config.source_cutoff, config.offset_days),
        )
        for table in TABLE_NAMES:
            cur.execute(f"SELECT count(*) FROM app.{table}")
            actual = cur.fetchone()[0]
            if actual != copied[table]:
                raise RuntimeError(f"{table}: copied {copied[table]} but found {actual}")
    by_type: dict[str, int] = {}
    for anomaly in anomalies:
        by_type[anomaly["anomaly_type"]] = by_type.get(anomaly["anomaly_type"], 0) + 1
    print(f"  injected anomalies: {by_type}", flush=True)
    return copied


def main() -> None:
    config = settings()
    started = time.monotonic()
    print(
        f"loading rows before {config.source_cutoff} shifted +{config.offset_days} days "
        f"(DEMO_DATE {config.demo_date})",
        flush=True,
    )
    pool = stream_pool(max_size=1)
    try:
        load_baseline(pool, config)
    finally:
        pool.close()
    print(f"load runtime_seconds={time.monotonic() - started:.1f}")


if __name__ == "__main__":
    main()
