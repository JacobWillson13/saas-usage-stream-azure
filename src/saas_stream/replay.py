"""Live emitter: insert the take's events as their scheduled time comes due.

Every FLUSH_SECONDS, the events whose scheduled time has passed are inserted in one
transaction with event_ts = now() and is_demo = true, with the ingest log, their
injected anomalies, and the resume cursor. ingest_ids are allocated in event order,
so consumers reading by ingest_id see the take's order.
"""

from __future__ import annotations

import threading
import time

from psycopg.rows import dict_row

from saas_stream.live import LiveEvent, build_take
from saas_stream.rows import APP_TABLES, COLUMNS, DISPLAY_TIMES
from saas_stream.settings import Settings

FLUSH_SECONDS = 1.0


def flag_columns(table: str) -> list[str]:
    return ["is_injected", "is_generated"] if table == "payments" else ["is_injected"]


def insert_sql(table: str) -> str:
    """Display timestamps are now() plus the row's offset from its event time."""
    columns = COLUMNS[table] + flag_columns(table)
    values = ", ".join(
        "now() + %s" if column in DISPLAY_TIMES.get(table, []) else "%s" for column in columns
    )
    return (
        f"INSERT INTO app.{table} ({', '.join(columns)}, event_ts, source_event_ts, "
        f"is_backfill, is_demo, ingest_id) VALUES ({values}, now(), %s, false, true, %s) "
        f"ON CONFLICT DO NOTHING"
    )


def insert_values(table: str, row: dict) -> list:
    return [row[column] for column in COLUMNS[table]] + [
        bool(row.get(column, False)) for column in flag_columns(table)
    ]


class LiveEmitter:
    def __init__(self, pool, config: Settings, log) -> None:
        self.pool = pool
        self.config = config
        self.log = log

    def restore(self) -> None:
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            state = cur.execute("SELECT * FROM ops.replay_state").fetchone()
        if state is None or state["offset_days"] is None:
            raise RuntimeError("no baseline: run `make baseline` first")
        self.offset_days = state["offset_days"]
        self.day = state["source_cutoff"]
        self.cursor = state["cursor_index"]
        self.flushes = state["demo_batches"]
        started = time.monotonic()
        self.events: list[LiveEvent] = build_take(
            self.day, self.offset_days, self.config.demo_seed, self.config.demo_rate
        )
        self.log(
            f"emitter: take of {len(self.events)} events "
            f"({self.events[-1].t / 60:.1f} min at {self.config.demo_rate:g}/min) "
            f"built in {time.monotonic() - started:.1f} s; resuming at event {self.cursor}"
        )

    def write(self, batch: list[LiveEvent]) -> int:
        by_table: dict[str, list[list]] = {}
        anomalies = [anomaly for event in batch for anomaly in event.anomalies]
        with self.pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            inserted = 0
            if batch:
                cur.execute(
                    "SELECT nextval('app.ingest_id_seq') FROM generate_series(1, %s)",
                    (len(batch),),
                )
                ids = sorted(value for (value,) in cur.fetchall())
                for ingest_id, event in zip(ids, batch, strict=True):
                    values = insert_values(event.table, event.row)
                    by_table.setdefault(event.table, []).append(
                        values + [event.row["source_event_ts"], ingest_id]
                    )
                cur.execute(
                    """
                    INSERT INTO ops.ingest_log (min_source_ts, max_source_ts, demo_batch, is_demo)
                    VALUES (%s, %s, %s, true) RETURNING batch_id
                    """,
                    (
                        min(e.row["source_event_ts"] for e in batch),
                        max(e.row["source_event_ts"] for e in batch),
                        self.flushes + 1,
                    ),
                )
                batch_id = cur.fetchone()[0]
                for table in APP_TABLES:  # parents first, so FKs hold inside a flush
                    if table not in by_table:
                        continue
                    cur.executemany(insert_sql(table), by_table[table])
                    count = cur.rowcount
                    inserted += count
                    cur.execute(
                        "INSERT INTO ops.ingest_log_tables (batch_id, table_name, rows_inserted) "
                        "VALUES (%s, %s, %s)",
                        (batch_id, table, count),
                    )
                cur.execute(
                    "UPDATE ops.ingest_log SET rows_inserted = %s, "
                    "finished_at = clock_timestamp() WHERE batch_id = %s",
                    (inserted, batch_id),
                )
            if anomalies:
                cur.executemany(
                    """
                    INSERT INTO ops.injected_anomalies (anomaly_type, table_name, row_key,
                        source_event_ts, event_ts, scripted, is_demo)
                    VALUES (%(anomaly_type)s, %(table_name)s, %(row_key)s, %(source_event_ts)s,
                        now(), %(scripted)s, true)
                    """,
                    anomalies,
                )
            cursor = self.cursor + len(batch)
            cur.execute(
                """
                UPDATE ops.replay_state
                SET status = 'running', cursor_index = %s, demo_batches = %s,
                    cursor_ts = coalesce(%s, cursor_ts), updated_at = now()
                """,
                (
                    cursor,
                    self.flushes + (1 if batch else 0),
                    batch[-1].row["source_event_ts"] if batch else None,
                ),
            )
        self.cursor = cursor
        self.flushes += 1 if batch else 0
        for anomaly in anomalies:
            if anomaly["scripted"]:
                self.log(f"emitter: scripted {anomaly['anomaly_type']} at event {self.cursor}")
        return inserted

    def set_status(self, status: str) -> None:
        with self.pool.connection() as conn:
            conn.execute("UPDATE ops.replay_state SET status = %s, updated_at = now()", (status,))

    def run(self, stop: threading.Event) -> None:
        self.restore()
        resume_at = self.events[self.cursor - 1].t if self.cursor else 0.0
        take_start = time.monotonic() - resume_at
        next_flush = time.monotonic()
        last_log = time.monotonic()
        emitted = 0
        while not stop.is_set():
            stop.wait(max(0.0, next_flush - time.monotonic()))
            if stop.is_set():
                break
            next_flush += FLUSH_SECONDS
            due = time.monotonic() - take_start
            end = self.cursor
            while end < len(self.events) and self.events[end].t <= due:
                end += 1
            emitted += self.write(self.events[self.cursor : end])
            if time.monotonic() - last_log >= 30:
                self.log(f"emitter: {emitted} events this run, at event {self.cursor}")
                last_log = time.monotonic()
            if self.cursor >= len(self.events):
                self.log("emitter: take complete")
                self.set_status("finished")
                stop.set()
                return
        self.set_status("stopped")
        self.log(f"emitter: stopped at event {self.cursor} ({emitted} events this run)")
