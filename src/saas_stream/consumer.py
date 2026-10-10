"""Consumer framework (SPEC 6 and 7): watermarks, polling, and idempotent writes.

A consumer reads new app rows in ingest_id order (one sequence across all tables),
processes them in memory, and writes its outputs and its new watermark in one
transaction. Output tables have unique keys and inserts use ON CONFLICT DO NOTHING,
so a replayed poll writes nothing twice.

Fast mode (baseline) reads the loaded history in (source_event_ts, rank, ingest_id)
order, which is the order the demo streamer inserts rows in, and calls the same
process() code.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from datetime import datetime, timedelta

import psycopg
from psycopg.rows import dict_row

from saas_stream.rows import APP_TABLES, RANK

POLL_SECONDS = 5.0
LIVE_CHUNK_IDS = 50_000


class Consumer:
    """Subclasses set name and tables, and implement process() and write()."""

    name: str = ""
    tables: list[str] = []

    def process(self, table: str, row: dict) -> None:
        raise NotImplementedError

    def write(self, cur: psycopg.Cursor) -> int:
        """Write and clear buffered outputs. Returns rows written."""
        raise NotImplementedError

    def restore(self, pool) -> None:
        """Rebuild in-memory state up to the committed watermark (start and after errors)."""
        raise NotImplementedError

    def after_poll(self, conn: psycopg.Connection) -> None:
        """Optional hook for checks that read the database rather than rows."""


def get_offset(conn: psycopg.Connection, name: str) -> int:
    row = conn.execute(
        "SELECT last_ingest_id FROM ops.consumer_offsets WHERE consumer_name = %s", (name,)
    ).fetchone()
    return row[0] if row else 0


def set_offset(cur: psycopg.Cursor, name: str, ingest_id: int) -> None:
    cur.execute(
        """
        INSERT INTO ops.consumer_offsets (consumer_name, last_ingest_id, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (consumer_name)
        DO UPDATE SET last_ingest_id = EXCLUDED.last_ingest_id, updated_at = now()
        """,
        (name, ingest_id),
    )


def max_ingest_id(conn: psycopg.Connection) -> int:
    """Highest committed ingest_id. One writer commits whole batches, so no gaps hide below."""
    parts = " UNION ALL ".join(f"SELECT max(ingest_id) AS m FROM app.{t}" for t in APP_TABLES)
    return conn.execute(f"SELECT coalesce(max(m), 0) FROM ({parts}) s").fetchone()[0]


def fetch_range(
    conn: psycopg.Connection, tables: list[str], after_id: int, upto_id: int
) -> list[tuple[str, dict]]:
    rows: list[tuple[int, str, dict]] = []
    with conn.cursor(row_factory=dict_row) as cur:
        for table in tables:
            cur.execute(
                f"SELECT * FROM app.{table} WHERE ingest_id > %s AND ingest_id <= %s",
                (after_id, upto_id),
            )
            rows += [(row["ingest_id"], table, row) for row in cur.fetchall()]
    rows.sort(key=lambda item: item[0])
    return [(table, row) for _, table, row in rows]


def iter_history(
    conn: psycopg.Connection, tables: list[str], upto_id: int, window_days: int = 31
) -> Iterator[tuple[str, dict]]:
    """Rows with ingest_id <= upto_id in (source_event_ts, rank, ingest_id) order."""
    parts = " UNION ALL ".join(
        f"SELECT min(source_event_ts) AS lo, max(source_event_ts) AS hi FROM app.{t}"
        for t in tables
    )
    lo, hi = conn.execute(f"SELECT min(lo), max(hi) FROM ({parts}) s").fetchone()
    if lo is None:
        return
    start = lo
    while start <= hi:
        end = start + timedelta(days=window_days)
        rows: list[tuple] = []
        with conn.cursor(row_factory=dict_row) as cur:
            for table in tables:
                cur.execute(
                    f"SELECT * FROM app.{table} WHERE source_event_ts >= %s "
                    f"AND source_event_ts < %s AND ingest_id <= %s",
                    (start, end, upto_id),
                )
                rows += [
                    (row["source_event_ts"], RANK[table], row["ingest_id"], table, row)
                    for row in cur.fetchall()
                ]
        rows.sort(key=lambda item: item[:3])
        for *_, table, row in rows:
            yield table, row
        start = end


def run_fast(pool, consumers: list[Consumer], upto_id: int) -> None:
    """Run consumers over history (baseline) and set their watermarks to upto_id."""
    tables = sorted({t for c in consumers for t in c.tables}, key=APP_TABLES.index)
    wanted = {c.name: set(c.tables) for c in consumers}
    with pool.connection() as conn:
        for table, row in iter_history(conn, tables, upto_id):
            for consumer in consumers:
                if table in wanted[consumer.name]:
                    consumer.process(table, row)
        with conn.transaction(), conn.cursor() as cur:
            for consumer in consumers:
                consumer.write(cur)
                set_offset(cur, consumer.name, upto_id)


def poll_once(pool, consumer: Consumer) -> int:
    """Process everything committed since the watermark. Returns rows processed."""
    processed = 0
    with pool.connection() as conn:
        offset = get_offset(conn, consumer.name)
        top = max_ingest_id(conn)
        while offset < top:
            upto = min(top, offset + LIVE_CHUNK_IDS)
            rows = fetch_range(conn, consumer.tables, offset, upto)
            for table, row in rows:
                consumer.process(table, row)
            with conn.transaction(), conn.cursor() as cur:
                consumer.write(cur)
                set_offset(cur, consumer.name, upto)
            processed += len(rows)
            offset = upto
        consumer.after_poll(conn)
    return processed


def run_live(pool, consumer: Consumer, stop: threading.Event, log) -> None:
    """Poll every POLL_SECONDS until stop is set. Each poll commits outputs and watermark."""
    needs_restore = True
    log(f"{consumer.name}: polling every {POLL_SECONDS:g} s")
    while not stop.is_set():
        started = datetime.now()
        try:
            if needs_restore:
                consumer.restore(pool)
                needs_restore = False
            processed = poll_once(pool, consumer)
            if processed:
                log(f"{consumer.name}: processed {processed} rows")
        except psycopg.OperationalError as error:
            # A failed write rolled back; rebuild state at the committed watermark.
            log(f"{consumer.name}: database error {type(error).__name__}, will restore")
            needs_restore = True
        elapsed = (datetime.now() - started).total_seconds()
        stop.wait(max(0.0, POLL_SECONDS - elapsed))
    log(f"{consumer.name}: stopped")
