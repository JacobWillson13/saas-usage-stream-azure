"""Outlier scorer (SPEC 6): rule checks and robust z-score (MAD) checks.

Windows use source_event_ts (original time); event_ts is copied onto flags for display.
"""

from __future__ import annotations

import math
import statistics
import threading
from collections import defaultdict, deque
from datetime import datetime, timedelta

import psycopg

from saas_stream.consumer import Consumer, get_offset, run_live
from saas_stream.db import stream_pool
from saas_stream.inject import hour_key, minute_key

Z_THRESHOLD = 3.5
# activity_spike threshold: best F1 against injected spikes (docs/results/outliers.md)
SPIKE_Z_THRESHOLD = 7.0
MAD_SCALE = 1.4826
USAGE_WINDOW = timedelta(days=28)
USAGE_MIN_DAYS = 7
USAGE_FLOOR = 1.0  # users: flat accounts have MAD 0, so allow +-3.5 users before flagging
RATE_WINDOW = timedelta(days=7)
RATE_MIN_PAYMENTS = 5
RATE_MIN_BASE = 0.02  # pooled 7-day failure rate is clamped to [2%, 98%] for the noise term
SESSION_MINUTES = 10  # trailing minutes of the take
SESSION_FLOOR = 2.0  # sessions; raised to sqrt(median) for busy accounts
VOLUME_MINUTES = 30
VOLUME_MIN_MINUTES = 10
VOLUME_FLOOR_SHARE = 0.2  # of the median rows per minute


def robust_z(x: float, history: list[float], floor: float) -> tuple[float, float]:
    """(z, median) with z = |x - median| / max(1.4826 * MAD, floor)."""
    median = statistics.median(history)
    mad = statistics.median([abs(v - median) for v in history])
    return abs(x - median) / max(MAD_SCALE * mad, floor), median


def payment_key(row: dict) -> tuple:
    return (
        row["account_id"], row["invoice_id"], row["amount"], row["status"],
        row["source_event_ts"],
    )  # fmt: skip


class OutlierScorer(Consumer):
    name = "outliers"
    tables = ["sessions", "usage_daily", "license_events", "invoices", "payments"]

    def __init__(self) -> None:
        self.clear()

    def clear(self) -> None:
        self.usage: dict[str, deque] = defaultdict(deque)
        self.payment_keys: set[tuple] = set()
        self.hours: dict[datetime, list[int]] = {}
        self.flagged_hours: set[str] = set()
        self.session_minutes: dict[str, dict[datetime, int]] = defaultdict(dict)
        self.flagged_minutes: set[str] = set()
        self.flags: list[dict] = []

    # State -----------------------------------------------------------------

    def restore(self, pool) -> None:
        self.clear()
        with pool.connection() as conn:
            offset = get_offset(conn, self.name)
            latest = conn.execute(
                "SELECT max(source_event_ts) FROM app.usage_daily WHERE ingest_id <= %s",
                (offset,),
            ).fetchone()[0]
            if latest is not None:
                rows = conn.execute(
                    "SELECT account_id, source_event_ts, active_users FROM app.usage_daily "
                    "WHERE ingest_id <= %s AND source_event_ts > %s "
                    "ORDER BY source_event_ts, ingest_id",
                    (offset, latest - USAGE_WINDOW - timedelta(days=1)),
                ).fetchall()
                for account_id, ts, value in rows:
                    self.usage[account_id].append((ts, value))
            payments = conn.execute(
                "SELECT account_id, invoice_id, amount, status, source_event_ts, is_test "
                "FROM app.payments WHERE ingest_id <= %s ORDER BY source_event_ts, ingest_id",
                (offset,),
            ).fetchall()
            for account_id, invoice_id, amount, status, ts, is_test in payments:
                self.payment_keys.add((account_id, invoice_id, amount, status, ts))
                if not is_test:
                    self.count_payment(ts, status)
            if payments:
                self.prune_hours(payments[-1][4])
            flagged = conn.execute(
                "SELECT flag_type, row_key FROM ops.outlier_flags "
                "WHERE flag_type IN ('failure_rate', 'session_rate')"
            ).fetchall()
            self.flagged_hours = {key for kind, key in flagged if kind == "failure_rate"}
            self.flagged_minutes = {key for kind, key in flagged if kind == "session_rate"}
            sessions = conn.execute(
                "SELECT account_id, source_event_ts FROM app.sessions WHERE ingest_id <= %s "
                "AND source_event_ts > (SELECT max(source_event_ts) FROM app.sessions "
                "WHERE ingest_id <= %s) - make_interval(mins => %s)",
                (offset, offset, SESSION_MINUTES + 1),
            ).fetchall()
            for account_id, ts in sessions:
                minute = ts.replace(second=0, microsecond=0)
                counts = self.session_minutes[account_id]
                counts[minute] = counts.get(minute, 0) + 1

    def count_payment(self, ts: datetime, status: str) -> list[int]:
        hour = ts.replace(minute=0, second=0, microsecond=0)
        counts = self.hours.setdefault(hour, [0, 0])
        counts[0] += 1
        counts[1] += status == "failed"
        return counts

    def prune_hours(self, now: datetime) -> None:
        cutoff = now - RATE_WINDOW - timedelta(hours=1)
        for hour in [h for h in self.hours if h < cutoff]:
            del self.hours[hour]

    # Checks ------------------------------------------------------------------

    def flag(self, table: str, key: str, row: dict, flag_type: str, method: str,
             score: float, reason: str) -> None:  # fmt: skip
        self.flags.append({
            "table_name": table, "row_key": key, "flag_type": flag_type, "method": method,
            "score": score, "reason": reason,
            "source_event_ts": row["source_event_ts"], "event_ts": row["event_ts"],
        })  # fmt: skip

    def process(self, table: str, row: dict) -> None:
        if table == "sessions":
            self.check_session_rate(row)
        elif table == "usage_daily":
            self.check_usage(row)
        elif table == "license_events":
            if row["licenses_used_after"] > row["licenses_held_after"]:
                self.flag(table, row["license_event_id"], row, "license_overuse", "rule", 1.0,
                          f"{row['licenses_used_after']} licenses used of "
                          f"{row['licenses_held_after']} held")  # fmt: skip
        elif table == "invoices":
            if row["total"] < 0:
                self.flag(table, row["invoice_id"], row, "invalid_amount", "rule", 1.0,
                          f"invoice total {row['total']} < 0")  # fmt: skip
        elif table == "payments":
            self.check_payment(row)

    def usage_z(self, row: dict) -> tuple[float, float, int] | None:
        """Robust z of active_users vs the account's trailing 28 days, then record the row.

        Returns (z, median, days of history), or None with under 7 days of history.
        """
        ts, x = row["source_event_ts"], row["active_users"]
        history = self.usage[row["account_id"]]
        while history and history[0][0] <= ts - USAGE_WINDOW:
            history.popleft()
        result = None
        if len(history) >= USAGE_MIN_DAYS:
            z, median = robust_z(x, [v for _, v in history], USAGE_FLOOR)
            result = (z, median, len(history))
        history.append((ts, x))
        return result

    def check_usage(self, row: dict) -> None:
        scored = self.usage_z(row)
        if scored is not None and scored[0] > SPIKE_Z_THRESHOLD:
            z, median, days = scored
            key = f"{row['account_id']}|{row['activity_date']}"
            reason = f"active_users {row['active_users']} vs median {median:g} over {days} days"
            self.flag("usage_daily", key, row, "activity_spike", "mad", z, reason)

    def check_session_rate(self, row: dict) -> None:
        """An account's sessions this minute vs its previous minutes of the take."""
        account_id, ts = row["account_id"], row["source_event_ts"]
        minute = ts.replace(second=0, microsecond=0)
        counts = self.session_minutes[account_id]
        for old in [m for m in counts if m < minute - timedelta(minutes=SESSION_MINUTES)]:
            del counts[old]
        counts[minute] = counts.get(minute, 0) + 1
        key = f"{account_id}|{minute_key(minute)}"
        day_start = minute.replace(hour=0, minute=0)  # live sessions start at the take's midnight
        history = [
            minute - timedelta(minutes=i)
            for i in range(1, SESSION_MINUTES + 1)
            if minute - timedelta(minutes=i) >= day_start
        ]
        if not history or key in self.flagged_minutes:
            return
        values = [counts.get(m, 0) for m in history]
        median = statistics.median(values)
        z, _ = robust_z(counts[minute], values, max(SESSION_FLOOR, math.sqrt(median)))
        if counts[minute] > median and z > Z_THRESHOLD:
            self.flagged_minutes.add(key)
            reason = (
                f"{counts[minute]} sessions this minute vs median {median:g} "
                f"over the previous {len(values)} minutes"
            )
            self.flag("sessions", key, row, "session_rate", "mad", z, reason)

    def check_payment(self, row: dict) -> None:
        key = row["payment_id"]
        if row["is_test"]:
            self.flag("payments", key, row, "test_mode_payment", "rule", 1.0, "test-mode payment")
        if row["invoice_id"] is None:
            self.flag("payments", key, row, "orphan_payment", "rule", 1.0, "no invoice")
        if row["amount"] <= 0:
            self.flag("payments", key, row, "invalid_amount", "rule", 1.0,
                      f"payment amount {row['amount']} <= 0")  # fmt: skip
        natural = payment_key(row)
        if natural in self.payment_keys:
            reason = "same account, invoice, amount, status, and time as an earlier payment"
            self.flag("payments", key, row, "duplicate_row", "rule", 1.0, reason)
        self.payment_keys.add(natural)
        if not row["is_test"]:
            self.check_failure_rate(row)

    def check_failure_rate(self, row: dict) -> None:
        ts = row["source_event_ts"]
        self.prune_hours(ts)
        n, failed = self.count_payment(ts, row["status"])
        bucket = hour_key(ts)
        if n < RATE_MIN_PAYMENTS or bucket in self.flagged_hours:
            return
        hour = ts.replace(minute=0, second=0, microsecond=0)
        window = [(c, f) for h, (c, f) in self.hours.items() if hour - RATE_WINDOW <= h < hour]
        history = [f / c for c, f in window]
        pooled = sum(f for _, f in window) / max(1, sum(c for c, _ in window))
        base = min(max(pooled, RATE_MIN_BASE), 1 - RATE_MIN_BASE)
        # Floor: the binomial noise of this hour's rate at the 7-day pooled rate, so a
        # single failure among a few payments does not flag but a burst does.
        rate = failed / n
        z, median = robust_z(rate, history or [0.0], math.sqrt(base * (1 - base) / n))
        if z > Z_THRESHOLD:
            self.flagged_hours.add(bucket)
            self.flag("payments", bucket, row, "failure_rate", "mad", z,
                      f"{failed} of {n} payments failed in the hour vs median rate "
                      f"{median:.2f} over 7 days")  # fmt: skip

    def after_poll(self, conn: psycopg.Connection) -> None:
        """Rows per wall minute vs the trailing 30 minutes (demo batches only)."""
        minutes = conn.execute(
            """
            WITH bounds AS (
                SELECT greatest(min(date_trunc('minute', finished_at)),
                                date_trunc('minute', now()) - interval '31 minutes') AS first,
                       date_trunc('minute', now()) - interval '1 minute' AS last
                FROM ops.ingest_log
                WHERE is_demo AND finished_at > now() - interval '32 minutes'
            )
            SELECT m.minute, coalesce(sum(l.rows_inserted), 0)
            FROM bounds, generate_series(bounds.first, bounds.last, interval '1 minute') m(minute)
            LEFT JOIN ops.ingest_log l
                ON l.is_demo AND date_trunc('minute', l.finished_at) = m.minute
            GROUP BY m.minute
            ORDER BY m.minute
            """
        ).fetchall()
        if len(minutes) < VOLUME_MIN_MINUTES + 1:
            return
        *history, (minute, rows) = minutes[-(VOLUME_MINUTES + 1):]
        values = [float(v) for _, v in history]
        median = statistics.median(values)
        z, _ = robust_z(float(rows), values, max(VOLUME_FLOOR_SHARE * median, 1.0))
        if z > Z_THRESHOLD:
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO ops.outlier_flags (table_name, row_key, flag_type, method,
                        score, reason, source_event_ts, event_ts)
                    VALUES ('ingest_log', %s, 'ingest_volume', 'mad', %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (minute.isoformat(), z,
                     f"{rows} rows in the minute vs median {median:g}", minute, minute),
                )  # fmt: skip

    def write(self, cur: psycopg.Cursor) -> int:
        if not self.flags:
            return 0
        cur.executemany(
            """
            INSERT INTO ops.outlier_flags (table_name, row_key, flag_type, method, score,
                reason, source_event_ts, event_ts)
            VALUES (%(table_name)s, %(row_key)s, %(flag_type)s, %(method)s, %(score)s,
                %(reason)s, %(source_event_ts)s, %(event_ts)s)
            ON CONFLICT DO NOTHING
            """,
            self.flags,
        )
        written = len(self.flags)
        self.flags = []
        return written


def main() -> None:
    from saas_stream.demo import install_stop, log

    stop = threading.Event()
    install_stop(stop)
    pool = stream_pool(max_size=2)
    try:
        run_live(pool, OutlierScorer(), stop, log)
    finally:
        pool.close()


if __name__ == "__main__":
    main()
