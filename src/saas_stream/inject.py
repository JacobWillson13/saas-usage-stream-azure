"""Anomaly injection (SPEC 6), shared by the baseline load and the demo streamer.

Every choice is a hash of the seed and a row key, so the same input always gets the
same anomalies. Anomalies either modify the row about to be inserted or add rows
after it; each one is logged for evaluation with the row key its flag will carry.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict, deque
from datetime import datetime, timedelta

from saas_stream.rows import row_key

ANOMALY_TABLE = {
    "activity_spike": "usage_daily",
    "session_spike": "sessions",
    "negative_invoice": "invoices",
    "duplicate_payment": "payments",
    "failure_burst": "payments",
}
# The flag type each anomaly should raise (used by evaluation).
ANOMALY_FLAG = {
    "activity_spike": "activity_spike",
    "session_spike": "session_rate",
    "negative_invoice": "invalid_amount",
    "duplicate_payment": "duplicate_row",
    "failure_burst": "failure_rate",
}
BASELINE_KINDS = ["activity_spike", "negative_invoice", "duplicate_payment", "failure_burst"]
# A take spans one source hour, so the hourly failure-rate check fires at most once
# per take: failure bursts are scripted only, never random, in the live demo.
LIVE_KINDS = ["session_spike", "negative_invoice", "duplicate_payment"]
BURST_SIZE = 20
SPIKE_FACTOR = 10
SESSION_SPIKE_MIN = 20
SESSION_SPIKE_MAX = 60
SPIKE_WINDOW = timedelta(days=28)
SPIKE_MIN_DAYS = 7


def digest(*parts: object) -> str:
    return hashlib.md5(":".join(str(part) for part in parts).encode()).hexdigest()


def unit_hash(*parts: object) -> float:
    return int(digest(*parts)[:13], 16) / 16**13


def hour_key(ts: datetime) -> str:
    return f"{ts:%Y-%m-%dT%H}"


def minute_key(ts: datetime) -> str:
    return f"{ts:%Y-%m-%dT%H:%M}"


class Injector:
    def __init__(self, rates: dict[str, float], seed: str, roster=None) -> None:
        self.rates = rates
        self.seed = seed
        self.roster = roster  # live only: users, devices, and activity per account
        self.usage_history: dict[str, deque] = defaultdict(deque)
        self.last_invoice: dict | None = None
        self.last_payment: dict | None = None

    # State -----------------------------------------------------------------

    def observe(self, table: str, row: dict) -> None:
        if table == "usage_daily":
            self.usage_history[row["account_id"]].append(row["source_event_ts"])
        elif table == "invoices" and row["total"] > 0:
            self.last_invoice = row
        elif table == "payments":
            self.last_payment = row

    def spike_eligible(self, row: dict) -> bool:
        history = self.usage_history[row["account_id"]]
        while history and history[0] <= row["source_event_ts"] - SPIKE_WINDOW:
            history.popleft()
        return row["active_users"] >= 1 and len(history) >= SPIKE_MIN_DAYS

    # Anomalies ----------------------------------------------------------------

    def record(self, kind: str, key: str, row: dict, scripted: bool) -> dict:
        return {
            "anomaly_type": kind,
            "table_name": ANOMALY_TABLE[kind],
            "row_key": key,
            "source_event_ts": row["source_event_ts"],
            "event_ts": row["event_ts"],
            "scripted": scripted,
        }

    def spike(self, row: dict, scripted: bool) -> list[dict]:
        row["active_users"] *= SPIKE_FACTOR
        row["is_injected"] = True
        return [self.record("activity_spike", row_key("usage_daily", row), row, scripted)]

    def negate_invoice(self, row: dict, scripted: bool) -> list[dict]:
        row["total"] = -row["total"]
        row["total_usd"] = -row["total_usd"]
        row["is_injected"] = True
        return [self.record("negative_invoice", row["invoice_id"], row, scripted)]

    def duplicate_payment(self, row: dict, tag: str, scripted: bool) -> tuple[dict, list[dict]]:
        copy = dict(
            row,
            payment_id="pay_" + digest(self.seed, "dup", tag, row["payment_id"])[:12],
            is_injected=True,
            is_generated=False,
        )
        return copy, [self.record("duplicate_payment", copy["payment_id"], copy, scripted)]

    def session_spike(
        self, account_id: str, at: datetime, tag: str, scripted: bool
    ) -> tuple[list[dict], list[dict]]:
        """A burst of sessions for one account within one source minute."""
        count = self.roster.active_users.get(account_id, 1) * SPIKE_FACTOR
        count = max(SESSION_SPIKE_MIN, min(SESSION_SPIKE_MAX, count))
        rows = []
        for i in range(count):
            key = digest(self.seed, "spike", tag, i)
            user_id, device_id = self.roster.pick(account_id, key)
            ts = at + timedelta(microseconds=i + 1)
            rows.append({
                "session_id": "ses_" + key[:12], "account_id": account_id,
                "user_id": user_id, "device_id": device_id,
                "started_at": self.roster.display(ts), "duration_s": 60,
                "is_injected": True,
                "source_event_ts": ts, "event_ts": None,
            })  # fmt: skip
        anomaly = self.record("session_spike", f"{account_id}|{minute_key(at)}", rows[0], scripted)
        return rows, [anomaly]

    def failure_burst(
        self, template: dict, at: datetime, event_ts: datetime, tag: str, scripted: bool
    ) -> tuple[list[dict], list[dict]]:
        rows = []
        for k in range(1, BURST_SIZE + 1):
            rows.append(
                dict(
                    template,
                    payment_id="pay_" + digest(self.seed, "burst", tag, k)[:12],
                    status="failed",
                    paid=False,
                    captured=False,
                    refunded=False,
                    is_test=False,
                    is_injected=True,
                    is_generated=False,
                    amount_refunded=0,
                    amount_refunded_usd=0,
                    source_event_ts=at + timedelta(microseconds=k),
                    event_ts=None if event_ts is None else event_ts + timedelta(microseconds=k),
                )
            )
        anomaly = self.record("failure_burst", hour_key(at), rows[0], scripted)
        return rows, [anomaly]

    # Entry points --------------------------------------------------------------

    def random(self, table: str, row: dict) -> tuple[list[dict], list[dict]]:
        """Maybe inject into `row` (modified in place). Returns (extra rows, anomalies)."""
        key = row_key(table, row)
        hit = lambda kind: unit_hash(self.seed, kind, key) < self.rates.get(kind, 0)  # noqa: E731
        extra: list[dict] = []
        anomalies: list[dict] = []
        if row.get("is_injected"):
            pass
        elif table == "usage_daily" and self.spike_eligible(row) and hit("activity_spike"):
            anomalies = self.spike(row, False)
        elif table == "invoices" and row["total"] > 0 and hit("negative_invoice"):
            anomalies = self.negate_invoice(row, False)
        elif table == "sessions" and self.roster is not None and hit("session_spike"):
            extra, anomalies = self.session_spike(
                row["account_id"], row["source_event_ts"], key, False
            )
        elif table == "payments" and hit("duplicate_payment"):
            copy, anomalies = self.duplicate_payment(row, key, False)
            extra = [copy]
        elif table == "payments" and hit("failure_burst"):
            extra, anomalies = self.failure_burst(
                row, row["source_event_ts"], row["event_ts"], key, False
            )
        self.observe(table, row)
        return extra, anomalies

    def scripted(
        self, kind: str, at: datetime, account_id: str, tag: str
    ) -> tuple[list[tuple[str, dict]], list[dict]] | None:
        """One scripted anomaly at source time `at`. Returns None if it must wait.

        Invoice and payment anomalies are synthetic rows templated on the latest real
        one, because invoices and payments are rare and may not arrive on schedule.
        """
        if kind == "session_spike":
            rows, anomalies = self.session_spike(account_id, at, tag, True)
            return [("sessions", row) for row in rows], anomalies
        if kind == "negative_invoice":
            if self.last_invoice is None:
                return None
            invoice = dict(
                self.last_invoice,
                invoice_id="inv_" + digest(self.seed, "scripted", tag)[:12],
                source_event_ts=at,
                event_ts=None,
                is_injected=True,
            )
            return [("invoices", invoice)], self.negate_invoice(invoice, True)
        if self.last_payment is None:
            return None
        if kind == "duplicate_payment":
            copy, anomalies = self.duplicate_payment(self.last_payment, tag, True)
            return [("payments", copy)], anomalies
        if kind == "failure_burst":
            rows, anomalies = self.failure_burst(self.last_payment, at, at, tag, True)
            return [("payments", row) for row in rows], anomalies
        raise ValueError(kind)


def injection_rates(
    table_counts: dict[str, int], rate: float, kinds: list[str]
) -> dict[str, float]:
    """Per-row probabilities giving about `rate` anomalies per row, split evenly by type."""
    per_type = sum(table_counts.values()) * rate / len(kinds)
    return {
        kind: min(1.0, per_type / max(1, table_counts.get(ANOMALY_TABLE[kind], 0)))
        for kind in kinds
    }
