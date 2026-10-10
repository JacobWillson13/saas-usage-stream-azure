"""Build a live take: real-time events generated from the source records of DEMO_DATE.

The take covers the source day SOURCE_CUTOFF (DEMO_DATE before the shift):
- each usage_daily row becomes active_users sessions, each assigned to a real user
  and device of that account
- each feature_usage_daily row becomes `attempts` feature events
- signups, user invites, devices, license events, plan changes, invoices, and
  payments of that day are single rows
- generated payments: real payments from the 30 days before DEMO_DATE, re-keyed,
  about one every GENERATED_PAYMENT_SECONDS, succeeded and failed in their historical
  ratio (is_generated)

Events are interleaved in a seeded random order (a child never before a parent from
the same day) with exponential gaps at DEMO_RATE events/min. The take clock is the
source day's midnight plus the scheduled seconds into the take; it becomes each
event's source_event_ts. Display timestamps are stored as offsets from the event time
and written as now() plus that offset, so they line up with event_ts. The whole take
is a pure function of the prepared data and DEMO_SEED.
"""

from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta

from saas_stream.inject import LIVE_KINDS, Injector, digest, injection_rates
from saas_stream.rows import DISPLAY_TIMES, KEYS, shift_row, source_rows, utc
from saas_stream.settings import DEMO_INJECT_RATE, SCRIPTED

SINGLE_TABLES = [
    "accounts", "users", "devices", "license_events", "plan_changes", "invoices", "payments",
]  # fmt: skip
SESSION_MEAN_SECONDS = 1500
GENERATED_PAYMENT_SECONDS = 10
GENERATED_PAYMENT_DAYS = 30
PARENT_GAP = 1e-9


@dataclass
class LiveEvent:
    t: float  # scheduled seconds into the take
    table: str
    row: dict  # app column values; event_ts is set to now() at insert
    anomalies: list[dict] = field(default_factory=list)


class Roster:
    """Users and devices per account as of the end of the take day (from Parquet)."""

    def __init__(self, end: datetime, offset_days: int) -> None:
        self.offset = timedelta(days=offset_days)
        self.users: dict[str, list[str]] = defaultdict(list)
        self.user_devices: dict[str, list[str]] = defaultdict(list)
        self.account_devices: dict[str, list[str]] = defaultdict(list)
        self.active_users: dict[str, int] = {}
        for _, row in source_rows("users", end=end):
            self.users[row["account_id"]].append(row["user_id"])
        for _, row in source_rows("devices", end=end):
            self.account_devices[row["account_id"]].append(row["device_id"])
            if row["user_id"] is not None:
                self.user_devices[row["user_id"]].append(row["device_id"])

    def device_for(self, account_id: str, user_id: str | None, rng: random.Random) -> str | None:
        devices = self.user_devices.get(user_id) or self.account_devices.get(account_id)
        return rng.choice(devices) if devices else None

    def pick(self, account_id: str, key: str) -> tuple[str | None, str | None]:
        rng = random.Random(key)
        users = self.users.get(account_id)
        user_id = rng.choice(users) if users else None
        return user_id, self.device_for(account_id, user_id, rng)

    def sample_users(self, account_id: str, n: int, rng: random.Random) -> list[str | None]:
        users = self.users.get(account_id) or [None]
        if n <= len(users):
            return rng.sample(users, n)
        return [rng.choice(users) for _ in range(n)]

    def display(self, source_ts: datetime) -> datetime:
        return source_ts + self.offset


def session_seconds(rng: random.Random) -> int:
    return int(min(4 * 3600, max(30, rng.expovariate(1 / SESSION_MEAN_SECONDS))))


def row_id(table: str, row: dict) -> str:
    return str(row[KEYS[table][0]])


def live_row(table: str, raw: dict, at: datetime, offset_days: int) -> dict:
    """A source row as a live row at take time `at`.

    Dates move by the offset; display timestamps become offsets from the event time
    (written as now() + offset at insert); source_event_ts is the take clock.
    """
    event = utc(raw["event_ts"])
    row = shift_row(raw, offset_days)
    for column in DISPLAY_TIMES.get(table, []):
        if raw.get(column) is not None:
            row[column] = utc(raw[column]) - event
    row["source_event_ts"] = at
    row["event_ts"] = None
    return row


def as_offsets(table: str, row: dict) -> None:
    """Template-based rows: absolute display times become 'now' (offset zero)."""
    for column in DISPLAY_TIMES.get(table, []):
        if isinstance(row.get(column), datetime):
            row[column] = timedelta(0)


def generated_payments(
    day: date, offset_days: int, seed: str, until: float, midnight: datetime
) -> list[tuple[float, str, dict]]:
    """Real non-test payments from the 30 days before the take day, re-keyed."""
    start = datetime.combine(day, time())
    history = [
        shift_row(row, offset_days)
        for _, row in source_rows(
            "payments", start=start - timedelta(days=GENERATED_PAYMENT_DAYS), end=start
        )
        if not row["is_test"]
    ]
    failed = [row for row in history if row["status"] == "failed"]
    succeeded = [row for row in history if row["status"] != "failed"]
    failure_ratio = len(failed) / len(history)
    rng = random.Random(digest(seed, "generated", day))
    payments: list[tuple[float, str, dict]] = []
    t = rng.expovariate(1 / GENERATED_PAYMENT_SECONDS)
    while t <= until:
        pool = failed if failed and rng.random() < failure_ratio else succeeded
        payment = dict(
            rng.choice(pool),
            payment_id="pay_" + digest(seed, "generated", day, len(payments))[:12],
            source_event_ts=midnight + timedelta(seconds=t),
            event_ts=None,
            is_generated=True,
        )
        payments.append((t, "payments", payment))
        t += rng.expovariate(1 / GENERATED_PAYMENT_SECONDS)
    return payments


def ordered_items(day: date, seed: str, roster: Roster) -> list[tuple[str, dict]]:
    """Every event of the day as (table, raw row), in seeded random order, parents first."""
    start = datetime.combine(day, time())
    end = start + timedelta(days=1)
    order = random.Random(digest(seed, "order", day))
    keys: dict[tuple[str, str], float] = {}
    items: list[tuple[float, int, str, dict]] = []

    def place(table: str, row: dict, parents: list[tuple[str, str | None]]) -> None:
        key = order.random()
        for parent in parents:
            if parent in keys:
                key = max(key, keys[parent] + PARENT_GAP)
        keys[(table, row_id(table, row))] = key
        items.append((key, len(items), table, row))

    for table in SINGLE_TABLES:
        for _, row in source_rows(table, start=start, end=end):
            parents = [("accounts", row["account_id"]), ("users", row.get("user_id"))]
            if table == "payments":
                parents.append(("invoices", row["invoice_id"]))
            place(table, row, parents)

    for _, usage in source_rows("usage_daily", start=start, end=end):
        account_id = usage["account_id"]
        roster.active_users[account_id] = usage["active_users"]
        rng = random.Random(digest(seed, "sessions", account_id, day))
        for i, user_id in enumerate(roster.sample_users(account_id, usage["active_users"], rng)):
            device_id = roster.device_for(account_id, user_id, rng)
            session = {
                "session_id": "ses_" + digest(seed, "session", account_id, day, i)[:12],
                "account_id": account_id, "user_id": user_id, "device_id": device_id,
                "duration_s": session_seconds(rng),
            }  # fmt: skip
            place("sessions", session, [
                ("accounts", account_id), ("users", user_id), ("devices", device_id),
            ])  # fmt: skip

    for _, feature in source_rows("feature_usage_daily", start=start, end=end):
        account_id = feature["account_id"]
        rng = random.Random(digest(seed, "features", account_id, day, feature["feature"]))
        for j in range(feature["attempts"]):
            user_id = rng.choice(roster.users.get(account_id) or [None])
            event_key = digest(seed, "feature", account_id, day, feature["feature"], j)
            event = {
                "event_id": "fev_" + event_key[:12],
                "account_id": account_id, "user_id": user_id, "feature": feature["feature"],
                "gated_blocked": feature["gated_blocked"],
            }  # fmt: skip
            place("feature_events", event, [("accounts", account_id), ("users", user_id)])

    items.sort(key=lambda item: item[:2])
    return [(table, row) for _, _, table, row in items]


def latest_before(table: str, end: datetime, offset_days: int, where=lambda row: True):
    latest = None
    for _, row in source_rows(table, end=end):
        if where(row):
            latest = row
    return shift_row(latest, offset_days) if latest else None


def build_take(day: date, offset_days: int, seed: str, rate_per_min: float) -> list[LiveEvent]:
    start = datetime.combine(day, time())
    end = start + timedelta(days=1)
    roster = Roster(end, offset_days)
    items = ordered_items(day, seed, roster)

    counts: dict[str, int] = defaultdict(int)
    for table, _ in items:
        counts[table] += 1
    injector = Injector(injection_rates(counts, DEMO_INJECT_RATE, LIVE_KINDS), seed, roster)
    injector.last_invoice = latest_before("invoices", start, offset_days, lambda r: r["total"] > 0)
    injector.last_payment = latest_before("payments", start, offset_days)

    gaps = random.Random(digest(seed, "gaps", day))
    midnight = utc(start)
    scheduled: list[tuple[float, str, dict]] = []
    t = 0.0
    for table, raw in items:
        t += gaps.expovariate(rate_per_min / 60)
        at = midnight + timedelta(seconds=t)
        if table in ("sessions", "feature_events"):
            row = dict(raw, source_event_ts=at, event_ts=None)
            row["started_at" if table == "sessions" else "occurred_at"] = timedelta(0)
        else:
            row = live_row(table, raw, at, offset_days)
        scheduled.append((t, table, row))
    scheduled += generated_payments(day, offset_days, seed, t, midnight)
    scheduled.sort(key=lambda item: item[0])  # stable: ties keep generation order

    counts: dict[str, int] = defaultdict(int)
    for _, table, _ in scheduled:
        counts[table] += 1
    injector = Injector(injection_rates(counts, DEMO_INJECT_RATE, LIVE_KINDS), seed, roster)
    injector.last_invoice = latest_before("invoices", start, offset_days, lambda r: r["total"] > 0)
    injector.last_payment = latest_before("payments", start, offset_days)

    scripted = sorted(SCRIPTED.items())
    events: list[LiveEvent] = []
    for index, (t, table, row) in enumerate(scheduled):
        at = row["source_event_ts"]
        while scripted and scripted[0][0] <= t:
            _, kind = scripted[0]
            account_id = next_session_account(scheduled, index)
            result = injector.scripted(kind, at, account_id, f"{seed}:{kind}")
            if result is None:
                break  # no template yet: try again at the next event
            scripted.pop(0)
            extra, anomalies = result
            for n, (extra_table, extra_row) in enumerate(extra):
                events.append(LiveEvent(t, extra_table, extra_row, anomalies if n == 0 else []))

        extra, anomalies = injector.random(table, row)
        events.append(LiveEvent(t, table, row, anomalies if not extra else []))
        for n, extra_row in enumerate(extra):
            events.append(LiveEvent(t, table, extra_row, anomalies if n == 0 else []))
    for event in events:
        as_offsets(event.table, event.row)
    return events


def next_session_account(scheduled: list[tuple[float, str, dict]], index: int) -> str:
    for _, table, row in scheduled[index:]:
        if table == "sessions":
            return row["account_id"]
    return scheduled[index][2]["account_id"]
