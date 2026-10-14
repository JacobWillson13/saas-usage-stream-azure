"""Online trial-conversion model (SPEC 7), evaluated prequentially.

All feature windows and label timing use source time: source_event_ts for rows, and
shifted columns (trial_started_at, trial_ends_at) minus the baseline offset. event_ts
is copied onto outputs for display only.
"""

from __future__ import annotations

import heapq
import json
import math
import pickle
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import psycopg
from river import compose, linear_model, optim, preprocessing

from saas_stream.consumer import Consumer, fetch_range, get_offset, run_live
from saas_stream.db import stream_pool
from saas_stream.settings import CHECKPOINTS, data_end

LABEL_GRACE = timedelta(days=3)
METRIC_EVERY = 10
ROLLING_WINDOW = 100
MODEL_NAMES = ["logreg", "points"]
LICENSE_ADDS = {"auto_license_added", "licenses_set"}
TRIAL_DAYS = 14
SCORING_DAY = 7  # each trial is scored at its latest prediction on or before this day
COUNT_FEATURES = [
    "max_active_users", "active_days", "max_tagged_resources", "gated_attempts",
    "distinct_features", "users_invited", "devices_registered", "distinct_os", "license_adds",
    "devices_per_active_user",
]  # fmt: skip


@dataclass(frozen=True)
class LearnerConfig:
    """Training setup for the logistic regression (see SPEC 7)."""

    transform: str = "log1p"  # "raw" or "log1p" on count features
    optimizer: str = "adam"  # "sgd" or "adam"
    learning_rate: float = 0.003
    l2: float = 0.001
    train_on: str = "scored"  # the day-7 "scored" vector, or the "final" vector at the label


LEARNER = LearnerConfig()


def make_logreg(config: LearnerConfig) -> compose.Pipeline:
    optimizer = (
        optim.Adam(config.learning_rate)
        if config.optimizer == "adam"
        else optim.SGD(config.learning_rate)
    )
    return compose.Pipeline(
        preprocessing.StandardScaler(),
        linear_model.LogisticRegression(optimizer=optimizer, l2=config.l2),
    )


def transform(x: dict[str, float], config: LearnerConfig) -> dict[str, float]:
    if config.transform == "raw":
        return x
    out = dict(x)
    for name in COUNT_FEATURES:
        out[name] = math.log1p(x[name])
    out["days_into_trial"] = min(1.25, x["days_into_trial"] / TRIAL_DAYS)
    return out


def source_time(row: dict, column: str) -> datetime:
    """A column's time in source time: source_event_ts plus its offset from event_ts.

    Works for shifted history and for live rows, whose display times are now().
    """
    return row["source_event_ts"] + (row[column] - row["event_ts"])


@dataclass
class Trial:
    account_id: str
    start: datetime
    deadline: datetime
    excluded: bool
    price_current: float
    is_nonprofit: float
    is_usd: float
    users_invited: int = 0
    devices_registered: int = 0
    os_seen: set = field(default_factory=set)
    license_adds: int = 0
    prior_personal_device: int = 0
    max_active_users: int = 0
    active_days: int = 0
    active_user_days: int = 0
    user_device_days: int = 0
    max_tagged_resources: int = 0
    gated_attempts: int = 0
    features_used: set = field(default_factory=set)
    last_day: object = None
    scored_prediction: dict = field(default_factory=dict)  # latest on or before day 7
    scored_features: dict | None = None
    live_day: object = None  # live sessions: distinct users and devices per day
    day_users: set = field(default_factory=set)
    day_devices: set = field(default_factory=set)

    def features(self, now: datetime) -> dict[str, float]:
        return {
            "days_into_trial": (now - self.start).total_seconds() / 86400,
            "max_active_users": self.max_active_users,
            "active_days": self.active_days,
            "devices_per_active_user": self.user_device_days / max(1, self.active_user_days),
            "max_tagged_resources": self.max_tagged_resources,
            "gated_attempts": self.gated_attempts,
            "distinct_features": len(self.features_used),
            "users_invited": self.users_invited,
            "devices_registered": self.devices_registered,
            "distinct_os": len(self.os_seen),
            "license_adds": self.license_adds,
            "prior_personal_device": self.prior_personal_device,
            "price_current": self.price_current,
            "is_nonprofit": self.is_nonprofit,
            "is_usd": self.is_usd,
        }


def points_probability(x: dict[str, float]) -> float:
    """Fixed baseline, no learning: one point per sign of engagement."""
    points = sum(
        [
            x["max_active_users"] >= 3,
            x["active_days"] >= 5,
            x["users_invited"] >= 3,
            x["devices_registered"] >= 5,
            x["distinct_features"] >= 2,
            x["gated_attempts"] >= 1,
            x["license_adds"] >= 1,
            x["prior_personal_device"] >= 1,
        ]
    )
    return min(0.95, max(0.05, points / 8))


def auc(labels: list[int], scores: list[float]) -> float | None:
    """Exact ROC AUC (Mann-Whitney U with average ranks for ties)."""
    positives = sum(labels)
    negatives = len(labels) - positives
    if positives == 0 or negatives == 0:
        return None
    order = sorted(range(len(scores)), key=lambda i: scores[i])
    ranks = [0.0] * len(scores)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and scores[order[j + 1]] == scores[order[i]]:
            j += 1
        for k in range(i, j + 1):
            ranks[order[k]] = (i + j) / 2 + 1
        i = j + 1
    positive_ranks = sum(r for r, y in zip(ranks, labels, strict=True) if y)
    return (positive_ranks - positives * (positives + 1) / 2) / (positives * negatives)


def log_loss(labels: list[int], scores: list[float]) -> float:
    eps = 1e-6
    total = 0.0
    for y, p in zip(labels, scores, strict=True):
        p = min(1 - eps, max(eps, p))
        total -= y * math.log(p) + (1 - y) * math.log(1 - p)
    return total / len(labels)


def top_decile_lift(labels: list[int], scores: list[float]) -> float | None:
    base = sum(labels) / len(labels)
    if base == 0:
        return None
    ranked = sorted(zip(scores, labels, strict=True), key=lambda item: -item[0])
    top = ranked[: max(1, len(ranked) // 10)]
    return (sum(y for _, y in top) / len(top)) / base


class TrialModel(Consumer):
    name = "model"
    tables = [
        "accounts", "users", "devices", "sessions", "usage_daily", "feature_usage_daily",
        "feature_events", "license_events", "plan_changes",
    ]  # fmt: skip

    def __init__(self, phase: str = "warm", learner: LearnerConfig = LEARNER) -> None:
        self.learner = learner
        self.phase = phase
        self.data_end = data_end().replace(tzinfo=UTC)
        self.account_types: dict[str, str] = {}
        self.personal_fingerprints: set[str] = set()
        self.open: dict[str, Trial] = {}
        self.deadlines: list[tuple[datetime, str]] = []
        self.logreg = make_logreg(learner)
        self.resolved: dict[str, list[tuple[int, float]]] = {name: [] for name in MODEL_NAMES}
        self.labels_seen = [0, 0]  # resolved trials, conversions: the running base rate
        self.clear_outputs()

    def clear_outputs(self) -> None:
        self.predictions: list[dict] = []
        self.outcomes: list[dict] = []
        self.metrics: list[dict] = []

    # Checkpoints -----------------------------------------------------------

    def save(self, path) -> None:
        self.clear_outputs()
        path.write_bytes(pickle.dumps(self))

    @staticmethod
    def load(path) -> TrialModel:
        model = pickle.loads(path.read_bytes())
        model.clear_outputs()
        return model

    def restore(self, pool) -> None:
        """Checkpoint state, then replay rows after it without writing (already written)."""
        checkpoint = json.loads((CHECKPOINTS / "checkpoint.json").read_text())
        saved = TrialModel.load(CHECKPOINTS / "model.pkl")
        self.__dict__.update(saved.__dict__)
        self.phase = "live"
        with pool.connection() as conn:
            done = get_offset(conn, self.name)
            start = checkpoint["offsets"][self.name]
            for table, row in fetch_range(conn, self.tables, start, done):
                self.process(table, row)
        self.clear_outputs()

    # Rows ----------------------------------------------------------------------

    def probability(self, name: str, x: dict[str, float]) -> float:
        if name == "points":
            return points_probability(x)
        return self.logreg.predict_proba_one(transform(x, self.learner)).get(True, 0.5)

    def expire(self, now: datetime) -> None:
        while self.deadlines and self.deadlines[0][0] < now:
            deadline, account_id = heapq.heappop(self.deadlines)
            trial = self.open.get(account_id)
            if trial is not None and trial.deadline == deadline:
                del self.open[account_id]  # no resolution in time: unlabeled

    def process(self, table: str, row: dict) -> None:
        now = row["source_event_ts"]
        self.expire(now)
        account_id = row["account_id"]
        trial = self.open.get(account_id)
        if table == "accounts":
            self.add_account(row)
        elif table == "devices":
            if self.account_types.get(account_id) == "personal":
                self.personal_fingerprints.add(row["device_fingerprint"])
            if trial is not None:
                trial.devices_registered += 1
                trial.os_seen.add(row["os"])
                if row["device_fingerprint"] in self.personal_fingerprints:
                    trial.prior_personal_device = 1
        elif trial is None:
            return
        elif table == "users":
            trial.users_invited += 1
        elif table == "license_events":
            trial.license_adds += row["event_type"] in LICENSE_ADDS
        elif table == "usage_daily" and trial.start <= now <= trial.deadline:
            trial.max_active_users = max(trial.max_active_users, row["active_users"])
            trial.max_tagged_resources = max(trial.max_tagged_resources, row["tagged_resources"])
            if row["active_users"] > 0:
                trial.active_days += 1
                trial.active_user_days += row["active_users"]
                trial.user_device_days += row["user_devices"]
            self.predict(trial, row)
        elif table == "sessions" and trial.start <= now <= trial.deadline:
            self.add_session(trial, row)
            self.predict(trial, row)
        elif table == "feature_events" and trial.start <= now <= trial.deadline:
            trial.features_used.add(row["feature"])
            trial.gated_attempts += row["gated_blocked"]
            self.predict(trial, row)
        elif table == "feature_usage_daily" and trial.start <= now <= trial.deadline:
            trial.features_used.add(row["feature"])
            if row["gated_blocked"]:
                trial.gated_attempts += row["attempts"]
            self.predict(trial, row)
        elif table == "plan_changes" and row["change_source"] == "trial_resolution":
            if now >= trial.start:
                self.resolve(trial, 0 if row["to_plan"] == "free" else 1, row)

    @staticmethod
    def add_session(trial: Trial, row: dict) -> None:
        """Live sessions build the same daily features the usage_daily rows do."""
        day = row["source_event_ts"].date()
        if trial.live_day != day:
            trial.live_day, trial.day_users, trial.day_devices = day, set(), set()
            trial.active_days += 1
        if row["user_id"] not in trial.day_users:
            trial.day_users.add(row["user_id"])
            trial.active_user_days += 1
            trial.max_active_users = max(trial.max_active_users, len(trial.day_users))
        if row["device_id"] is not None and row["device_id"] not in trial.day_devices:
            trial.day_devices.add(row["device_id"])
            trial.user_device_days += 1

    def add_account(self, row: dict) -> None:
        self.account_types[row["account_id"]] = row["account_type"]
        if row["trial_started_at"] is None:
            return
        start = source_time(row, "trial_started_at")
        deadline = source_time(row, "trial_ends_at") + LABEL_GRACE
        trial = Trial(
            account_id=row["account_id"],
            start=start,
            deadline=deadline,
            excluded=deadline > self.data_end,
            price_current=float(row["price_version"] == "current"),
            is_nonprofit=float(row["is_nonprofit"]),
            is_usd=float(row["currency"] == "usd"),
        )
        self.open[trial.account_id] = trial
        heapq.heappush(self.deadlines, (deadline, trial.account_id))

    def predict(self, trial: Trial, row: dict) -> None:
        now = row["source_event_ts"]
        day = now.date()
        if trial.last_day == day:
            return
        trial.last_day = day
        x = trial.features(now)
        on_time = x["days_into_trial"] <= SCORING_DAY
        if on_time:
            trial.scored_features = x
        for name in MODEL_NAMES:
            p = self.probability(name, x)
            if on_time:
                trial.scored_prediction[name] = p
            self.predictions.append({
                "account_id": trial.account_id, "model_name": name, "source_day": day,
                "source_event_ts": now, "event_ts": row["event_ts"], "probability": p,
                "phase": self.phase,
            })  # fmt: skip

    def base_rate(self) -> float:
        seen, converted = self.labels_seen
        return (converted + 1) / (seen + 2)

    def resolve(self, trial: Trial, label: int, row: dict) -> None:
        """Score the trial at its day-7 prediction (the base rate if it has none), then learn."""
        del self.open[trial.account_id]
        if trial.excluded:
            return
        now = row["source_event_ts"]
        base = self.base_rate()
        scored = trial.scored_features
        for name in MODEL_NAMES:
            p = trial.scored_prediction.get(name, base)
            self.outcomes.append({
                "account_id": trial.account_id, "model_name": name, "label": label,
                "probability": p, "source_event_ts": now, "event_ts": row["event_ts"],
                "phase": self.phase, "at_base_rate": scored is None,
            })  # fmt: skip
            self.resolved[name].append((label, p))
            if len(self.resolved[name]) % METRIC_EVERY == 0:
                self.metrics.append(self.metric_row(name, row))
        self.labels_seen[0] += 1
        self.labels_seen[1] += label
        features = trial.features(now) if self.learner.train_on == "final" else scored
        if features is not None:
            self.logreg.learn_one(transform(features, self.learner), bool(label))

    def metric_row(self, name: str, row: dict) -> dict:
        resolved = self.resolved[name]
        labels = [y for y, _ in resolved]
        scores = [p for _, p in resolved]
        recent_labels, recent_scores = labels[-ROLLING_WINDOW:], scores[-ROLLING_WINDOW:]
        return {
            "model_name": name, "phase": self.phase, "resolved_trials": len(resolved),
            "rolling_auc": auc(recent_labels, recent_scores),
            "rolling_log_loss": log_loss(recent_labels, recent_scores),
            "cumulative_auc": auc(labels, scores),
            "top_decile_lift": top_decile_lift(labels, scores),
            "source_event_ts": row["source_event_ts"], "event_ts": row["event_ts"],
        }  # fmt: skip

    # Output -------------------------------------------------------------------

    def write(self, cur: psycopg.Cursor) -> int:
        written = len(self.predictions) + len(self.outcomes) + len(self.metrics)
        if self.predictions:
            cur.executemany(
                """
                INSERT INTO ops.predictions (account_id, model_name, source_day,
                    source_event_ts, event_ts, probability, phase)
                VALUES (%(account_id)s, %(model_name)s, %(source_day)s, %(source_event_ts)s,
                    %(event_ts)s, %(probability)s, %(phase)s)
                ON CONFLICT DO NOTHING
                """,
                self.predictions,
            )
        if self.outcomes:
            cur.executemany(
                """
                INSERT INTO ops.trial_outcomes (account_id, model_name, label, probability,
                    source_event_ts, event_ts, phase)
                VALUES (%(account_id)s, %(model_name)s, %(label)s, %(probability)s,
                    %(source_event_ts)s, %(event_ts)s, %(phase)s)
                ON CONFLICT DO NOTHING
                """,
                self.outcomes,
            )
        if self.metrics:
            cur.executemany(
                """
                INSERT INTO ops.model_metrics (model_name, phase, resolved_trials, rolling_auc,
                    rolling_log_loss, cumulative_auc, top_decile_lift, source_event_ts, event_ts)
                VALUES (%(model_name)s, %(phase)s, %(resolved_trials)s, %(rolling_auc)s,
                    %(rolling_log_loss)s, %(cumulative_auc)s, %(top_decile_lift)s,
                    %(source_event_ts)s, %(event_ts)s)
                ON CONFLICT DO NOTHING
                """,
                self.metrics,
            )
        self.clear_outputs()
        return written


def main() -> None:
    from saas_stream.demo import install_stop, log

    stop = threading.Event()
    install_stop(stop)
    pool = stream_pool(max_size=2)
    try:
        run_live(pool, TrialModel(phase="live"), stop, log)
    finally:
        pool.close()


if __name__ == "__main__":
    main()
