"""Evaluations (tasks 3.5 and 4.5): outlier detection and the trial model.

`make evaluate-outliers` writes docs/results/outliers.md.
`make evaluate-model` writes docs/results/model.md and docs/img/model_rolling_auc.svg.
"""

from __future__ import annotations

import argparse
import json
import statistics
from collections.abc import Iterator
from datetime import UTC, datetime, time

from saas_stream.inject import ANOMALY_FLAG
from saas_stream.model import TrialModel, auc, log_loss, top_decile_lift
from saas_stream.rows import merged_source_rows, utc
from saas_stream.settings import CHECKPOINTS, ROOT, settings

RESULTS = ROOT / "docs" / "results"
IMAGES = ROOT / "docs" / "img"


# Prequential replay over prepared data -------------------------------------------


def prepared_rows(
    tables: list[str], start: datetime | None = None, end: datetime | None = None
) -> Iterator[tuple[str, dict]]:
    """Prepared Parquet rows as app rows with no shift (event_ts = source_event_ts)."""
    for (ts, _, _), table, row in merged_source_rows(start=start):
        if end is not None and ts >= end:
            return
        if table not in tables:
            continue
        app = {k: utc(v) if isinstance(v, datetime) else v for k, v in row.items()}
        app["source_event_ts"] = app["event_ts"]
        yield table, app


def replay(model: TrialModel, rows: Iterator[tuple[str, dict]]) -> list[dict]:
    """Run the model prequentially over rows; return its trial outcomes in order."""
    outcomes: list[dict] = []
    for table, row in rows:
        model.process(table, row)
        if model.outcomes:
            outcomes += model.outcomes
        model.clear_outputs()
    return outcomes


def summarize(outcomes: list[dict], model_name: str) -> dict:
    rows = [o for o in outcomes if o["model_name"] == model_name]
    if not rows:
        return {"n": 0}
    labels = [o["label"] for o in rows]
    scores = [o["probability"] for o in rows]
    base = sum(labels) / len(labels)
    return {
        "n": len(rows),
        "observed": base,
        "mean_prediction": statistics.mean(scores),
        "log_loss": log_loss(labels, scores),
        "constant_log_loss": log_loss(labels, [base] * len(labels)),
        "auc": auc(labels, scores),
        "top_decile_lift": top_decile_lift(labels, scores),
    }


def cutoff_datetime(day) -> datetime:
    return datetime.combine(day, time(), tzinfo=UTC).replace(tzinfo=None)


# 3.5 Outliers -----------------------------------------------------------------------

EXPECTED_FLAG_SQL = (
    "CASE a.anomaly_type "
    + " ".join(f"WHEN '{kind}' THEN '{flag}'" for kind, flag in ANOMALY_FLAG.items())
    + " END"
)


def median_or_none(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def fmt(value, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def outlier_rows(conn, live: bool, flag_mark: int) -> list[dict]:
    scope = "a.is_demo" if live else "NOT a.is_demo"
    flag_scope = "f.flag_id > %s" if live else "f.flag_id <= %s"
    rows = conn.execute(
        f"""
        SELECT a.anomaly_type, {EXPECTED_FLAG_SQL} AS flag_type, a.injected_at, a.source_event_ts,
               f.flagged_at, f.source_event_ts
        FROM ops.injected_anomalies a
        LEFT JOIN ops.outlier_flags f
            ON f.table_name = a.table_name AND f.row_key = a.row_key
           AND f.flag_type = {EXPECTED_FLAG_SQL}
        WHERE {scope}
        ORDER BY a.anomaly_id
        """
    ).fetchall()
    by_type: dict[str, dict] = {}
    for kind, flag_type, injected_at, source_ts, flagged_at, flag_source_ts in rows:
        entry = by_type.setdefault(kind, {"flag_type": flag_type, "n": 0, "hit": 0, "delays": []})
        entry["n"] += 1
        if flagged_at is not None:
            entry["hit"] += 1
            delay = (flagged_at - injected_at) if live else (flag_source_ts - source_ts)
            entry["delays"].append(max(0.0, delay.total_seconds()))
    for kind, entry in by_type.items():
        flags, matched = conn.execute(
            f"""
            SELECT count(*), count(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM ops.injected_anomalies a
                WHERE a.table_name = f.table_name AND a.row_key = f.row_key
                  AND a.anomaly_type = %s))
            FROM ops.outlier_flags f WHERE f.flag_type = %s AND {flag_scope}
            """,
            (kind, entry["flag_type"], flag_mark),
        ).fetchone()
        entry["flags"], entry["matched"] = flags, matched
    return [{"anomaly_type": kind, **entry} for kind, entry in sorted(by_type.items())]


SPIKE_THRESHOLDS = (3.5, 5.0, 7.0)


def spike_threshold_rows(conn) -> list[dict]:
    """Activity-spike recall, precision, and F1 at several z thresholds on history.

    z does not depend on the threshold, so each eligible row is scored once.
    """
    from psycopg.rows import dict_row

    from saas_stream.outliers import OutlierScorer

    with conn.cursor(row_factory=dict_row) as cur:
        injected = {
            r["row_key"]
            for r in cur.execute(
                "SELECT row_key FROM ops.injected_anomalies "
                "WHERE anomaly_type = 'activity_spike' AND NOT is_demo"
            ).fetchall()
        }
        rows = cur.execute(
            "SELECT account_id, activity_date, active_users, source_event_ts "
            "FROM app.usage_daily WHERE NOT is_demo ORDER BY source_event_ts, ingest_id"
        ).fetchall()
    scorer = OutlierScorer()
    scored = []
    for row in rows:
        result = scorer.usage_z(row)
        if result is not None:
            scored.append((result[0], f"{row['account_id']}|{row['activity_date']}" in injected))
    table = []
    for threshold in SPIKE_THRESHOLDS:
        hits = sum(1 for z, label in scored if z > threshold and label)
        flags = sum(1 for z, _ in scored if z > threshold)
        recall = hits / len(injected) if injected else 0.0
        precision = hits / flags if flags else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        table.append({"threshold": threshold, "flags": flags, "hits": hits,
                      "recall": recall, "precision": precision, "f1": f1})  # fmt: skip
    return table


def write_outlier_report() -> None:
    from saas_stream.db import admin_connection

    checkpoint = json.loads((CHECKPOINTS / "checkpoint.json").read_text())
    flag_mark = checkpoint["high_water"]["ops.outlier_flags"]
    lines = [
        "# Outlier detection evaluation (task 3.5)",
        "",
        f"Generated {datetime.now(UTC):%Y-%m-%d %H:%M} UTC by `make evaluate-outliers`.",
        "Recall: injected anomalies whose expected flag was raised on the injected row key.",
        "Precision: flags of that type on injected rows of that anomaly, out of all flags of",
        "that type in the same scope. **Precision is measured against injected labels only**:",
        "flags on rows that were not injected count as false positives even when they are",
        "real oddities in the source, so precision here is a lower bound.",
        "",
    ]
    with admin_connection() as conn:
        for live, title, delay_note in (
            (False, "Baseline history (random injection, about 1 per 2,000 rows)",
             "Delay is in source time from the anomaly row to the flag "
             "(history is scored in bulk)."),
            (True, "Live demo take (scripted at 60, 120, 180, 240 s)",
             "Delay is wall time from insert to flag; consumers poll every 5 s."),
        ):  # fmt: skip
            rows = outlier_rows(conn, live, flag_mark)
            lines += [f"## {title}", "", delay_note, ""]
            if not rows:
                lines += ["No anomalies in this scope yet: run a demo take first.", ""]
                continue
            lines += [
                "| Anomaly | Flag | Injected | Detected | Recall | Flags | Precision "
                "| Median delay |",
                "|---|---|---:|---:|---:|---:|---:|---:|",
            ]
            for r in rows:
                precision = r["matched"] / r["flags"] if r["flags"] else None
                delay = median_or_none(r["delays"])
                delay_text = "n/a" if delay is None else (
                    f"{delay:.1f} s" if live or delay < 120 else f"{delay / 3600:.1f} h"
                )
                lines.append(
                    f"| {r['anomaly_type']} | {r['flag_type']} | {r['n']} | {r['hit']} | "
                    f"{fmt(r['hit'] / r['n'])} | {r['flags']} | {fmt(precision)} | {delay_text} |"
                )
            lines.append("")
        thresholds = spike_threshold_rows(conn)
        natural = conn.execute(
            """
            SELECT f.flag_type,
                   count(*) FILTER (WHERE f.flag_id <= %s) AS baseline,
                   count(*) FILTER (WHERE f.flag_id > %s) AS live
            FROM ops.outlier_flags f
            WHERE NOT EXISTS (SELECT 1 FROM ops.injected_anomalies a
                              WHERE a.table_name = f.table_name AND a.row_key = f.row_key)
            GROUP BY 1 ORDER BY 1
            """,
            (flag_mark, flag_mark),
        ).fetchall()
    from saas_stream.outliers import SPIKE_Z_THRESHOLD

    best = max(thresholds, key=lambda r: r["f1"])
    lines += [
        "## Activity spike: threshold choice",
        "",
        "Robust z of an account's active_users vs its trailing 28 days, on all eligible",
        "history rows. Precision is against injected labels only.",
        "",
        "| z threshold | Flags | On injected rows | Recall | Precision | F1 |",
        "|---:|---:|---:|---:|---:|---:|",
        *[
            f"| {r['threshold']:g}{' (default)' if r['threshold'] == SPIKE_Z_THRESHOLD else ''} | "
            f"{r['flags']} | {r['hits']} | {fmt(r['recall'])} | {fmt(r['precision'])} | "
            f"{fmt(r['f1'])} |"
            for r in thresholds
        ],
        "",
        f"Best F1 is at z > {best['threshold']:g}; the scorer's default is z > "
        f"{SPIKE_Z_THRESHOLD:g}. The tables above use the default.",
        "",
        "## Naturally occurring hits",
        "",
        "Flags on rows that were not injected.",
        "",
        "| Flag | Baseline | Live take |",
        "|---|---:|---:|",
    ]
    lines += [f"| {flag} | {base} | {live} |" for flag, base, live in natural]
    lines += ["", NOTES_OUTLIERS, ""]
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "outliers.md").write_text("\n".join(lines))
    print(f"wrote {RESULTS / 'outliers.md'}")


NOTES_OUTLIERS = """## Notes

- `activity_spike` (history) uses z > 7, the best F1 of the thresholds tested (table
  above). At 3.5 it caught 96% of injected spikes but raised 2,858 other flags.
- `failure_rate` has one flag per source hour. Natural payment failures in the source
  are monthly retry runs (up to 23 failures in one hour), which look like injected
  bursts and are flagged as natural hits. Two bursts in one hour share one flag.
- Missed bursts land on the 2nd of a month, right after the monthly retry run on the
  1st: that run is nearly all of the trailing week's few payments (pooled failure rate
  about 0.9), so 20 failures in a busy hour look normal against it.
- `session_rate` and `ingest_volume` only run in the live demo.
- Rule checks (`test_mode_payment`, `orphan_payment`, `license_overuse`) have no injected
  counterpart; their counts are natural data errors kept in the prepared data."""


# 4.5 Model -----------------------------------------------------------------------------

PRICE_CHANGE = datetime(2026, 4, 8, tzinfo=UTC)


def rolling_series(outcomes: list[dict], model_name: str, window: int = 100) -> list[tuple]:
    rows = [o for o in outcomes if o["model_name"] == model_name]
    series = []
    for i in range(10, len(rows) + 1, 10):
        recent = rows[max(0, i - window) : i]
        value = auc([o["label"] for o in recent], [o["probability"] for o in recent])
        if value is not None:
            series.append((rows[i - 1]["source_event_ts"], value))
    return series


def svg_chart(series: dict[str, list[tuple]], marks: dict[str, datetime]) -> str:
    width, height, left, right, top, bottom = 860, 340, 56, 20, 24, 44
    points = [p for values in series.values() for p in values]
    t0, t1 = min(p[0] for p in points), max(p[0] for p in points)
    y0, y1 = 0.3, 0.9
    span = (t1 - t0).total_seconds()
    x = lambda t: left + (t - t0).total_seconds() / span * (width - left - right)  # noqa: E731
    y = lambda v: top + (y1 - min(y1, max(y0, v))) / (y1 - y0) * (height - top - bottom)  # noqa: E731
    colors = {"logreg": "#2563eb", "points": "#d97706"}
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'font-family="sans-serif" font-size="12">',
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
    ]
    for v in (0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9):
        parts.append(f'<line x1="{left}" x2="{width - right}" y1="{y(v):.1f}" y2="{y(v):.1f}" '
                     f'stroke="#e5e7eb"/><text x="{left - 8}" y="{y(v) + 4:.1f}" '
                     f'text-anchor="end" fill="#374151">{v:.1f}</text>')  # fmt: skip
    for year in range(t0.year, t1.year + 1):
        for month in (1, 7):
            tick = datetime(year, month, 1, tzinfo=UTC)
            if t0 <= tick <= t1:
                parts.append(f'<text x="{x(tick):.1f}" y="{height - 22}" text-anchor="middle" '
                             f'fill="#374151">{tick:%Y-%m}</text>')  # fmt: skip
    for label, when in marks.items():
        if t0 <= when <= t1:
            parts.append(f'<line x1="{x(when):.1f}" x2="{x(when):.1f}" y1="{top}" '
                         f'y2="{height - bottom}" stroke="#6b7280" stroke-dasharray="4 3"/>'
                         f'<text x="{x(when) + 4:.1f}" y="{top + 12}" '
                         f'fill="#374151">{label}</text>')  # fmt: skip
    for name, values in series.items():
        path = " ".join(f"{x(t):.1f},{y(v):.1f}" for t, v in values)
        parts.append(f'<polyline points="{path}" fill="none" stroke="{colors.get(name, "#111")}" '
                     f'stroke-width="1.5"/>')  # fmt: skip
    legend_x = left + 10
    for i, name in enumerate(series):
        parts.append(f'<rect x="{legend_x + i * 150}" y="{height - 14}" width="12" height="3" '
                     f'fill="{colors.get(name, "#111")}"/><text x="{legend_x + i * 150 + 18}" '
                     f'y="{height - 9}" fill="#111827">'
                     f'{name} rolling AUC (last 100)</text>')  # fmt: skip
    parts.append(f'<text x="{left}" y="{top - 8}" fill="#111827">Rolling ROC AUC over the '
                 f'last 100 resolved trials, by source date</text></svg>')  # fmt: skip
    return "\n".join(parts)


def summary_line(label: str, s: dict) -> str:
    if not s.get("n"):
        return f"| {label} | 0 | | | | | | |"
    return (
        f"| {label} | {s['n']} | {fmt(s['observed'])} | {fmt(s['mean_prediction'])} | "
        f"{fmt(s['log_loss'])} | {fmt(s['constant_log_loss'])} | {fmt(s['auc'])} | "
        f"{fmt(s['top_decile_lift'], 2)} |"
    )


def bootstrap_auc(outcomes: list[dict], draws: int = 2000, seed: int = 7) -> dict[str, tuple]:
    """Percentile 95% CIs for each model's AUC and the logreg - points difference.

    Trials are resampled with replacement and both models are scored on the same draw.
    """
    import random

    by_trial: dict[str, dict] = {}
    for o in outcomes:
        trial = by_trial.setdefault(o["account_id"], {"label": o["label"]})
        trial[o["model_name"]] = o["probability"]
    trials = list(by_trial.values())
    rng = random.Random(seed)
    stats: dict[str, list[float]] = {"logreg": [], "points": [], "difference": []}
    for _ in range(draws):
        sample = [rng.choice(trials) for _ in trials]
        labels = [t["label"] for t in sample]
        values = {name: auc(labels, [t[name] for t in sample]) for name in ("logreg", "points")}
        if None in values.values():
            continue
        stats["logreg"].append(values["logreg"])
        stats["points"].append(values["points"])
        stats["difference"].append(values["logreg"] - values["points"])
    def interval(values: list[float]) -> tuple[float, float]:
        values = sorted(values)
        return values[int(0.025 * len(values))], values[int(0.975 * len(values)) - 1]
    return {name: interval(values) for name, values in stats.items()}


def comparison_rows(outcomes: list[dict], label: str) -> list[str]:
    logreg = summarize(outcomes, "logreg")
    points = summarize(outcomes, "points")
    at_base = sum(1 for o in outcomes if o["model_name"] == "logreg" and o["at_base_rate"])
    return [
        f"| {label} | logistic regression | {logreg['n']} | {fmt(logreg['observed'])} | "
        f"{fmt(logreg['mean_prediction'])} | {fmt(logreg['log_loss'])} | {fmt(logreg['auc'])} | "
        f"{fmt(logreg['top_decile_lift'], 2)} |",
        f"| {label} | points baseline | {points['n']} | {fmt(points['observed'])} | "
        f"{fmt(points['mean_prediction'])} | {fmt(points['log_loss'])} | {fmt(points['auc'])} | "
        f"{fmt(points['top_decile_lift'], 2)} |",
        f"| {label} | constant (observed rate) | {logreg['n']} | {fmt(logreg['observed'])} | "
        f"{fmt(logreg['observed'])} | {fmt(logreg['constant_log_loss'])} | 0.500 | 1.00 |",
        f"| {label} | (scored at base rate: {at_base} of {logreg['n']}) | | | | | | |",
    ]


def write_model_report() -> None:
    from saas_stream.db import admin_connection
    from saas_stream.model import LEARNER, SCORING_DAY, TrialModel

    config = settings()
    cutoff = datetime.combine(config.source_cutoff, time(), tzinfo=UTC)
    model = TrialModel(phase="warm", learner=LEARNER)
    outcomes = replay(model, prepared_rows(TrialModel.tables))
    warm = [o for o in outcomes if o["source_event_ts"] < cutoff]
    holdout = [o for o in outcomes if o["source_event_ts"] >= cutoff]
    warm_trials = [o["account_id"] for o in warm if o["model_name"] == "logreg"]
    late_ids = set(warm_trials[int(0.7 * len(warm_trials)):])
    warm_late = [o for o in warm if o["account_id"] in late_ids]
    ci = bootstrap_auc(holdout)
    holdout_auc = {name: summarize(holdout, name)["auc"] for name in ("logreg", "points")}

    with admin_connection() as conn:
        db = [
            dict(zip(("model_name", "phase", "label", "probability", "source_event_ts"), r,
                     strict=True))
            for r in conn.execute(
                "SELECT model_name, phase, label, probability, source_event_ts "
                "FROM ops.trial_outcomes ORDER BY outcome_id"
            ).fetchall()
        ]  # fmt: skip
    weights = model.logreg["LogisticRegression"].weights
    top_weights = sorted(weights.items(), key=lambda kv: -abs(kv[1]))[:8]
    header = [
        "| Slice | Model | n | Observed | Mean prediction | Log loss | AUC | Top-decile lift |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    secondary = [
        "| Slice | n | Observed | Mean prediction | Log loss | Constant log loss | AUC "
        "| Top-decile lift |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    lines = [
        "# Trial model evaluation (task 4.5)",
        "",
        f"Generated {datetime.now(UTC):%Y-%m-%d %H:%M} UTC by `make evaluate-model`.",
        "",
        "## Protocol",
        "",
        f"- Each trial is scored with its latest prediction on or before day {SCORING_DAY} of the",
        "  trial. A trial with no prediction by then (no activity yet) is scored at the running",
        "  base rate of trials resolved so far, for both models. The label arrives at trial end",
        "  (about day 14 to 17), so the scoring time does not depend on the label.",
        "- Prequential: a trial is scored when its label arrives, then the logistic regression",
        f"  learns from the same day-{SCORING_DAY} vector it was scored on.",
        "- Learner: StandardScaler and LogisticRegression; log1p count features, days into trial",
        f"  as a fraction of 14, Adam (learning rate {LEARNER.learning_rate}), L2 {LEARNER.l2}.",
        "- Warm start: every trial resolved before the cutoff "
        f"({config.source_cutoff}); the table uses its last 30%, after the model has warmed up.",
        f"- Holdout: the {summarize(holdout, 'logreg')['n']} trials resolved after the cutoff, "
        "replayed from prepared data with the same code (a 5-minute demo take resolves about",
        "  one trial, too few to evaluate).",
        "- Constant: always predicts the slice's observed conversion rate. It knows that rate in",
        "  advance, so it is a strict reference for log loss.",
        "",
        "## Results",
        "",
        *header,
        *comparison_rows(warm_late, "warm start, last 30%"),
        *comparison_rows(holdout, "holdout after cutoff"),
        "",
        "Holdout AUC, bootstrap 95% CI (2,000 resamples of trials, both models on the same draw):",
        "",
        "| | AUC | 95% CI |",
        "|---|---:|---:|",
        f"| logistic regression | {fmt(holdout_auc['logreg'])} | "
        f"{ci['logreg'][0]:.3f} to {ci['logreg'][1]:.3f} |",
        f"| points baseline | {fmt(holdout_auc['points'])} | "
        f"{ci['points'][0]:.3f} to {ci['points'][1]:.3f} |",
        f"| difference | {holdout_auc['logreg'] - holdout_auc['points']:+.3f} | "
        f"{ci['difference'][0]:+.3f} to {ci['difference'][1]:+.3f} |",
        "",
        "## Other slices (logistic regression)",
        "",
        *secondary,
    ]
    for label, rows in (
        ("warm start, all", warm),
        ("warm start, before 2026-04-08", [o for o in warm if o["source_event_ts"] < PRICE_CHANGE]),
        ("warm start, from 2026-04-08", [o for o in warm if o["source_event_ts"] >= PRICE_CHANGE]),
    ):  # fmt: skip
        lines.append(summary_line(label, summarize(rows, "logreg")))
    lines += [
        "",
        "## What the running system produced",
        "",
        "From `ops.trial_outcomes` (baseline warm start, and demo takes since the last reset).",
        "",
        *secondary,
    ]
    for name in ("logreg", "points"):
        for phase in ("warm", "live"):
            phase_rows = [o for o in db if o["phase"] == phase]
            lines.append(summary_line(f"{name}, {phase}", summarize(phase_rows, name)))
    lines += [
        "",
        "## Warm start updates the weights",
        "",
        f"After the warm start the logistic regression has {len(weights)} weights, "
        f"L1 norm {sum(abs(v) for v in weights.values()):.2f} (from 0). Largest:",
        "",
        "| Feature | Weight |",
        "|---|---:|",
        *[f"| {k} | {v:+.3f} |" for k, v in top_weights],
        "",
        "## Rolling AUC",
        "",
        "![Rolling AUC](../img/model_rolling_auc.svg)",
        "",
        "Rolling AUC over the last 100 resolved trials. Dashed lines mark 2026-04-08 (new",
        f"signups move to the current price version) and the cutoff ({config.source_cutoff}).",
        "",
        "## Why day-7 scoring",
        "",
        "The first protocol scored each trial at its last prediction before the label. Converted",
        "trials were scored slightly later on average (day 13.4 vs 13.2), so the model learned",
        "label timing through days into trial: AUC 0.707 on the late warm start fell to 0.604",
        "with that feature held constant. Scoring at a fixed day removes the effect.",
        "",
    ]
    RESULTS.mkdir(parents=True, exist_ok=True)
    IMAGES.mkdir(parents=True, exist_ok=True)
    (RESULTS / "model.md").write_text("\n".join(lines))
    chart = svg_chart(
        {name: rolling_series(outcomes, name) for name in ("logreg", "points")},
        {"2026-04-08 price change": PRICE_CHANGE, "cutoff": cutoff},
    )
    (IMAGES / "model_rolling_auc.svg").write_text(chart + "\n")
    print(f"wrote {RESULTS / 'model.md'} and {IMAGES / 'model_rolling_auc.svg'}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=["outliers", "model"])
    args = parser.parse_args()
    if args.target == "outliers":
        write_outlier_report()
    else:
        write_model_report()


if __name__ == "__main__":
    main()
