"""Resettable live demo.

  baseline  load history before SOURCE_CUTOFF (shifted, with random injection), run the
            outlier scorer and model over it in fast mode, and save a checkpoint
  demo      run the live emitter, scorer, and model together until Ctrl-C
  reset     remove demo rows and every ops row written after the checkpoint
"""

from __future__ import annotations

import argparse
import json
import signal
import threading
import time
from datetime import UTC, datetime, timedelta

from saas_stream.backfill import load_baseline
from saas_stream.consumer import max_ingest_id, run_fast, run_live, set_offset
from saas_stream.db import stream_pool
from saas_stream.model import TrialModel
from saas_stream.outliers import OutlierScorer
from saas_stream.replay import LiveEmitter
from saas_stream.rows import APP_TABLES
from saas_stream.settings import CHECKPOINTS, settings

# ops output tables and their id columns; all insert-only (reset deletes above the mark)
OPS_IDS = {
    "ops.outlier_flags": "flag_id",
    "ops.predictions": "prediction_id",
    "ops.trial_outcomes": "outcome_id",
    "ops.model_metrics": "metric_id",
    "ops.injected_anomalies": "anomaly_id",
    "ops.ingest_log": "batch_id",
}
CONSUMERS = ["outliers", "model"]
LIVE_HEARTBEAT_SECONDS = 15


def log(message: str) -> None:
    print(f"{datetime.now(UTC):%H:%M:%S} {message}", flush=True)


def install_stop(stop: threading.Event) -> None:
    """First Ctrl-C (or SIGTERM) asks every component to finish; a second one aborts."""

    def handle(signum, frame) -> None:
        if stop.is_set():
            raise KeyboardInterrupt
        log("stopping: finishing current batch and polls (Ctrl-C again to abort)")
        stop.set()

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)


def baseline() -> None:
    config = settings()
    started = time.monotonic()
    log(
        f"baseline: rows before {config.source_cutoff}, shifted +{config.offset_days} days "
        f"so history ends {config.demo_date - timedelta(days=1)}"
    )
    pool = stream_pool(max_size=2)
    try:
        load_baseline(pool, config)
        log(f"baseline: loaded in {time.monotonic() - started:.0f} s; scoring history")
        with pool.connection() as conn:
            top = max_ingest_id(conn)
        scorer = OutlierScorer()
        model = TrialModel(phase="warm")
        scored = time.monotonic()
        run_fast(pool, [scorer, model], top)
        log(f"baseline: scorer and model done in {time.monotonic() - scored:.0f} s")

        CHECKPOINTS.mkdir(parents=True, exist_ok=True)
        model.save(CHECKPOINTS / "model.pkl")
        with pool.connection() as conn:
            marks = {
                table: conn.execute(f"SELECT coalesce(max({column}), 0) FROM {table}").fetchone()[0]
                for table, column in OPS_IDS.items()
            }
            summary = {
                table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in OPS_IDS
            }
            parts = " UNION ALL ".join(
                f"SELECT max(event_ts) AS ts FROM app.{t}" for t in APP_TABLES
            )
            latest = conn.execute(f"SELECT max(ts) FROM ({parts}) s").fetchone()[0]
        checkpoint = {
            "created_at": datetime.now(UTC).isoformat(),
            "demo_date": config.demo_date.isoformat(),
            "source_cutoff": config.source_cutoff.isoformat(),
            "offset_days": config.offset_days,
            "offsets": {name: top for name in CONSUMERS},
            "high_water": marks,
        }
        (CHECKPOINTS / "checkpoint.json").write_text(json.dumps(checkpoint, indent=2) + "\n")
        log(f"baseline: ops rows {summary}")
        log(f"baseline: latest event_ts {latest:%Y-%m-%d %H:%M}, checkpoint saved")
    finally:
        pool.close()
    log(f"baseline: runtime_seconds={time.monotonic() - started:.1f}")


def demo() -> None:
    config = settings()
    stop = threading.Event()
    install_stop(stop)
    pool = stream_pool(max_size=4)
    emitter = LiveEmitter(pool, config, log)
    errors: list[BaseException] = []

    def guarded(name: str, target, *args) -> threading.Thread:
        def run() -> None:
            try:
                target(*args)
            except BaseException as error:  # noqa: BLE001 - report and stop the others
                errors.append(error)
                log(f"{name}: failed: {error!r}")
                stop.set()

        return threading.Thread(target=run, name=name)

    workers = [
        guarded("emitter", emitter.run, stop),
        guarded("outliers", run_live, pool, OutlierScorer(), stop, log),
        guarded("model", run_live, pool, TrialModel(phase="live"), stop, log),
    ]
    for worker in workers:
        worker.start()
    try:
        while any(worker.is_alive() for worker in workers):
            time.sleep(0.2)
    finally:
        stop.set()
        for worker in workers:
            worker.join()
        pool.close()
    if errors:
        raise SystemExit(1)
    log("demo: stopped cleanly")


def reset() -> None:
    started = time.monotonic()
    checkpoint = json.loads((CHECKPOINTS / "checkpoint.json").read_text())
    pool = stream_pool(max_size=1)
    try:
        with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            status, age = cur.execute(
                "SELECT status, extract(epoch FROM now() - updated_at) FROM ops.replay_state"
            ).fetchone()
            if status == "running" and age < LIVE_HEARTBEAT_SECONDS:
                raise SystemExit("demo-reset: a demo is running; stop it with Ctrl-C first")
            deleted = {}
            for table in reversed(APP_TABLES):  # children first
                cur.execute(f"DELETE FROM app.{table} WHERE is_demo")
                deleted[table] = cur.rowcount
            for table, column in OPS_IDS.items():
                cur.execute(
                    f"DELETE FROM {table} WHERE {column} > %s", (checkpoint["high_water"][table],)
                )
                deleted[table] = cur.rowcount
            cur.execute("DELETE FROM ops.consumer_offsets")
            for name, offset in checkpoint["offsets"].items():
                set_offset(cur, name, offset)
            cur.execute(
                """
                UPDATE ops.replay_state
                SET status = 'baseline', cursor_index = 0, cursor_ts = NULL, demo_batches = 0,
                    updated_at = now()
                """
            )
    finally:
        pool.close()
    removed = {table: n for table, n in deleted.items() if n}
    log(f"demo-reset: removed {removed or 'nothing'}; offsets and cursor restored")
    log("demo-reset: the model restarts from data/checkpoints/model.pkl on the next demo")
    log(f"demo-reset: runtime_seconds={time.monotonic() - started:.1f}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["baseline", "demo", "reset"])
    command = parser.parse_args().command
    {"baseline": baseline, "demo": demo, "reset": reset}[command]()


if __name__ == "__main__":
    main()
