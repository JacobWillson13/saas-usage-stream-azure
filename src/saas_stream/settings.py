"""Runtime settings from .env, with defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from functools import cache
from pathlib import Path

import duckdb
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
PREPARED = ROOT / "data" / "prepared"
CHECKPOINTS = ROOT / "data" / "checkpoints"

# Random injection: about 1 anomaly per 2,000 rows in the baseline (SPEC 6),
# and a low background rate during the demo on top of the scripted anomalies.
BASELINE_INJECT_RATE = 1 / 2000
DEMO_INJECT_RATE = 1 / 5000
INJECT_SEED = "saas-stream-v1"

# Scripted demo anomalies: seconds into the take.
SCRIPTED = {
    60: "session_spike",
    120: "duplicate_payment",
    180: "negative_invoice",
    240: "failure_burst",
}


@dataclass(frozen=True)
class Settings:
    source_cutoff: date
    demo_date: date
    demo_rate: float  # events per minute
    demo_seed: str

    @property
    def offset_days(self) -> int:
        return (self.demo_date - self.source_cutoff).days


def settings() -> Settings:
    load_dotenv(ROOT / ".env")
    return Settings(
        source_cutoff=date.fromisoformat(os.getenv("SOURCE_CUTOFF") or "2026-09-04"),
        demo_date=date.fromisoformat(os.getenv("DEMO_DATE") or date.today().isoformat()),
        demo_rate=float(os.getenv("DEMO_RATE") or "500"),
        demo_seed=os.getenv("DEMO_SEED") or "1",
    )


def parquet_path(table: str) -> Path:
    path = PREPARED / f"{table}.parquet"
    if not path.exists():
        raise RuntimeError(f"missing prepared file: {path}")
    return path


@cache
def data_end() -> datetime:
    """Last source event in the prepared data (naive UTC)."""
    from saas_stream.rows import TABLE_NAMES

    files = [str(parquet_path(table)) for table in TABLE_NAMES]
    query = f"SELECT max(event_ts) FROM read_parquet({files!r}, union_by_name = true)"
    return duckdb.sql(query).fetchone()[0]
