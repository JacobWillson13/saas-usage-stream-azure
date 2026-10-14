from __future__ import annotations

import unittest
from datetime import date

from saas_stream.live import build_take
from saas_stream.settings import SCRIPTED

DAY = date(2026, 9, 4)
PARENT_KEYS = {"accounts": "account_id", "users": "user_id", "devices": "device_id"}


def fingerprint(events) -> list:
    return [(round(e.t, 6), e.table, sorted((k, str(v)) for k, v in e.row.items())) for e in events]


class LiveTakeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.events = build_take(DAY, 32, "test", 500)

    def test_same_seed_same_take(self) -> None:
        again = build_take(DAY, 32, "test", 500)
        self.assertEqual(fingerprint(self.events), fingerprint(again))

    def test_other_seed_other_order(self) -> None:
        other = build_take(DAY, 32, "other", 500)
        self.assertNotEqual(fingerprint(self.events[:50]), fingerprint(other[:50]))

    def test_rate_and_clock(self) -> None:
        first_five = [e for e in self.events if e.t <= 300]
        self.assertTrue(2300 <= len(first_five) <= 2800, len(first_five))
        times = [e.t for e in self.events]
        self.assertEqual(times, sorted(times))
        sources = [e.row["source_event_ts"] for e in self.events if e.table == "sessions"]
        self.assertTrue(all(ts.date() == DAY for ts in sources))

    def test_parents_before_children(self) -> None:
        emitted_today = {
            (table, e.row[key]) for e in self.events for table, key in PARENT_KEYS.items()
            if e.table == table
        }  # fmt: skip
        seen = set()
        for e in self.events:
            for table, key in PARENT_KEYS.items():
                parent = (table, e.row.get(key))
                if e.table != table and parent in emitted_today:
                    self.assertIn(parent, seen, f"{e.table} before its {table}")
            if e.table in PARENT_KEYS:
                seen.add((e.table, e.row[PARENT_KEYS[e.table]]))

    def test_scripted_anomalies_on_schedule(self) -> None:
        scripted = [
            (e.t, a["anomaly_type"]) for e in self.events for a in e.anomalies if a["scripted"]
        ]
        self.assertEqual([kind for _, kind in scripted], [k for _, k in sorted(SCRIPTED.items())])
        for (t, _), second in zip(scripted, sorted(SCRIPTED), strict=True):
            self.assertTrue(second <= t < second + 5, (t, second))


if __name__ == "__main__":
    unittest.main()
