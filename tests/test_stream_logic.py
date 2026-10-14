from __future__ import annotations

import unittest
from datetime import UTC, date, datetime, timedelta

from saas_stream.inject import Injector, hour_key
from saas_stream.model import auc, log_loss, top_decile_lift
from saas_stream.outliers import OutlierScorer, robust_z
from saas_stream.rows import shift_row


class MetricsTest(unittest.TestCase):
    def test_auc(self) -> None:
        self.assertEqual(auc([0, 0, 1, 1], [0.1, 0.2, 0.8, 0.9]), 1.0)
        self.assertEqual(auc([1, 1, 0, 0], [0.1, 0.2, 0.8, 0.9]), 0.0)
        self.assertEqual(auc([0, 1], [0.5, 0.5]), 0.5)  # ties count half
        self.assertIsNone(auc([1, 1], [0.2, 0.3]))

    def test_log_loss_and_lift(self) -> None:
        self.assertAlmostEqual(log_loss([1, 0], [0.5, 0.5]), 0.6931, places=4)
        labels = [1] + [0] * 9
        self.assertEqual(top_decile_lift(labels, [0.9] + [0.1] * 9), 10.0)


class RobustZTest(unittest.TestCase):
    def test_floor_applies_when_mad_is_zero(self) -> None:
        z, median = robust_z(30, [3] * 10, floor=1.0)
        self.assertEqual((z, median), (27.0, 3))
        self.assertLess(robust_z(6, [3] * 10, floor=1.0)[0], 3.5)


class FailureRateTest(unittest.TestCase):
    def payment(self, minute: int, status: str) -> dict:
        ts = datetime(2026, 9, 1, 18, minute, tzinfo=UTC)
        return {
            "payment_id": f"pay_{status}_{minute}", "account_id": "acc_1", "invoice_id": "inv_1",
            "amount": 10, "status": status, "is_test": False, "source_event_ts": ts, "event_ts": ts,
        }  # fmt: skip

    def test_all_failed_history_does_not_divide_by_zero(self) -> None:
        scorer = OutlierScorer()
        for minute in range(10):  # an hour where every payment failed
            scorer.process("payments", self.payment(minute, "failed"))
        later = dict(self.payment(0, "failed"), payment_id="pay_next")
        later["source_event_ts"] = later["event_ts"] = later["source_event_ts"] + timedelta(hours=2)
        for n in range(6):
            scorer.process("payments", dict(later, payment_id=f"pay_next_{n}"))

    def test_one_failure_in_a_few_payments_does_not_flag(self) -> None:
        scorer = OutlierScorer()
        for minute, status in enumerate(["succeeded"] * 4 + ["failed"]):
            scorer.process("payments", self.payment(minute, status))
        self.assertFalse([f for f in scorer.flags if f["flag_type"] == "failure_rate"])

    def test_burst_flags(self) -> None:
        scorer = OutlierScorer()
        for minute in range(20):
            scorer.process("payments", self.payment(minute, "succeeded"))
        for minute in range(20, 40):
            scorer.process("payments", self.payment(minute, "failed"))
        self.assertTrue([f for f in scorer.flags if f["flag_type"] == "failure_rate"])


class ShiftTest(unittest.TestCase):
    def test_shift_keeps_source_time(self) -> None:
        row = {"event_ts": datetime(2026, 9, 3, 12), "activity_date": date(2026, 9, 3), "n": 1}
        shifted = shift_row(row, 35)
        self.assertEqual(shifted["event_ts"], datetime(2026, 10, 8, 12, tzinfo=UTC))
        self.assertEqual(shifted["source_event_ts"], datetime(2026, 9, 3, 12, tzinfo=UTC))
        self.assertEqual(shifted["activity_date"], date(2026, 10, 8))
        self.assertEqual(shifted["n"], 1)


class InjectorTest(unittest.TestCase):
    def payment(self, n: int) -> dict:
        ts = datetime(2026, 9, 5, 22, tzinfo=UTC) + timedelta(minutes=n)
        return {
            "payment_id": f"pay_{n}", "account_id": "acc_1", "invoice_id": "inv_1",
            "status": "succeeded", "amount": 10, "source_event_ts": ts, "event_ts": ts,
        }  # fmt: skip

    def test_same_input_same_anomalies(self) -> None:
        rates = {"duplicate_payment": 0.3, "failure_burst": 0.3}
        runs = []
        for _ in range(2):
            injector = Injector(rates, "seed")
            out = [injector.random("payments", self.payment(n)) for n in range(50)]
            runs.append([(len(extra), [a["row_key"] for a in found]) for extra, found in out])
        self.assertEqual(runs[0], runs[1])
        self.assertTrue(any(found for _, found in runs[0]))

    def test_failure_burst_stays_in_one_hour(self) -> None:
        injector = Injector({}, "seed")
        at = datetime(2026, 9, 5, 22, 59, 59, tzinfo=UTC)
        rows, anomalies = injector.failure_burst(self.payment(0), at, at, "t", True)
        self.assertEqual(len(rows), 20)
        self.assertEqual({hour_key(r["source_event_ts"]) for r in rows}, {hour_key(at)})
        self.assertTrue(all(r["status"] == "failed" for r in rows))
        self.assertEqual(anomalies[0]["row_key"], hour_key(at))


if __name__ == "__main__":
    unittest.main()
