"""Engine guarantees, tested against temporary SQLite files."""

import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intake_workbench.engine import (  # noqa: E402
    IntakeEngine,
    REASON_MASTER_ARRIVED,
    REASON_ORDER_NOT_ARRIVED,
    REASON_TIMEOUT,
    _iso,
)


def order(oid, version=1, op="CREATE", **kw):
    item = {"entity_type": "order", "order_id": oid, "op": op, "version": version}
    item.update(kw)
    return item


def line(lid, oid, amount=10.0, version=1):
    return {"entity_type": "order_line", "line_id": lid, "order_id": oid,
            "sku": "SKU", "qty": 1, "amount": amount, "version": version}


def pay(pid, oid, amount=10.0, version=1):
    return {"entity_type": "payment", "payment_id": pid, "order_id": oid,
            "method": "CARD", "amount": amount, "version": version}


class EngineTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "test.db")
        self.engine = IntakeEngine(self.db, timeout_seconds=300)

    def tearDown(self):
        self.tmp.cleanup()

    def line(self, lid):
        return next(r for r in self.engine.list_lines() if r["line_id"] == lid)

    def payment(self, pid):
        return next(r for r in self.engine.list_payments()
                    if r["payment_id"] == pid)

    def order(self, oid):
        return next(r for r in self.engine.list_orders()
                    if r["order_id"] == oid)


class TestCorrelation(EngineTestBase):
    def test_detail_waits_then_auto_links(self):
        report = self.engine.ingest_batch("B1", [line("L1", "O1"), pay("P1", "O1")])
        self.assertEqual(report["applied"], 2)
        waiting = self.engine.list_waiting(active_only=True)
        self.assertEqual(len(waiting), 2)
        self.assertEqual(waiting[0]["reason_code"], REASON_ORDER_NOT_ARRIVED)
        self.assertEqual(self.line("L1")["state"], "WAITING")

        report = self.engine.ingest_batch("B2", [order("O1")])
        self.assertEqual(report["linked_from_waiting"], 2)
        self.assertEqual(self.line("L1")["state"], "LINKED")
        self.assertEqual(self.payment("P1")["state"], "LINKED")
        self.assertEqual(self.engine.list_waiting(active_only=True), [])

    def test_same_batch_order_then_line_links_directly(self):
        report = self.engine.ingest_batch("B1", [order("O1"), line("L1", "O1")])
        self.assertEqual(self.line("L1")["state"], "LINKED")
        self.assertEqual(report["linked_from_waiting"], 0)

    def test_line_after_cancel_is_voided(self):
        self.engine.ingest_batch("B1", [order("O1")])
        self.engine.ingest_batch("B2", [order("O1", version=2, op="CANCEL")])
        self.engine.ingest_batch("B3", [line("L1", "O1")])
        self.assertEqual(self.line("L1")["state"], "CANCELLED")
        # nothing can ever be published for a cancelled order
        self.assertEqual(self.engine.publish()["published"], False)


class TestTimeout(EngineTestBase):
    def test_timeout_moves_to_exception_but_keeps_data(self):
        old = _iso(time.time() - 400)
        self.engine.ingest_batch("B1", [line("L1", "O1")], received_at=old)
        result = self.engine.sweep_timeouts()
        self.assertEqual(result["timed_out"], 1)
        self.assertEqual(self.line("L1")["state"], "EXCEPTION")
        waiting = self.engine.list_waiting(active_only=True)
        self.assertEqual(waiting[0]["state"], "EXCEPTION")
        self.assertEqual(waiting[0]["reason_code"], REASON_TIMEOUT)

        # master arrives afterwards: exception recovers, data was not deleted
        report = self.engine.ingest_batch("B2", [order("O1")])
        self.assertEqual(report["linked_from_waiting"], 1)
        self.assertEqual(self.line("L1")["state"], "LINKED")
        events = [e["event"] for e in self.engine.lineage("order_line", "L1")]
        self.assertIn("WAIT_TIMEOUT", events)
        self.assertIn("DETAIL_LINKED", events)

    def test_young_wait_is_not_swept(self):
        self.engine.ingest_batch("B1", [line("L1", "O1")])
        self.assertEqual(self.engine.sweep_timeouts()["timed_out"], 0)
        self.assertEqual(self.line("L1")["state"], "WAITING")


class TestIdempotency(EngineTestBase):
    def test_duplicate_batch_ignored_entirely(self):
        first = self.engine.ingest_batch("B1", [order("O1")])
        second = self.engine.ingest_batch("B1", [order("O1")])
        self.assertEqual(first["status"], "NEW")
        self.assertEqual(second["status"], "DUPLICATE")
        orders = self.engine.list_orders()
        self.assertEqual(len(orders), 1)
        self.assertEqual(len(self.engine.list_batches()), 2)  # raw dup retained

    def test_same_business_version_in_different_batch_counts_once(self):
        self.engine.ingest_batch("B1", [order("O1")])
        report = self.engine.ingest_batch("B2", [order("O1")])
        self.assertEqual(len(report["duplicate_versions"]), 1)
        self.assertEqual(len(self.engine.list_orders()), 1)
        versions = self.engine._conn().execute(
            "SELECT COUNT(*) c FROM biz_versions").fetchone()["c"]
        self.assertEqual(versions, 1)

    def test_duplicate_cancel_version_counts_once(self):
        self.engine.ingest_batch("B1", [order("O1")])
        self.engine.ingest_batch("B2", [order("O1", version=2, op="CANCEL")])
        again = self.engine.ingest_batch("B3", [order("O1", version=2, op="CANCEL")])
        self.assertEqual(len(again["duplicate_versions"]), 1)
        events = self.engine._conn().execute(
            "SELECT COUNT(*) c FROM order_events WHERE op='CANCEL'").fetchone()["c"]
        self.assertEqual(events, 1)

    def test_redelivered_line_version_does_not_duplicate(self):
        self.engine.ingest_batch("B1", [order("O1")])
        self.engine.ingest_batch("B2", [line("L1", "O1", amount=10.0)])
        again = self.engine.ingest_batch("B3", [line("L1", "O1", amount=10.0)])
        self.assertEqual(len(again["duplicate_versions"]), 1)
        self.assertEqual(len(self.engine.list_lines()), 1)


class TestCancelBeforeCreate(EngineTestBase):
    def test_marker_waits_then_cancels_new_order(self):
        self.engine.ingest_batch("B1", [order("O1", version=2, op="CANCEL")])
        self.assertEqual(self.order_exists("O1"), False)
        report = self.engine.ingest_batch("B2", [order("O1", version=1)])
        self.assertEqual(report["markers_consumed"], 1)
        self.assertEqual(self.order("O1")["state"], "CANCELLED")
        self.assertEqual(self.order("O1")["cancel_batch_id"], "B1")

    def test_timed_out_marker_still_honoured(self):
        old = _iso(time.time() - 400)
        self.engine.ingest_batch("B1", [order("O1", version=2, op="CANCEL")],
                                 received_at=old)
        self.assertEqual(self.engine.sweep_timeouts()["timed_out"], 1)
        self.engine.ingest_batch("B2", [order("O1", version=1)])
        self.assertEqual(self.order("O1")["state"], "CANCELLED")

    def order_exists(self, oid):
        return any(r["order_id"] == oid for r in self.engine.list_orders())


class TestReversal(EngineTestBase):
    def _publish_one_order(self):
        self.engine.ingest_batch("B1", [
            order("O1"), line("L1", "O1"), line("L2", "O1"), pay("P1", "O1")])
        pub = self.engine.publish()
        self.assertTrue(pub["published"])
        return pub["publish_id"]

    def test_reversal_carries_source_publication(self):
        publish_id = self._publish_one_order()
        report = self.engine.ingest_batch(
            "B2", [order("O1", version=2, op="CANCEL")])
        self.assertEqual(report["reversals_created"], 3)  # 2 lines + 1 payment
        revs = self.engine.list_reversals()
        self.assertTrue(all(r["state"] == "PENDING" for r in revs))
        self.assertTrue(all(r["source_publish_id"] == publish_id for r in revs))
        self.assertTrue(all(r["batch_id"] == "B2" for r in revs))
        second = self.engine.publish()
        self.assertEqual(second["reversals"], 3)
        published = self.engine.list_published()["reversals"]
        self.assertEqual(len(published), 3)
        self.assertTrue(all(r["publish_id"] == second["publish_id"] for r in published))
        # cancelling twice must not create duplicate reversals
        self.engine.ingest_batch("B3", [order("O1", version=3, op="CANCEL")])
        self.assertEqual(len(self.engine.list_reversals()), 3)

    def test_unpublished_linked_details_get_no_reversal(self):
        self.engine.ingest_batch("B1", [order("O1"), line("L1", "O1")])
        # no publish before cancel
        self.engine.ingest_batch("B2", [order("O1", version=2, op="CANCEL")])
        self.assertEqual(self.engine.list_reversals(), [])
        self.assertEqual(self.line("L1")["state"], "CANCELLED")


class TestPublishConsistency(EngineTestBase):
    def test_order_and_details_publish_together(self):
        self.engine.ingest_batch("B1", [order("O1"), line("L1", "O1")])
        result = self.engine.publish()
        self.assertEqual(result["orders"], 1)
        self.assertEqual(result["lines"], 1)
        data = self.engine.list_published()
        self.assertEqual(data["orders"][0]["publish_id"],
                         data["lines"][0]["publish_id"])

    def test_incremental_publish_never_double_counts(self):
        self.engine.ingest_batch("B1", [order("O1")])
        self.engine.ingest_batch("B2", [line("L1", "O1")])
        first = self.engine.publish()
        self.assertEqual((first["orders"], first["lines"]), (1, 1))
        self.engine.ingest_batch("B3", [line("L2", "O1"), pay("P1", "O1")])
        second = self.engine.publish()
        self.assertEqual((second["orders"], second["lines"], second["payments"]),
                         (0, 1, 1))
        # nothing left -> idempotent no-op
        third = self.engine.publish()
        self.assertFalse(third["published"])
        data = self.engine.list_published()
        self.assertEqual(len(data["orders"]), 1)
        self.assertEqual(len(data["lines"]), 2)
        self.assertEqual(len(data["payments"]), 1)

    def test_publish_skips_orders_without_unpublished_details(self):
        self.engine.ingest_batch("B1", [order("O1"), line("L1", "O1")])
        self.engine.publish()
        # a brand new order with no details yet is not publishable
        self.engine.ingest_batch("B2", [order("O2")])
        result = self.engine.publish()
        self.assertFalse(result["published"])

    def test_waiting_detail_never_published_without_master(self):
        self.engine.ingest_batch("B1", [line("L1", "O1")])
        self.assertFalse(self.engine.publish()["published"])


class TestLineage(EngineTestBase):
    def test_each_final_record_lists_batches_it_passed_through(self):
        self.engine.ingest_batch("B1", [line("L1", "O1")],
                                 received_at=_iso(time.time() - 400))
        self.engine.sweep_timeouts()
        self.engine.ingest_batch("B2", [order("O1")])
        events = self.engine.lineage("order_line", "L1")
        names = [e["event"] for e in events]
        self.assertEqual(names,
                         ["DETAIL_PARKED", "WAIT_TIMEOUT", "DETAIL_LINKED"])
        batches = {e["batch_id"] for e in events if e["batch_id"]}
        self.assertEqual(batches, {"B1", "B2"})

    def test_batch_duplicate_is_explained_in_lineage(self):
        self.engine.ingest_batch("B1", [order("O1")])
        self.engine.ingest_batch("B1", [order("O1")])
        events = self.engine.lineage("batch", "B1")
        self.assertIn("BATCH_RECEIVED", [e["event"] for e in events])
        self.assertIn("BATCH_DUPLICATE", [e["event"] for e in events])


class TestReject(EngineTestBase):
    def test_malformed_item_is_rejected_not_crashing_batch(self):
        report = self.engine.ingest_batch("B1", [
            order("O1"),
            {"entity_type": "order_line"},  # missing fields
            {"entity_type": "mystery"},
        ])
        self.assertEqual(report["applied"], 1)
        self.assertEqual(len(report["rejected"]), 2)
        self.assertEqual(len(self.engine.list_orders()), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
