"""Core correlation engine.

All state changes happen inside SQLite transactions and every state change
appends a ``lineage`` row referencing the batch that caused it.  The engine is
single-process guarded by a re-entrant lock so the HTTP layer can call it from
worker threads.
"""

from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from . import store

# Reason codes used in the waiting area and in lineage details.
REASON_ORDER_NOT_ARRIVED = "ORDER_NOT_ARRIVED"
REASON_ORDER_CANCELLED = "ORDER_CANCELLED_BEFORE_DETAIL"
REASON_CANCEL_BEFORE_ORDER = "CANCEL_BEFORE_ORDER"
REASON_TIMEOUT = "TIMEOUT_EXCEPTION"
REASON_MASTER_ARRIVED = "MASTER_RECORD_ARRIVED"

DEFAULT_TIMEOUT_SECONDS = 300

# Human-readable labels shared by the web UI / reports.
LABELS = {
    "event": {
        "BATCH_RECEIVED": "批次接收",
        "BATCH_DUPLICATE": "批次重复投递(整批忽略)",
        "VERSION_SKIPPED_DUPLICATE": "业务版本重复(跳过,不重复计入)",
        "ORDER_CREATED": "订单创建",
        "ORDER_CANCELLED": "订单撤销",
        "MARKER_PARKED": "撤销先到,进入等待区",
        "DETAIL_PARKED": "主记录未到,进入等待区",
        "DETAIL_LINKED": "补关联成功",
        "DETAIL_WAIT_CANCELLED": "订单已撤销,等待明细作废",
        "WAIT_TIMEOUT": "等待超时,转入异常处理",
        "EXCEPTION_RECOVERED": "异常恢复,主记录补达",
        "REVERSAL_CREATED": "生成有来源撤销结果",
        "REVERSAL_PUBLISHED": "撤销结果发布",
        "PUBLISHED": "发布",
        "REJECTED": "条目拒收",
    },
    "reason": {
        REASON_ORDER_NOT_ARRIVED: "订单主记录尚未到达",
        REASON_ORDER_CANCELLED: "订单在明细到达前已撤销",
        REASON_CANCEL_BEFORE_ORDER: "撤销指令先于订单创建到达",
        REASON_TIMEOUT: "等待超时(仅进入异常处理,数据仍保留)",
        REASON_MASTER_ARRIVED: "订单主记录补达,自动补关联",
    },
    "state": {
        "WAITING": "等待中",
        "LINKED": "已关联",
        "ACTIVE": "有效",
        "CANCELLED": "已撤销",
        "EXCEPTION": "异常处理中",
        "RESOLVED": "已解决",
        "PENDING": "待发布",
        "PUBLISHED": "已发布",
        "NEW": "新批次",
        "DUPLICATE": "重复批次",
    },
}


def _iso(epoch: float | None = None) -> str:
    if epoch is None:
        epoch = time.time()
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def _epoch(iso_ts: str) -> float:
    return datetime.fromisoformat(iso_ts).timestamp()


class IntakeEngine:
    def __init__(self, db_path: str = "data/intake.db",
                 timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS):
        self.db_path = db_path
        self.timeout_seconds = timeout_seconds
        self._lock = threading.RLock()
        with self._conn() as conn:
            store.init_schema(conn)

    # ------------------------------------------------------------------ infra
    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    @contextmanager
    def _read(self):
        """One connection for a read-only operation, always closed."""
        with self._lock, self._conn() as conn:
            yield conn

    @contextmanager
    def _write(self):
        """One connection + one transaction for a state-changing operation."""
        with self._lock, self._conn() as conn, store.transaction(conn):
            yield conn

    def _lineage(self, conn: sqlite3.Connection, entity_type: str, entity_id: str,
                 event: str, detail: str, batch_id: str | None,
                 now: str) -> None:
        conn.execute(
            "INSERT INTO lineage(entity_type, entity_id, event, detail, batch_id, at)"
            " VALUES (?,?,?,?,?,?)",
            (entity_type, str(entity_id), event, detail, batch_id, now),
        )

    # =============================================================== ingestion
    def ingest_batch(self, batch_id: str, items: list[dict[str, Any]],
                     received_at: str | None = None) -> dict[str, Any]:
        """Persist one raw batch and apply every item it contains.

        Returns an explainable report: what was applied, what was deduplicated,
        what linked out of the waiting area and what was rejected.
        """
        now = received_at or _iso()
        report: dict[str, Any] = {
            "batch_id": batch_id,
            "received_at": now,
            "received_items": len(items),
            "applied": 0,
            "duplicate_versions": [],
            "rejected": [],
            "linked_from_waiting": 0,
            "markers_consumed": 0,
            "cancelled_orders": [],
            "reversals_created": 0,
        }
        with self._write() as conn:
            existing = conn.execute(
                "SELECT 1 FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if existing:
                # Duplicate delivery: keep the first copy, explain the second.
                conn.execute(
                    "INSERT INTO batches(batch_id, received_at, item_count, status, raw_payload)"
                    " VALUES (?,?,?,?,?)",
                    (batch_id + "#dup@" + now, now, len(items), "DUPLICATE",
                     store.dumps({"duplicate_of": batch_id, "items": items})),
                )
                self._lineage(conn, "batch", batch_id, "BATCH_DUPLICATE",
                              f"重复批次投递, {len(items)} 条全部不重复计入",
                              batch_id, now)
                report["status"] = "DUPLICATE"
                report["message"] = "批次重复投递,整批忽略,原始载荷已留痕"
                return report

            conn.execute(
                "INSERT INTO batches(batch_id, received_at, item_count, status, raw_payload)"
                " VALUES (?,?,?,?,?)",
                (batch_id, now, len(items), "NEW", store.dumps(items)),
            )
            self._lineage(conn, "batch", batch_id, "BATCH_RECEIVED",
                          f"批次入仓, 共 {len(items)} 条", batch_id, now)
            report["status"] = "NEW"

            for index, item in enumerate(items):
                try:
                    self._apply_item(conn, item, batch_id, now, report, index)
                except _RejectItem as exc:
                    report["rejected"].append(
                        {"index": index, "item": item, "reason": str(exc)})
                    self._lineage(conn, "batch", batch_id, "REJECTED",
                                  f"第 {index} 条拒收: {exc}", batch_id, now)
        return report

    def _apply_item(self, conn, item, batch_id, now, report, index) -> None:
        etype = item.get("entity_type")
        if etype == "order":
            self._apply_order(conn, item, batch_id, now, report)
        elif etype == "order_line":
            self._apply_line(conn, item, batch_id, now, report)
        elif etype == "payment":
            self._apply_payment(conn, item, batch_id, now, report)
        else:
            raise _RejectItem(f"未知 entity_type: {etype!r}")

    def _version_seen(self, conn, etype, eid, version) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM biz_versions WHERE entity_type=? AND entity_id=? AND version=?",
            (etype, eid, version)).fetchone()

    def _register_version(self, conn, etype, eid, version, batch_id, now) -> None:
        conn.execute(
            "INSERT INTO biz_versions(entity_type, entity_id, version, batch_id, applied_at)"
            " VALUES (?,?,?,?,?)",
            (etype, eid, version, batch_id, now))

    # ------------------------------------------------------------------ orders
    def _apply_order(self, conn, item, batch_id, now, report) -> None:
        order_id = item.get("order_id")
        version = item.get("version")
        op = (item.get("op") or "CREATE").upper()
        if not order_id or version is None:
            raise _RejectItem("order 缺少 order_id/version")

        if op == "CANCEL":
            seen = self._version_seen(conn, "order", order_id, version)
            if seen:
                report["duplicate_versions"].append(
                    {"entity_type": "order", "entity_id": order_id,
                     "version": version})
                self._lineage(conn, "order", order_id, "VERSION_SKIPPED_DUPLICATE",
                              f"撤销版本 v{version} 已处理过,不重复计入",
                              batch_id, now)
                return
            self._register_version(conn, "order", order_id, version, batch_id, now)
            order = conn.execute("SELECT * FROM orders WHERE order_id=?",
                                 (order_id,)).fetchone()
            if order is None:
                # Cancel-before-create: park a marker. It stays valid even if
                # the wait later times out.
                conn.execute(
                    "INSERT INTO waiting(wait_kind, entity_id, target_order, version,"
                    " batch_id, arrived_at, reason_code, state, payload)"
                    " VALUES ('CANCEL_MARKER', ?, ?, ?, ?, ?, ?, 'WAITING', ?)",
                    (order_id, order_id, version, batch_id, now,
                     REASON_CANCEL_BEFORE_ORDER, store.dumps(item)))
                self._lineage(conn, "order", order_id, "MARKER_PARKED",
                              f"撤销指令 v{version} 先于订单到达,进入等待区",
                              batch_id, now)
            else:
                self._cancel_order(conn, order_id, item, batch_id, now, report)
            return

        if op != "CREATE":
            raise _RejectItem(f"order 未知操作: {op}")

        # Idempotency boundary first: the same business version never counts
        # twice, even when redelivered after the order already exists.
        seen = self._version_seen(conn, "order", order_id, version)
        if seen:
            report["duplicate_versions"].append(
                {"entity_type": "order", "entity_id": order_id, "version": version})
            self._lineage(conn, "order", order_id, "VERSION_SKIPPED_DUPLICATE",
                          f"创建版本 v{version} 已处理过,不重复计入",
                          batch_id, now)
            return
        if conn.execute("SELECT 1 FROM orders WHERE order_id=?",
                        (order_id,)).fetchone():
            raise _RejectItem(
                f"订单 {order_id} 已存在且新批次版本号 v{version} 未登记,"
                " 疑似冲突 CREATE, 拒收")

        self._register_version(conn, "order", order_id, version, batch_id, now)
        conn.execute(
            "INSERT INTO orders(order_id, current_version, state, created_at,"
            " create_batch_id) VALUES (?, ?, 'ACTIVE', ?, ?)",
            (order_id, version, now, batch_id))
        conn.execute(
            "INSERT INTO order_events(order_id, version, op, batch_id, event_at, payload)"
            " VALUES (?,?, 'CREATE', ?, ?, ?)",
            (order_id, version, batch_id, now, store.dumps(item)))
        self._lineage(conn, "order", order_id, "ORDER_CREATED",
                      f"订单主记录到达 (创建批次 {batch_id}, v{version})",
                      batch_id, now)
        report["applied"] += 1

        # A cancel marker may have been waiting for this order.
        marker = conn.execute(
            "SELECT * FROM waiting WHERE wait_kind='CANCEL_MARKER' AND target_order=?"
            " AND state IN ('WAITING','EXCEPTION') ORDER BY id LIMIT 1",
            (order_id,)).fetchone()
        if marker:
            timed_out = marker["state"] == "EXCEPTION"
            conn.execute(
                "UPDATE waiting SET state='RESOLVED', resolved_at=?, resolved_by=?"
                " WHERE id=?", (now, batch_id, marker["id"]))
            self._lineage(conn, "order", order_id, "ORDER_CANCELLED",
                          f"先到的撤销指令{('(已超时转异常,仍被承认)' if timed_out else '')}"
                          f"生效, 来源批次 {marker['batch_id']}",
                          marker["batch_id"], now)
            report["markers_consumed"] += 1
            self._cancel_order(conn, order_id,
                               _json(marker["payload"]), marker["batch_id"],
                               now, report, retroactive=True)
        else:
            # Master arrived: drain every waiting detail, even ones that timed
            # out into exception handling — timeout is not "data missing".
            report["linked_from_waiting"] += self._drain_waiting_details(
                conn, order_id, batch_id, now)

    def _cancel_order(self, conn, order_id, item, batch_id, now, report,
                      retroactive: bool = False) -> None:
        order = conn.execute("SELECT * FROM orders WHERE order_id=?",
                             (order_id,)).fetchone()
        version = item["version"]
        conn.execute(
            "INSERT INTO order_events(order_id, version, op, batch_id, event_at, payload)"
            " VALUES (?,?, 'CANCEL', ?, ?, ?)",
            (order_id, version, batch_id, now, store.dumps(item)))
        if order["state"] == "CANCELLED":
            self._lineage(conn, "order", order_id, "ORDER_CANCELLED",
                          f"重复撤销 v{version}: 订单已是撤销态,事件留痕但状态不变",
                          batch_id, now)
            return
        conn.execute(
            "UPDATE orders SET state='CANCELLED', cancelled_at=?, cancel_batch_id=?,"
            " current_version=? WHERE order_id=?",
            (now, batch_id, version, order_id))
        origin = "先到撤销标记补生效" if retroactive else f"批次 {batch_id}"
        self._lineage(conn, "order", order_id, "ORDER_CANCELLED",
                      f"订单撤销 ({origin}, v{version},"
                      f" 原因: {item.get('reason', '未注明')})",
                      batch_id, now)
        report["cancelled_orders"].append(order_id)

        # Details still waiting will never link: cancel them in the waiting area.
        parked = conn.execute(
            "SELECT * FROM waiting WHERE wait_kind='DETAIL' AND target_order=?"
            " AND state IN ('WAITING','EXCEPTION')", (order_id,)).fetchall()
        for row in parked:
            conn.execute(
                "UPDATE waiting SET state='CANCELLED', resolved_at=?, resolved_by=?"
                " WHERE id=?", (now, batch_id, row["id"]))
            table = "order_lines" if row["entity_type"] == "order_line" else "payments"
            id_col = _id_col(row["entity_type"])
            conn.execute(
                f"UPDATE {table} SET state='CANCELLED' WHERE {id_col}=?",
                (row["entity_id"],))
            self._lineage(conn, row["entity_type"], row["entity_id"],
                          "DETAIL_WAIT_CANCELLED",
                          f"所属订单在关联前已撤销, 明细作废 (来源批次 {row['batch_id']})",
                          batch_id, now)

        # Already linked, already published details get sourced reversals.
        rev_count = self._create_reversals(conn, order_id, batch_id, now)
        report["reversals_created"] += rev_count
        # Linked but unpublished details become CANCELLED and will never publish.
        conn.execute(
            "UPDATE order_lines SET state='CANCELLED' WHERE order_id=? AND state='LINKED'",
            (order_id,))
        conn.execute(
            "UPDATE payments SET state='CANCELLED' WHERE order_id=? AND state='LINKED'",
            (order_id,))

    def _create_reversals(self, conn, order_id, cancel_batch_id, now) -> int:
        """One reversal per published detail row, carrying its source publish."""
        count = 0
        for etype, table, id_col in (
                ("order_line", "line_snapshots", "line_id"),
                ("payment", "payment_snapshots", "payment_id")):
            rows = conn.execute(
                f"SELECT s.* FROM {table} s WHERE s.order_id=?",
                (order_id,)).fetchall()
            already = {
                r["source_id"] for r in conn.execute(
                    "SELECT source_id FROM reversals WHERE order_id=?", (order_id,))
            }
            for row in rows:
                source_id = row[id_col]
                if source_id in already:
                    continue
                rev_id = "REV-" + uuid.uuid4().hex[:10]
                conn.execute(
                    "INSERT INTO reversals(reversal_id, order_id, source_entity,"
                    " source_id, source_publish_id, reason, state, batch_id, created_at)"
                    " VALUES (?,?,?,?,?,?,'PENDING',?,?)",
                    (rev_id, order_id, etype, source_id, row["publish_id"],
                     f"订单 {order_id} 撤销,回滚已发布明细", cancel_batch_id, now))
                self._lineage(conn, "reversal", rev_id, "REVERSAL_CREATED",
                              f"撤销 {etype} {source_id}, 来源发布 {row['publish_id']},"
                              f" 撤销指令批次 {cancel_batch_id}",
                              cancel_batch_id, now)
                count += 1
        return count

    # ------------------------------------------------------------- line/payment
    def _apply_line(self, conn, item, batch_id, now, report) -> None:
        line_id = item.get("line_id")
        order_id = item.get("order_id")
        version = item.get("version")
        for field in ("line_id", "order_id", "version", "sku", "qty", "amount"):
            if item.get(field) is None:
                raise _RejectItem(f"order_line 缺少字段 {field}")
        if self._version_seen(conn, "order_line", line_id, version):
            report["duplicate_versions"].append(
                {"entity_type": "order_line", "entity_id": line_id, "version": version})
            self._lineage(conn, "order_line", line_id, "VERSION_SKIPPED_DUPLICATE",
                          f"订单行版本 v{version} 已处理过,不重复计入",
                          batch_id, now)
            return
        self._register_version(conn, "order_line", line_id, version, batch_id, now)
        conn.execute(
            "INSERT INTO order_lines(line_id, order_id, sku, qty, amount, version,"
            " state, arrived_at, batch_id) VALUES (?,?,?,?,?,?,'WAITING',?,?)",
            (line_id, order_id, item["sku"], item["qty"], item["amount"],
             version, now, batch_id))
        report["applied"] += 1
        self._route_detail(conn, "order_line", line_id, order_id, item,
                           batch_id, now, report)

    def _apply_payment(self, conn, item, batch_id, now, report) -> None:
        payment_id = item.get("payment_id")
        order_id = item.get("order_id")
        version = item.get("version")
        for field in ("payment_id", "order_id", "version", "method", "amount"):
            if item.get(field) is None:
                raise _RejectItem(f"payment 缺少字段 {field}")
        if self._version_seen(conn, "payment", payment_id, version):
            report["duplicate_versions"].append(
                {"entity_type": "payment", "entity_id": payment_id,
                 "version": version})
            self._lineage(conn, "payment", payment_id, "VERSION_SKIPPED_DUPLICATE",
                          f"付款版本 v{version} 已处理过,不重复计入",
                          batch_id, now)
            return
        self._register_version(conn, "payment", payment_id, version, batch_id, now)
        conn.execute(
            "INSERT INTO payments(payment_id, order_id, method, amount, version,"
            " state, arrived_at, batch_id) VALUES (?,?,?,?,?,'WAITING',?,?)",
            (payment_id, order_id, item["method"], item["amount"], version,
             now, batch_id))
        report["applied"] += 1
        self._route_detail(conn, "payment", payment_id, order_id, item,
                           batch_id, now, report)

    def _route_detail(self, conn, etype, eid, order_id, item, batch_id, now,
                      report) -> None:
        """Link immediately when the master exists, otherwise park + wait."""
        order = conn.execute("SELECT * FROM orders WHERE order_id=?",
                             (order_id,)).fetchone()
        if order is None:
            conn.execute(
                "INSERT INTO waiting(wait_kind, entity_type, entity_id, target_order,"
                " version, batch_id, arrived_at, reason_code, state, payload)"
                " VALUES ('DETAIL', ?, ?, ?, ?, ?, ?, ?, 'WAITING', ?)",
                (etype, eid, order_id, item["version"], batch_id, now,
                 REASON_ORDER_NOT_ARRIVED, store.dumps(item)))
            self._lineage(conn, etype, eid, "DETAIL_PARKED",
                          f"找不到订单 {order_id}, 留在等待区 (批次 {batch_id})",
                          batch_id, now)
            return
        if order["state"] == "CANCELLED":
            conn.execute(
                "INSERT INTO waiting(wait_kind, entity_type, entity_id, target_order,"
                " version, batch_id, arrived_at, reason_code, state, payload)"
                " VALUES ('DETAIL', ?, ?, ?, ?, ?, ?, ?, 'CANCELLED', ?)",
                (etype, eid, order_id, item["version"], batch_id, now,
                 REASON_ORDER_CANCELLED, store.dumps(item)))
            table = "order_lines" if etype == "order_line" else "payments"
            id_col = "line_id" if etype == "order_line" else "payment_id"
            conn.execute(f"UPDATE {table} SET state='CANCELLED' WHERE {id_col}=?",
                         (eid,))
            self._lineage(conn, etype, eid, "DETAIL_WAIT_CANCELLED",
                          f"订单 {order_id} 已撤销, 明细直接作废", batch_id, now)
            return
        self._link_detail(conn, etype, eid, order_id, batch_id, now, report)

    def _link_detail(self, conn, etype, eid, order_id, batch_id, now,
                     report, recovered: bool = False) -> None:
        table = "order_lines" if etype == "order_line" else "payments"
        id_col = "line_id" if etype == "order_line" else "payment_id"
        conn.execute(
            f"UPDATE {table} SET order_id=?, state='LINKED', linked_at=? WHERE {id_col}=?",
            (order_id, now, eid))
        wait_row = conn.execute(
            "SELECT * FROM waiting WHERE wait_kind='DETAIL' AND entity_type=?"
            " AND entity_id=? ORDER BY id LIMIT 1", (etype, eid)).fetchone()
        if wait_row:
            note = "异常恢复后补关联" if wait_row["state"] == "EXCEPTION" else "等待区补关联"
            conn.execute(
                "UPDATE waiting SET state='RESOLVED', resolved_at=?, resolved_by=?"
                " WHERE id=?", (now, batch_id, wait_row["id"]))
            self._lineage(conn, etype, eid, "DETAIL_LINKED",
                          f"{note}: 订单 {order_id} 主记录到达 (批次 {batch_id})",
                          batch_id, now)
        else:
            self._lineage(conn, etype, eid, "DETAIL_LINKED",
                          f"直接关联订单 {order_id} (批次 {batch_id})",
                          batch_id, now)
        if report is not None:
            report["linked_from_waiting"] += 1 if wait_row else 0

    def _drain_waiting_details(self, conn, order_id, batch_id, now) -> int:
        rows = conn.execute(
            "SELECT entity_type, entity_id, state FROM waiting WHERE wait_kind='DETAIL'"
            " AND target_order=? AND state IN ('WAITING','EXCEPTION')",
            (order_id,)).fetchall()
        for row in rows:
            self._link_detail(conn, row["entity_type"], row["entity_id"],
                              order_id, batch_id, now, None,
                              recovered=row["state"] == "EXCEPTION")
        return len(rows)

    # =================================================================== sweep
    def sweep_timeouts(self, effective_now: str | None = None) -> dict[str, Any]:
        """Move overdue waiting rows into exception handling.

        Timeout never deletes data: EXCEPTION rows are still drained normally
        when their master record (or cancel marker target) appears later.
        ``effective_now`` lets the demo fast-forward the wall clock.
        """
        now = effective_now or _iso()
        now_epoch = _epoch(now)
        cutoff = now_epoch - self.timeout_seconds
        result = {"timed_out": 0, "recovered_after_timeout": 0}
        with self._write() as conn:
            rows = conn.execute(
                "SELECT * FROM waiting WHERE state='WAITING'").fetchall()
            for row in rows:
                if _epoch(row["arrived_at"]) <= cutoff:
                    conn.execute(
                        "UPDATE waiting SET state='EXCEPTION', resolved_at=?,"
                        " resolved_by='SWEEP_TIMEOUT', reason_code=? WHERE id=?",
                        (now, REASON_TIMEOUT, row["id"]))
                    if row["wait_kind"] == "DETAIL":
                        table = ("order_lines" if row["entity_type"] == "order_line"
                                 else "payments")
                        id_col = "line_id" if row["entity_type"] == "order_line" else "payment_id"
                        conn.execute(
                            f"UPDATE {table} SET state='EXCEPTION' WHERE {id_col}=?",
                            (row["entity_id"],))
                        self._lineage(
                            conn, row["entity_type"], row["entity_id"],
                            "WAIT_TIMEOUT",
                            f"等待 {self.timeout_seconds}s 超时,转入异常处理;"
                            f" 数据保留,主记录补达仍可恢复 (批次 {row['batch_id']})",
                            row["batch_id"], now)
                    else:
                        self._lineage(
                            conn, "order", row["target_order"], "WAIT_TIMEOUT",
                            "先到的撤销标记等待超时,转入异常处理; 订单到达时仍生效",
                            row["batch_id"], now)
                    result["timed_out"] += 1
        return result

    # ================================================================= publish
    def publish(self) -> dict[str, Any]:
        """Atomically publish consistent order groups and pending reversals.

        One transaction covers everything: if the process is interrupted
        mid-publish the commit never happens and re-running publishes the same
        logical groups again — no business version is counted twice.
        """
        with self._write() as conn:
            groups = self._collect_publish_groups(conn)
            pending_revs = conn.execute(
                "SELECT * FROM reversals WHERE state='PENDING'"
                " ORDER BY created_at, reversal_id"
            ).fetchall()
            if not groups and not pending_revs:
                return {"published": False, "reason": "没有待发布内容",
                        "groups": 0, "reversals": 0}

            publish_id = "PUB-" + datetime.now(tz=timezone.utc).strftime(
                "%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
            now = _iso()
            order_n = line_n = pay_n = 0
            for group in groups:
                order = group["order"]
                if group["snapshot_order"]:
                    # The order master is only snapshotted on its first
                    # publication; later runs publish newly linked details.
                    conn.execute(
                        "INSERT INTO order_snapshots(publish_id, order_id, version,"
                        " state, payload) VALUES (?,?,?,?,?)",
                        (publish_id, order["order_id"], order["current_version"],
                         "ACTIVE", store.dumps(self._order_payload(conn, order))))
                    order_n += 1
                    self._lineage(conn, "order", order["order_id"], "PUBLISHED",
                                  f"随发布 {publish_id} 发布, 含明细 "
                                  f"{len(group['lines'])} 行 / {len(group['payments'])} 笔",
                                  None, now)
                else:
                    self._lineage(conn, "order", order["order_id"], "PUBLISHED",
                                  f"增量发布 {publish_id}: 新增明细 "
                                  f"{len(group['lines'])} 行 / {len(group['payments'])} 笔,"
                                  " 订单主记录不重复快照",
                                  None, now)
                for line in group["lines"]:
                    conn.execute(
                        "INSERT INTO line_snapshots(publish_id, line_id, order_id,"
                        " version, state, payload) VALUES (?,?,?,?,?,?)",
                        (publish_id, line["line_id"], order["order_id"],
                         line["version"], "LINKED", store.dumps(dict(line))))
                    line_n += 1
                    self._lineage(conn, "order_line", line["line_id"], "PUBLISHED",
                                  f"随订单 {order['order_id']} 发布于 {publish_id}",
                                  None, now)
                for pay in group["payments"]:
                    conn.execute(
                        "INSERT INTO payment_snapshots(publish_id, payment_id,"
                        " order_id, version, state, payload) VALUES (?,?,?,?,?,?)",
                        (publish_id, pay["payment_id"], order["order_id"],
                         pay["version"], "LINKED", store.dumps(dict(pay))))
                    pay_n += 1
                    self._lineage(conn, "payment", pay["payment_id"], "PUBLISHED",
                                  f"随订单 {order['order_id']} 发布于 {publish_id}",
                                  None, now)

            for rev in pending_revs:
                conn.execute(
                    "UPDATE reversals SET state='PUBLISHED', published_at=?,"
                    " publish_id=? WHERE reversal_id=?",
                    (now, publish_id, rev["reversal_id"]))
                self._lineage(conn, "reversal", rev["reversal_id"],
                              "REVERSAL_PUBLISHED",
                              f"撤销结果随 {publish_id} 发布, 回滚来源 {rev['source_publish_id']}",
                              None, now)

            conn.execute(
                "INSERT INTO publish_journal(publish_id, published_at, order_count,"
                " line_count, payment_count, reversal_count) VALUES (?,?,?,?,?,?)",
                (publish_id, now, order_n, line_n, pay_n, len(pending_revs)))
            return {
                "published": True, "publish_id": publish_id,
                "orders": order_n, "lines": line_n, "payments": pay_n,
                "reversals": len(pending_revs),
            }

    def _collect_publish_groups(self, conn) -> list[dict[str, Any]]:
        """Active orders with at least one never-published linked detail.

        Order and its details are only snapshotted together, so a publication
        can never expose an order without its current detail set (or details
        whose master order is missing).
        """
        groups = []
        orders = conn.execute(
            "SELECT * FROM orders WHERE state='ACTIVE' ORDER BY order_id").fetchall()
        for order in orders:
            unpublished_lines = conn.execute(
                "SELECT * FROM order_lines WHERE order_id=? AND state='LINKED'"
                " AND line_id NOT IN (SELECT line_id FROM line_snapshots)",
                (order["order_id"],)).fetchall()
            unpublished_pays = conn.execute(
                "SELECT * FROM payments WHERE order_id=? AND state='LINKED'"
                " AND payment_id NOT IN (SELECT payment_id FROM payment_snapshots)",
                (order["order_id"],)).fetchall()
            if not unpublished_lines and not unpublished_pays:
                continue
            already_published = conn.execute(
                "SELECT 1 FROM order_snapshots WHERE order_id=?",
                (order["order_id"],)).fetchone()
            groups.append({"order": order, "lines": unpublished_lines,
                           "payments": unpublished_pays,
                           "snapshot_order": already_published is None})
        return groups

    def _order_payload(self, conn, order) -> dict[str, Any]:
        event = conn.execute(
            "SELECT payload FROM order_events WHERE order_id=? AND op='CREATE'"
            " ORDER BY id LIMIT 1", (order["order_id"],)).fetchone()
        payload = _json(event["payload"]) if event else {}
        payload.update({"order_id": order["order_id"],
                        "version": order["current_version"], "state": "ACTIVE"})
        return payload

    # ================================================================ queries
    def overview(self) -> dict[str, Any]:
        with self._read() as conn:
            def count(sql, args=()):
                return conn.execute(sql, args).fetchone()[0]
            return {
                "batches": count("SELECT COUNT(*) FROM batches"),
                "duplicate_batches": count(
                    "SELECT COUNT(*) FROM batches WHERE status='DUPLICATE'"),
                "orders_active": count(
                    "SELECT COUNT(*) FROM orders WHERE state='ACTIVE'"),
                "orders_cancelled": count(
                    "SELECT COUNT(*) FROM orders WHERE state='CANCELLED'"),
                "lines": {
                    s: count("SELECT COUNT(*) FROM order_lines WHERE state=?", (s,))
                    for s in ("WAITING", "LINKED", "EXCEPTION", "CANCELLED")},
                "payments": {
                    s: count("SELECT COUNT(*) FROM payments WHERE state=?", (s,))
                    for s in ("WAITING", "LINKED", "EXCEPTION", "CANCELLED")},
                "waiting_active": count(
                    "SELECT COUNT(*) FROM waiting WHERE state IN ('WAITING','EXCEPTION')"),
                "exceptions": count(
                    "SELECT COUNT(*) FROM waiting WHERE state='EXCEPTION'"),
                "reversals_pending": count(
                    "SELECT COUNT(*) FROM reversals WHERE state='PENDING'"),
                "reversals_published": count(
                    "SELECT COUNT(*) FROM reversals WHERE state='PUBLISHED'"),
                "publish_runs": count("SELECT COUNT(*) FROM publish_journal"),
                "published_orders": count("SELECT COUNT(*) FROM order_snapshots"),
                "published_lines": count("SELECT COUNT(*) FROM line_snapshots"),
                "published_payments": count("SELECT COUNT(*) FROM payment_snapshots"),
            }

    def list_batches(self) -> list[dict[str, Any]]:
        with self._read() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT batch_id, received_at, item_count, status FROM batches"
                " ORDER BY rowid").fetchall()]

    def list_waiting(self, active_only: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM waiting"
        if active_only:
            sql += " WHERE state IN ('WAITING','EXCEPTION')"
        sql += " ORDER BY id"
        with self._read() as conn:
            rows = [dict(r) for r in conn.execute(sql).fetchall()]
        for row in rows:
            row["reason_label"] = LABELS["reason"].get(row["reason_code"],
                                                        row["reason_code"])
            row["state_label"] = LABELS["state"].get(row["state"], row["state"])
        return rows

    def list_orders(self) -> list[dict[str, Any]]:
        with self._read() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM orders ORDER BY order_id").fetchall()]

    def list_lines(self) -> list[dict[str, Any]]:
        with self._read() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM order_lines ORDER BY line_id").fetchall()]

    def list_payments(self) -> list[dict[str, Any]]:
        with self._read() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM payments ORDER BY payment_id").fetchall()]

    def list_reversals(self) -> list[dict[str, Any]]:
        with self._read() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM reversals ORDER BY created_at, reversal_id").fetchall()]

    def list_published(self) -> dict[str, Any]:
        with self._read() as conn:
            return {
                "journal": [dict(r) for r in conn.execute(
                    "SELECT * FROM publish_journal ORDER BY published_at").fetchall()],
                "orders": [dict(r) for r in conn.execute(
                    "SELECT * FROM order_snapshots ORDER BY publish_id").fetchall()],
                "lines": [dict(r) for r in conn.execute(
                    "SELECT * FROM line_snapshots ORDER BY publish_id").fetchall()],
                "payments": [dict(r) for r in conn.execute(
                    "SELECT * FROM payment_snapshots ORDER BY publish_id").fetchall()],
                "reversals": [dict(r) for r in conn.execute(
                    "SELECT * FROM reversals WHERE state='PUBLISHED'"
                    " ORDER BY published_at").fetchall()],
            }

    def lineage(self, entity_type: str, entity_id: str) -> list[dict[str, Any]]:
        with self._read() as conn:
            rows = [dict(r) for r in conn.execute(
                "SELECT * FROM lineage WHERE entity_type=? AND entity_id=?"
                " ORDER BY id", (entity_type, str(entity_id))).fetchall()]
        for row in rows:
            row["event_label"] = LABELS["event"].get(row["event"], row["event"])
        return rows

    def reset(self) -> None:
        """Wipe every table (demo support only; never used against real data)."""
        with self._write() as conn:
            for table in ("lineage", "line_snapshots", "payment_snapshots",
                          "order_snapshots", "publish_journal", "reversals",
                          "waiting", "payments", "order_lines", "order_events",
                          "orders", "biz_versions", "batches"):
                conn.execute(f"DELETE FROM {table}")


class _RejectItem(Exception):
    pass


def _json(raw: str):
    import json
    return json.loads(raw)


def _id_col(entity_type: str) -> str:
    return "line_id" if entity_type == "order_line" else "payment_id"
