"""Local synthetic data — never talks to a real trading system.

Each scenario is a small set of batches.  ``build()`` returns every batch in a
flat list (the web workbench can send them in shuffled order) plus a structured
plan that ``scripts/demo.py`` executes deterministically.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Scenario:
    key: str
    title: str
    description: str
    batches: list[dict[str, Any]] = field(default_factory=list)
    plan: list[dict[str, Any]] = field(default_factory=list)


def _line(line_id, order_id, sku, qty, amount, version=1):
    return {"entity_type": "order_line", "line_id": line_id, "order_id": order_id,
            "sku": sku, "qty": qty, "amount": amount, "version": version}


def _pay(payment_id, order_id, method, amount, version=1):
    return {"entity_type": "payment", "payment_id": payment_id,
            "order_id": order_id, "method": method, "amount": amount,
            "version": version}


def _order(order_id, version=1, **extra):
    return {"entity_type": "order", "order_id": order_id, "op": "CREATE",
            "version": version, **extra}


def _cancel(order_id, version=2, reason="客户撤单"):
    return {"entity_type": "order", "order_id": order_id, "op": "CANCEL",
            "version": version, "reason": reason}


def build() -> tuple[list[dict[str, Any]], list[Scenario]]:
    scenarios: list[Scenario] = []

    # A. 乱序到达: 明细先到, 主记录补达后自动补关联 -------------------------
    a = Scenario("A", "乱序补关联",
                 "订单行与付款先到进入等待区, 订单主记录补达后自动关联")
    a.batches = [
        {"batch_id": "B-A1", "items": [
            _line("L-A1", "O-A", "SKU-1", 2, 199.0),
            _pay("P-A1", "O-A", "CARD", 199.0)]},
        {"batch_id": "B-A2", "items": [
            _order("O-A", customer="C001")]},
    ]
    a.plan = [
        {"send": "B-A1", "expect": {"waiting_after": 2}},
        {"send": "B-A2", "expect": {"linked_from_waiting": 2}},
    ]
    scenarios.append(a)

    # B. 等待超时转异常, 主记录补达后恢复 -----------------------------------
    b = Scenario("B", "超时异常与恢复",
                 "等待超时只进异常处理不删数据, 订单补达后异常恢复并补关联")
    b.batches = [
        {"batch_id": "B-B1", "items": [_line("L-B1", "O-B", "SKU-2", 1, 88.0)]},
        {"batch_id": "B-B2", "items": [_order("O-B", customer="C002")]},
    ]
    b.plan = [
        {"send": "B-B1", "age_before_next": 400},
        {"sweep": True, "expect": {"timed_out": 1}},
        {"send": "B-B2", "expect": {"state_L-B1": "LINKED"}},
    ]
    scenarios.append(b)

    # C. 发布后撤销, 对已发布明细生成有来源的撤销结果 ------------------------
    c = Scenario("C", "发布后撤销",
                 "订单与明细同批到达并发布; 撤销后为每条已发布明细生成带来源发布号的撤销结果")
    c.batches = [
        {"batch_id": "B-C1", "items": [
            _order("O-C", customer="C003"),
            _line("L-C1", "O-C", "SKU-3", 1, 50.0),
            _line("L-C2", "O-C", "SKU-4", 3, 20.0),
            _pay("P-C1", "O-C", "ALIPAY", 110.0)]},
        {"batch_id": "B-C2", "items": [_cancel("O-C", version=2)]},
    ]
    c.plan = [
        {"send": "B-C1"},
        {"publish": True, "expect": {"orders": 1, "lines": 2, "payments": 1}},
        {"send": "B-C2", "expect": {"reversals_created": 3,
                                    "cancelled_orders": ["O-C"]}},
        {"publish": True, "expect": {"reversals": 3, "orders": 0}},
    ]
    scenarios.append(c)

    # D. 批次重复投递 + 同一业务版本重投 ------------------------------------
    d = Scenario("D", "重复投递幂等",
                 "同批次号重复投递整批忽略; 不同批次携带同一业务版本也不重复计入")
    d.batches = [
        {"batch_id": "B-D1", "items": [_order("O-D", customer="C004")]},
        {"batch_id": "B-D2", "items": [_order("O-D", customer="C004")]},
    ]
    d.plan = [
        {"send": "B-D1"},
        {"send": "B-D1", "expect": {"status": "DUPLICATE"}},
        {"send": "B-D2", "expect": {"duplicate_versions_count": 1}},
    ]
    scenarios.append(d)

    # E. 先撤销后创建 (含撤销标记超时) --------------------------------------
    e = Scenario("E", "先撤销后创建",
                 "撤销指令先到成为等待标记, 即使超时转异常, 订单创建时仍被承认;"
                 " 撤销后到达的明细直接作废")
    e.batches = [
        {"batch_id": "B-E1", "items": [_cancel("O-E", version=1, reason="风控拦截")]},
        {"batch_id": "B-E2", "items": [_order("O-E", version=2, customer="C005")]},
        {"batch_id": "B-E3", "items": [_line("L-E1", "O-E", "SKU-5", 1, 9.9)]},
    ]
    e.plan = [
        {"send": "B-E1", "age_before_next": 400},
        {"sweep": True, "expect": {"timed_out": 1}},
        {"send": "B-E2", "expect": {"state_O-E": "CANCELLED",
                                    "markers_consumed": 1}},
        {"send": "B-E3", "expect": {"state_L-E1": "CANCELLED"}},
    ]
    scenarios.append(e)

    # F. 增量到达, 多次发布, 同一业务版本不重复计入 -------------------------
    f = Scenario("F", "增量发布不重复",
                 "订单先发布, 后续明细分批到达分批发布; 每条业务版本只发布一次")
    f.batches = [
        {"batch_id": "B-F1", "items": [_order("O-F", customer="C006")]},
        {"batch_id": "B-F2", "items": [_line("L-F1", "O-F", "SKU-6", 1, 12.0)]},
        {"batch_id": "B-F3", "items": [
            _line("L-F2", "O-F", "SKU-7", 2, 30.0),
            _pay("P-F1", "O-F", "CARD", 72.0)]},
    ]
    f.plan = [
        {"send": "B-F1"},
        {"send": "B-F2"},
        {"publish": True, "expect": {"orders": 1, "lines": 1, "payments": 0}},
        {"send": "B-F3"},
        {"publish": True, "expect": {"orders": 0, "lines": 1, "payments": 1}},
        {"publish": True, "expect": {"published": False}},
    ]
    scenarios.append(f)

    # G. 同批次混合实体 (顺序正确时直接关联) --------------------------------
    g = Scenario("G", "同批混合实体",
                 "一个批次内同时含订单与明细, 按条目顺序处理即可直接关联")
    g.batches = [
        {"batch_id": "B-G1", "items": [
            _order("O-G", customer="C007"),
            _line("L-G1", "O-G", "SKU-8", 5, 8.0),
            _pay("P-G1", "O-G", "CASH", 40.0)]},
    ]
    g.plan = [{"send": "B-G1", "expect": {"applied": 3,
                                          "linked_from_waiting": 0}}]
    scenarios.append(g)

    all_batches = [b for s in scenarios for b in s.batches]
    return all_batches, scenarios


if __name__ == "__main__":
    import json
    import pathlib

    batches, scenarios = build()
    out = pathlib.Path("data/synth_batches.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(
        {"batches": batches,
         "scenarios": [{"key": s.key, "title": s.title,
                        "description": s.description,
                        "batch_ids": [b["batch_id"] for b in s.batches]}
                       for s in scenarios]},
        ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {len(batches)} batches across {len(scenarios)} scenarios -> {out}")
