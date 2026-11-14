#!/usr/bin/env python3
"""Deterministic end-to-end demo against fresh local databases.

Each synthetic scenario runs in an isolated temporary database so its plan
assertions are exact. Afterwards every batch is replayed once more into a
shared database to demonstrate the complete lifecycle, final state and lineage.
No network calls and no real trading system involved.

Usage:
    python -I scripts/demo.py            # console demo
    python -I scripts/demo.py --serve    # also start the web workbench afterwards
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from intake_workbench.engine import IntakeEngine, _iso  # noqa: E402
from intake_workbench.synthdata import build  # noqa: E402

PASS, FAIL = "✅", "❌"


class CheckFailure(AssertionError):
    pass


def _check(name: str, actual, expected) -> None:
    if actual != expected:
        raise CheckFailure(f"{name}: 期望 {expected!r}, 实际 {actual!r}")
    print(f"    {PASS} {name} = {actual!r}")


class DemoRunner:
    def __init__(self, shared_db: str):
        self.shared_db = shared_db
        Path(shared_db).unlink(missing_ok=True)
        for suffix in ("-wal", "-shm"):
            Path(shared_db + suffix).unlink(missing_ok=True)
        self.batches, self.scenarios = build()
        self.by_id = {b["batch_id"]: b for b in self.batches}

    # -------------------------------------------------------------- per scenario
    def run_isolated(self) -> int:
        failures = 0
        for scenario in self.scenarios:
            print(f"\n=== 场景 {scenario.key}: {scenario.title} ===")
            print(f"    {scenario.description}")
            with tempfile.TemporaryDirectory() as tmp:
                engine = IntakeEngine(str(Path(tmp) / "s.db"),
                                      timeout_seconds=300)
                try:
                    self._run_plan(engine, scenario)
                    print(f"    {PASS} 场景 {scenario.key} 全部断言通过")
                except CheckFailure as exc:
                    failures += 1
                    print(f"    {FAIL} {exc}")
        return failures

    def _run_plan(self, engine: IntakeEngine, scenario) -> None:
        for step_no, step in enumerate(scenario.plan, 1):
            if "send" in step:
                batch = self.by_id[step["send"]]
                age = step.get("age_before_next", 0)
                received_at = _iso(time.time() - age) if age else None
                report = engine.ingest_batch(
                    batch["batch_id"], batch["items"], received_at=received_at)
                print(f"  [{step_no}] 发送 {step['send']}"
                      + (f" (到达时间回溯 {age}s)" if age else ""))
                self._verify_send(engine, step, report)
            elif step.get("sweep"):
                print(f"  [{step_no}] 超时扫描")
                result = engine.sweep_timeouts()
                for key, want in step.get("expect", {}).items():
                    _check(key, result.get(key), want)
            elif step.get("publish"):
                print(f"  [{step_no}] 发布")
                result = engine.publish()
                for key, want in step.get("expect", {}).items():
                    _check(key, result.get(key), want)

    def _verify_send(self, engine, step, report) -> None:
        for key, want in step.get("expect", {}).items():
            if key == "waiting_after":
                active = engine.list_waiting(active_only=True)
                _check(key, len(active), want)
            elif key.startswith("state_"):
                entity = key.split("_", 1)[1]
                if entity.startswith("L-"):
                    table = {r["line_id"]: r for r in engine.list_lines()}
                else:
                    table = {r["order_id"]: r for r in engine.list_orders()}
                _check(key, table[entity]["state"], want)
            elif key == "duplicate_versions_count":
                _check(key, len(report["duplicate_versions"]), want)
            else:
                _check(key, report.get(key), want)

    # ------------------------------------------------------------- full replay
    def run_full_replay(self) -> IntakeEngine:
        """Replay every batch in one shared DB following the scenario plans."""
        engine = IntakeEngine(self.shared_db, timeout_seconds=300)
        print("\n=== 全量回放 (共享库, 展示完整生命周期) ===")
        for scenario in self.scenarios:
            print(f"  -- {scenario.key} {scenario.title}")
            for step in scenario.plan:
                if "send" in step:
                    batch = self.by_id[step["send"]]
                    age = step.get("age_before_next", 0)
                    received_at = _iso(time.time() - age) if age else None
                    engine.ingest_batch(batch["batch_id"], batch["items"],
                                        received_at=received_at)
                elif step.get("sweep"):
                    engine.sweep_timeouts()
                elif step.get("publish"):
                    engine.publish()
        # publish everything remaining, then cancel/publish C's reversal flow
        # is already covered by its plan; a final publish drains leftovers.
        engine.publish()
        return engine

    def print_summary(self, engine: IntakeEngine) -> None:
        print("\n================ 最终状态 ================")
        for key, value in engine.overview().items():
            print(f"  {key}: {value}")
        published = engine.list_published()
        print(f"\n发布流水 ({len(published['journal'])} 次):")
        for j in published["journal"]:
            print(f"  {j['publish_id']}  订单={j['order_count']} "
                  f"行={j['line_count']} 付款={j['payment_count']} "
                  f"撤销={j['reversal_count']}")
        print(f"\n已发布撤销结果 ({len(published['reversals'])} 条, 均带来源发布号):")
        for r in published["reversals"]:
            print(f"  {r['reversal_id']}: {r['source_entity']} {r['source_id']}"
                  f"  <- 来源 {r['source_publish_id']} -> 发布于 {r['publish_id']}")
        print("\n样例血缘 (订单 O-C: 创建 -> 发布 -> 撤销 -> 撤销发布):")
        for ev in engine.lineage("order", "O-C"):
            batch = f" [{ev['batch_id']}]" if ev["batch_id"] else ""
            print(f"  {ev['at']}  {ev['event_label']}{batch}  {ev['detail']}")
        print("\n样例血缘 (订单行 L-B1: 等待 -> 超时异常 -> 恢复 -> 发布):")
        for ev in engine.lineage("order_line", "L-B1"):
            batch = f" [{ev['batch_id']}]" if ev["batch_id"] else ""
            print(f"  {ev['at']}  {ev['event_label']}{batch}  {ev['detail']}")
        print("\n样例血缘 (撤销结果 REV of L-C1):")
        for r in engine.list_reversals():
            if r["source_id"] == "L-C1":
                for ev in engine.lineage("reversal", r["reversal_id"]):
                    print(f"  {ev['at']}  {ev['event_label']}  {ev['detail']}")
                break


def main() -> int:
    parser = argparse.ArgumentParser(description="入仓工作台本地演示")
    parser.add_argument("--db", default="data/demo.db")
    parser.add_argument("--serve", action="store_true",
                        help="演示结束后启动网页工作台 (共享库)")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    runner = DemoRunner(args.db)
    failures = runner.run_isolated()
    engine = runner.run_full_replay()
    runner.print_summary(engine)

    if failures:
        print(f"\n{FAIL} {failures} 个场景断言失败")
        return 1
    print(f"\n{PASS} 全部场景断言通过")
    if args.serve:
        from intake_workbench.server import serve
        serve(db_path=args.db, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
