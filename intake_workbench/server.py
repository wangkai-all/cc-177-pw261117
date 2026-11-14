"""Standard-library HTTP server exposing the engine and the workbench UI."""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .engine import LABELS, IntakeEngine, _iso
from .synthdata import build as build_synth

WEB_DIR = Path(__file__).resolve().parent.parent / "web"


class Handler(BaseHTTPRequestHandler):
    engine: IntakeEngine = None  # set on the server instance

    def log_message(self, fmt, *args):  # quieter logs
        pass

    # ------------------------------------------------------------- transport
    def _send_json(self, obj, status: int = 200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _static(self, name: str, content_type: str):
        path = WEB_DIR / name
        if not path.exists():
            self.send_error(404)
            return
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ----------------------------------------------------------------- routing
    def do_GET(self):
        parsed = urlparse(self.path)
        path, query = parsed.path, parse_qs(parsed.query)
        engine = self.server.engine
        try:
            if path in ("/", "/index.html"):
                return self._static("index.html", "text/html; charset=utf-8")
            if path == "/app.js":
                return self._static("app.js", "application/javascript; charset=utf-8")
            if path == "/styles.css":
                return self._static("styles.css", "text/css; charset=utf-8")
            if path == "/api/labels":
                return self._send_json(LABELS)
            if path == "/api/overview":
                return self._send_json(engine.overview())
            if path == "/api/batches":
                return self._send_json(engine.list_batches())
            if path == "/api/waiting":
                return self._send_json(
                    engine.list_waiting(active_only=query.get("active", ["0"])[0] == "1"))
            if path == "/api/orders":
                return self._send_json(engine.list_orders())
            if path == "/api/lines":
                return self._send_json(engine.list_lines())
            if path == "/api/payments":
                return self._send_json(engine.list_payments())
            if path == "/api/reversals":
                return self._send_json(engine.list_reversals())
            if path == "/api/published":
                return self._send_json(engine.list_published())
            if path == "/api/synth":
                batches, scenarios = build_synth()
                return self._send_json({
                    "batches": batches,
                    "scenarios": [{"key": s.key, "title": s.title,
                                   "description": s.description,
                                   "batch_ids": [b["batch_id"] for b in s.batches]}
                                  for s in scenarios],
                })
            if path == "/api/lineage":
                etype = query.get("entity_type", [""])[0]
                eid = query.get("entity_id", [""])[0]
                if not etype or not eid:
                    return self._send_json({"error": "需要 entity_type 和 entity_id"}, 400)
                return self._send_json(engine.lineage(etype, eid))
            self.send_error(404)
        except Exception as exc:  # surface engine errors to the UI
            self._send_json({"error": str(exc)}, 500)

    def do_POST(self):
        path = urlparse(self.path).path
        engine = self.server.engine
        try:
            if path == "/api/ingest":
                payload = self._read_json()
                batch_id = payload.get("batch_id")
                items = payload.get("items", [])
                if not batch_id or not isinstance(items, list):
                    return self._send_json(
                        {"error": "需要 batch_id 与 items 数组"}, 400)
                return self._send_json(engine.ingest_batch(batch_id, items))
            if path == "/api/sweep":
                payload = self._read_json()
                fast_forward = int(payload.get("fast_forward_seconds", 0))
                effective = (_iso(time.time() + fast_forward)
                             if fast_forward else None)
                return self._send_json(engine.sweep_timeouts(effective))
            if path == "/api/publish":
                return self._send_json(engine.publish())
            if path == "/api/synth/send-shuffled":
                return self._send_json(self._send_shuffled(engine))
            if path == "/api/synth/master":
                return self._send_json(self._send_master(engine, self._read_json()))
            if path == "/api/reset":
                engine.reset()
                return self._send_json({"ok": True})
            self.send_error(404)
        except json.JSONDecodeError:
            self._send_json({"error": "请求体不是合法 JSON"}, 400)
        except Exception as exc:
            self._send_json({"error": str(exc)}, 500)

    # ------------------------------------------------------------ synth helpers
    def _send_shuffled(self, engine: IntakeEngine) -> dict:
        """Send all synthetic batches in a deterministic-ish shuffled order."""
        batches, scenarios = build_synth()
        order = list(batches)
        # Hand-picked shuffle: details and cancels tend to land before masters.
        preference = {"B-A1": 0, "B-B1": 1, "B-E1": 2, "B-C1": 3, "B-D1": 4,
                      "B-F1": 5, "B-G1": 6, "B-A2": 7, "B-B2": 8, "B-D2": 9,
                      "B-E2": 10, "B-F2": 11, "B-E3": 12, "B-C2": 13,
                      "B-F3": 14}
        order.sort(key=lambda b: preference.get(b["batch_id"], 99))
        reports = []
        for batch in order:
            reports.append(engine.ingest_batch(batch["batch_id"], batch["items"]))
        return {"sent_order": [b["batch_id"] for b in order], "reports": reports}

    def _send_master(self, engine: IntakeEngine, payload: dict) -> dict:
        """Re-send (or synthesize) a missing order master for a waiting detail."""
        order_id = payload.get("order_id")
        if not order_id:
            raise ValueError("需要 order_id")
        batch_id = payload.get("batch_id") or f"B-MANUAL-{order_id}-{int(time.time())}"
        item = payload.get("item") or {
            "entity_type": "order", "order_id": order_id, "op": "CREATE",
            "version": int(payload.get("version", 1)),
            "customer": payload.get("customer", "手动补送"),
        }
        return engine.ingest_batch(batch_id, [item])


def serve(db_path: str = "data/intake.db", host: str = "127.0.0.1",
          port: int = 8000, timeout_seconds: int = 300) -> None:
    engine = IntakeEngine(db_path, timeout_seconds=timeout_seconds)
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.engine = engine
    httpd.daemon_threads = True
    print(f"入仓工作台: http://{host}:{port}  (db={db_path},"
          f" 等待超时={timeout_seconds}s)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n停止服务")
