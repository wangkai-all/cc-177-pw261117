"""SQLite persistence layer.

The schema deliberately keeps every fact that arrives:

* ``batches``        - raw batch envelope (the original payload is preserved)
* ``biz_versions``   - one row per (entity_type, entity_id, version): this is
                       the idempotency boundary. The same business version can
                       be redelivered in several batches but is applied once.
* ``orders`` / ``order_events`` - current order row plus every create/cancel
* ``order_lines`` / ``payments`` - detail rows, linkable to an order or parked
* ``waiting``        - waiting area (details waiting for a master, or cancel
                       markers arriving before the order they target)
* ``reversals``      - sourced reversal results for cancelled published detail
* ``publish_journal`` / ``*_snapshots`` - atomic publication runs and results
* ``lineage``        - append-only event trail; every state change records the
                       batch that caused it, so each final record can explain
                       which batches it passed through
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id      TEXT PRIMARY KEY,
    received_at   TEXT NOT NULL,
    item_count    INTEGER NOT NULL,
    status        TEXT NOT NULL,            -- NEW | DUPLICATE
    raw_payload   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS biz_versions (
    entity_type   TEXT NOT NULL,            -- order | order_line | payment
    entity_id     TEXT NOT NULL,
    version       INTEGER NOT NULL,
    batch_id      TEXT NOT NULL,
    applied_at    TEXT NOT NULL,
    PRIMARY KEY (entity_type, entity_id, version)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    current_version INTEGER NOT NULL,
    state           TEXT NOT NULL,          -- ACTIVE | CANCELLED
    created_at      TEXT NOT NULL,
    cancelled_at    TEXT,
    cancel_batch_id TEXT,
    create_batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS order_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id    TEXT NOT NULL,
    version     INTEGER NOT NULL,
    op          TEXT NOT NULL,              -- CREATE | CANCEL
    batch_id    TEXT NOT NULL,
    event_at    TEXT NOT NULL,
    payload     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS order_lines (
    line_id       TEXT PRIMARY KEY,
    order_id      TEXT,                     -- NULL while waiting
    sku           TEXT NOT NULL,
    qty           INTEGER NOT NULL,
    amount        REAL NOT NULL,
    version       INTEGER NOT NULL,
    state         TEXT NOT NULL,            -- WAITING | LINKED | EXCEPTION | CANCELLED
    arrived_at    TEXT NOT NULL,
    linked_at     TEXT,
    batch_id      TEXT NOT NULL,
    UNIQUE (line_id, version)
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id    TEXT PRIMARY KEY,
    order_id      TEXT,                     -- NULL while waiting
    method        TEXT NOT NULL,
    amount        REAL NOT NULL,
    version       INTEGER NOT NULL,
    state         TEXT NOT NULL,            -- WAITING | LINKED | EXCEPTION | CANCELLED
    arrived_at    TEXT NOT NULL,
    linked_at     TEXT,
    batch_id      TEXT NOT NULL,
    UNIQUE (payment_id, version)
);

CREATE TABLE IF NOT EXISTS waiting (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    wait_kind     TEXT NOT NULL,            -- DETAIL | CANCEL_MARKER
    entity_type   TEXT,                     -- order_line | payment (details)
    entity_id     TEXT NOT NULL,
    target_order  TEXT NOT NULL,
    version       INTEGER,
    batch_id      TEXT NOT NULL,
    arrived_at    TEXT NOT NULL,
    reason_code   TEXT NOT NULL,
    state         TEXT NOT NULL,            -- WAITING | EXCEPTION | RESOLVED | CANCELLED
    resolved_at   TEXT,
    resolved_by   TEXT,                     -- batch_id or 'SWEEP_TIMEOUT'
    payload       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_waiting_order ON waiting(target_order, state);

CREATE TABLE IF NOT EXISTS reversals (
    reversal_id       TEXT PRIMARY KEY,
    order_id          TEXT NOT NULL,
    source_entity     TEXT NOT NULL,        -- order_line | payment
    source_id         TEXT NOT NULL,
    source_publish_id TEXT NOT NULL,        -- publication being reversed
    reason            TEXT NOT NULL,
    state             TEXT NOT NULL,        -- PENDING | PUBLISHED
    batch_id          TEXT NOT NULL,        -- cancel batch that sourced it
    created_at        TEXT NOT NULL,
    published_at      TEXT,
    publish_id        TEXT
);

CREATE TABLE IF NOT EXISTS publish_journal (
    publish_id    TEXT PRIMARY KEY,
    published_at  TEXT NOT NULL,
    order_count   INTEGER NOT NULL,
    line_count    INTEGER NOT NULL,
    payment_count INTEGER NOT NULL,
    reversal_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS order_snapshots (
    publish_id  TEXT NOT NULL,
    order_id    TEXT NOT NULL,
    version     INTEGER NOT NULL,
    state       TEXT NOT NULL,
    payload     TEXT NOT NULL,
    PRIMARY KEY (publish_id, order_id)
);

CREATE TABLE IF NOT EXISTS line_snapshots (
    publish_id  TEXT NOT NULL,
    line_id     TEXT NOT NULL,
    order_id    TEXT NOT NULL,
    version     INTEGER NOT NULL,
    state       TEXT NOT NULL,
    payload     TEXT NOT NULL,
    PRIMARY KEY (publish_id, line_id)
);

CREATE TABLE IF NOT EXISTS payment_snapshots (
    publish_id  TEXT NOT NULL,
    payment_id  TEXT NOT NULL,
    order_id    TEXT NOT NULL,
    version     INTEGER NOT NULL,
    state       TEXT NOT NULL,
    payload     TEXT NOT NULL,
    PRIMARY KEY (publish_id, payment_id)
);

CREATE TABLE IF NOT EXISTS lineage (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,              -- batch | order | order_line | payment | waiting | reversal | publish
    entity_id   TEXT NOT NULL,
    event       TEXT NOT NULL,
    detail      TEXT NOT NULL,
    batch_id    TEXT,
    at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_lineage_entity ON lineage(entity_type, entity_id);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


@contextmanager
def transaction(conn: sqlite3.Connection):
    """Single-writer transactions make publication/interruption semantics safe."""
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)
