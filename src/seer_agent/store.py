"""Single-machine durable state, bounded memory and transactional action outbox."""
from __future__ import annotations

from contextlib import contextmanager
import json
import sqlite3
from pathlib import Path

from .contracts import ContractError, Event, MemoryRecord, MemoryView, encoded


class Store:
    def __init__(self, path: str | Path):
        self.db = sqlite3.connect(path, isolation_level=None, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version not in {0, 1, 2, 3, 4}:
            raise ContractError("unsupported_store_version")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS missions (
                id TEXT PRIMARY KEY, definition TEXT NOT NULL, state TEXT NOT NULL,
                clock INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS events (
                revision INTEGER PRIMARY KEY AUTOINCREMENT, mission TEXT NOT NULL,
                id TEXT NOT NULL, source TEXT NOT NULL, kind TEXT NOT NULL,
                observed INTEGER NOT NULL, available INTEGER NOT NULL,
                body TEXT NOT NULL, admitted INTEGER NOT NULL, depth INTEGER NOT NULL DEFAULT 0, UNIQUE(mission,id));
            CREATE INDEX IF NOT EXISTS event_scope ON events(mission,available,revision);
            CREATE TABLE IF NOT EXISTS decisions (
                revision INTEGER PRIMARY KEY AUTOINCREMENT, mission TEXT NOT NULL,
                event_id TEXT NOT NULL, transition TEXT, summary TEXT NOT NULL,
                evidence TEXT NOT NULL, state TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS operations (
                id TEXT PRIMARY KEY, mission TEXT NOT NULL, event_id TEXT NOT NULL, event_revision INTEGER NOT NULL,
                role TEXT NOT NULL, tool TEXT NOT NULL, fingerprint TEXT NOT NULL,
                arguments TEXT NOT NULL, memory_ids TEXT NOT NULL, emits TEXT,
                status TEXT NOT NULL, created INTEGER NOT NULL, deadline INTEGER NOT NULL,
                completed INTEGER, result TEXT, receipt TEXT, applied INTEGER NOT NULL DEFAULT 0,
                outcome_revision INTEGER NOT NULL DEFAULT 1);
            CREATE INDEX IF NOT EXISTS operation_queue ON operations(mission,status,role);
            CREATE TABLE IF NOT EXISTS operation_history (
                revision INTEGER PRIMARY KEY AUTOINCREMENT, operation TEXT NOT NULL,
                status TEXT NOT NULL, at INTEGER NOT NULL, result TEXT NOT NULL);
        """)
        if "event_revision" not in {r[1] for r in self.db.execute("PRAGMA table_info(operations)")}:
            self.db.execute("ALTER TABLE operations ADD COLUMN event_revision INTEGER NOT NULL DEFAULT 0")
            self.db.execute("UPDATE operations SET event_revision=(SELECT revision FROM events WHERE events.mission=operations.mission AND events.id=operations.event_id)")
        if "depth" not in {r[1] for r in self.db.execute("PRAGMA table_info(events)")}:
            self.db.execute("ALTER TABLE events ADD COLUMN depth INTEGER NOT NULL DEFAULT 0")
        if "outcome_revision" not in {r[1] for r in self.db.execute("PRAGMA table_info(operations)")}:
            self.db.execute("ALTER TABLE operations ADD COLUMN outcome_revision INTEGER NOT NULL DEFAULT 1")
        self.db.execute("PRAGMA user_version=4")

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def close(self):
        self.db.close()

    def open_mission(self, key: str, definition: dict):
        body = encoded(definition)
        with self.transaction():
            prior = self.db.execute("SELECT definition FROM missions WHERE id=?", (key,)).fetchone()
            if prior and prior[0] != body:
                raise ContractError("mission_or_plugin_changed_without_new_version")
            self.db.execute("INSERT OR IGNORE INTO missions(id,definition,state) VALUES(?,?,?)", (key, body, "{}"))

    def state(self, key: str) -> dict:
        return json.loads(self.db.execute("SELECT state FROM missions WHERE id=?", (key,)).fetchone()[0])

    def clock(self, key: str) -> int:
        return self.db.execute("SELECT clock FROM missions WHERE id=?", (key,)).fetchone()[0]

    def event(self, key: str, event_id: str):
        return self.db.execute("SELECT * FROM events WHERE mission=? AND id=?", (key, event_id)).fetchone()

    def memory(self, key: str, sources: list[str], *, now: int, max_age: int,
               limit: int, revision: int | None = None) -> MemoryView:
        if not sources:
            return MemoryView([])
        rows = []
        per_source = max(1, limit//len(sources))
        for source in sources:
            rows.extend(self.db.execute("""SELECT * FROM events WHERE mission=? AND admitted=1
                AND source=? AND available<=? AND observed<=? AND observed>=?
                AND revision<=? ORDER BY revision DESC LIMIT ?""",
                (key, source, now, now, max(0, now-max_age), revision or 9223372036854775807, per_source)).fetchall())
        rows.sort(key=lambda r: r["revision"])
        return MemoryView([MemoryRecord(Event.from_dict(json.loads(r["body"])), r["revision"], r["available"])
                           for r in rows])

    def pinned_memory(self, key: str, ids: list[str]) -> MemoryView:
        records = []
        for event_id in ids:
            row = self.event(key, event_id)
            if row is None:
                raise ContractError("missing_pinned_memory")
            records.append(MemoryRecord(Event.from_dict(json.loads(row["body"])), row["revision"], row["available"]))
        return MemoryView(records)

    def operations(self, key: str) -> list[dict]:
        return [dict(row) for row in self.db.execute("SELECT * FROM operations WHERE mission=? ORDER BY event_revision,id", (key,))]

    def history(self, key: str) -> list[dict]:
        return [dict(row) for row in self.db.execute("SELECT * FROM decisions WHERE mission=? ORDER BY revision", (key,))]

    def claim(self, operation: str, now: int, max_running: int = 1):
        with self.transaction():
            row = self.db.execute("SELECT * FROM operations WHERE id=?", (operation,)).fetchone()
            if row is None or row["status"] != "pending" or row["deadline"] <= now:
                return None
            running = self.db.execute("SELECT COUNT(*) FROM operations WHERE mission=? AND status='running'", (row["mission"],)).fetchone()[0]
            if running >= max_running:
                return None
            self.db.execute("UPDATE operations SET status='running' WHERE id=? AND status='pending'", (operation,))
            self.db.execute("INSERT INTO operation_history VALUES(NULL,?,?,?,?)", (operation, "running", now, "{}"))
            return dict(row)

    def finish(self, operation: str, status: str, data: dict, receipt: str | None, now: int):
        with self.transaction():
            row = self.db.execute("SELECT status,completed FROM operations WHERE id=?", (operation,)).fetchone()
            late = row is not None and row[0] == "unknown" and status in {"succeeded", "accepted", "failed"} and now >= (row[1] or 0)
            if row is None or (row[0] != "running" and not late):
                return False
            self.db.execute("UPDATE operations SET status=?,result=?,receipt=?,completed=?,applied=0,outcome_revision=outcome_revision+? WHERE id=?",
                            (status, encoded(data), receipt, now, int(late), operation))
            self.db.execute("INSERT INTO operation_history VALUES(NULL,?,?,?,?)",
                            (operation, status, now, encoded({"data": data, "receipt": receipt})))
            return True

    def expire(self, key: str, now: int):
        with self.transaction():
            rows = self.db.execute("SELECT id,status FROM operations WHERE mission=? AND status IN ('pending','running') AND deadline<=?",
                                   (key, now)).fetchall()
            for row in rows:
                status = "unknown" if row["status"] == "running" else "failed"
                data = encoded({"reason": "deadline_expired"})
                self.db.execute("UPDATE operations SET status=?,result=?,completed=? WHERE id=?", (status, data, now, row["id"]))
                self.db.execute("INSERT INTO operation_history VALUES(NULL,?,?,?,?)", (row["id"], status, now, data))

    def resolve(self, key: str, operation: str, status: str, receipt: str, now: int):
        """A trusted host supplies independently verified reconciliation, never a resend."""
        if status not in {"succeeded", "failed"} or not receipt or len(receipt) > 512:
            raise ContractError("invalid_resolution")
        with self.transaction():
            row = self.db.execute("SELECT status,role FROM operations WHERE mission=? AND id=?", (key, operation)).fetchone()
            if row is None or row[0] not in {"unknown", "accepted"} or row[1] != "act":
                raise ContractError("operation_not_resolvable")
            result = encoded({"verified_receipt": receipt, "previous_status": row[0]})
            self.db.execute("UPDATE operations SET status=?,receipt=?,completed=?,result=?,applied=0,outcome_revision=outcome_revision+1 WHERE id=?", (status, receipt, now, result, operation))
            self.db.execute("INSERT INTO operation_history VALUES(NULL,?,?,?,?)", (operation, status, now, result))
