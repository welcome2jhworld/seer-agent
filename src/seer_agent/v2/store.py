"""SQLite records of the replay core. Every read that could reach past the cutoff goes through the clock."""
from __future__ import annotations

from contextlib import contextmanager
import json
import sqlite3
from pathlib import Path

from ..contracts import ContractError
from .clock import CausalClock
from .records import Trace

SCHEMA = """
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS watches(id TEXT PRIMARY KEY, revision INTEGER NOT NULL, body TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS frames(
        handle TEXT PRIMARY KEY, source TEXT NOT NULL, session TEXT NOT NULL, sequence INTEGER NOT NULL,
        captured_ms INTEGER NOT NULL, published_ms INTEGER NOT NULL, size_bytes INTEGER NOT NULL,
        content TEXT NOT NULL, UNIQUE(source, session, sequence));
    CREATE INDEX IF NOT EXISTS frame_time ON frames(source, captured_ms);
    CREATE TABLE IF NOT EXISTS coverage(
        source TEXT NOT NULL, level TEXT NOT NULL, from_ms INTEGER NOT NULL, to_ms INTEGER NOT NULL,
        recorded_ms INTEGER NOT NULL, ref TEXT NOT NULL, PRIMARY KEY(source, level, ref));
    CREATE TABLE IF NOT EXISTS evidence(
        handle TEXT PRIMARY KEY, source TEXT NOT NULL, session TEXT NOT NULL, sequence INTEGER NOT NULL,
        captured_ms INTEGER NOT NULL, available_from_ms INTEGER NOT NULL, available_until_ms INTEGER NOT NULL,
        size_bytes INTEGER NOT NULL, pinned_by TEXT);
    CREATE TABLE IF NOT EXISTS manifests(
        ref TEXT PRIMARY KEY, handles TEXT NOT NULL, size_bytes INTEGER NOT NULL, created_ms INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS jobs(
        id TEXT PRIMARY KEY, watch_id TEXT NOT NULL, watch_revision INTEGER NOT NULL,
        mission_version INTEGER NOT NULL, context_version INTEGER NOT NULL, owner_epoch INTEGER NOT NULL,
        kind TEXT NOT NULL, priority INTEGER NOT NULL, source TEXT NOT NULL, session TEXT NOT NULL,
        sequence_from INTEGER NOT NULL, sequence_to INTEGER NOT NULL, from_ms INTEGER NOT NULL,
        to_ms INTEGER NOT NULL, manifest_ref TEXT NOT NULL, created_ms INTEGER NOT NULL,
        deadline_ms INTEGER NOT NULL, status TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS job_queue ON jobs(status, priority, deadline_ms, created_ms);
    CREATE TABLE IF NOT EXISTS attempts(
        job_id TEXT NOT NULL, attempt INTEGER NOT NULL, owner_epoch INTEGER NOT NULL, status TEXT NOT NULL,
        started_ms INTEGER NOT NULL, queue_ms INTEGER NOT NULL, available_ms INTEGER, inference_ms INTEGER,
        model_id TEXT, raw_result TEXT, reduced INTEGER NOT NULL DEFAULT 0, reduce_outcome TEXT,
        PRIMARY KEY(job_id, attempt));
    CREATE UNIQUE INDEX IF NOT EXISTS one_valid_result ON attempts(job_id) WHERE status='valid';
    CREATE TABLE IF NOT EXISTS situations(
        id TEXT PRIMARY KEY, merge_key TEXT NOT NULL UNIQUE, source TEXT NOT NULL, region TEXT NOT NULL,
        predicate TEXT NOT NULL, from_ms INTEGER NOT NULL, to_ms INTEGER NOT NULL, coverage_level TEXT NOT NULL,
        mission_version INTEGER NOT NULL, context_version INTEGER NOT NULL, revision INTEGER NOT NULL,
        revision_reason TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS alternatives(
        situation_id TEXT NOT NULL, hypothesis_id TEXT NOT NULL, supporting TEXT NOT NULL,
        contradicting TEXT NOT NULL, score_kind TEXT NOT NULL, score REAL, last_verified_ms INTEGER,
        shared_error_group TEXT NOT NULL, PRIMARY KEY(situation_id, hypothesis_id));
    CREATE TABLE IF NOT EXISTS observations(
        id TEXT PRIMARY KEY, situation_id TEXT NOT NULL, job_id TEXT NOT NULL, attempt INTEGER NOT NULL,
        frames_digest TEXT NOT NULL, from_ms INTEGER NOT NULL, to_ms INTEGER NOT NULL, picked TEXT NOT NULL,
        scores TEXT NOT NULL, score_kind TEXT NOT NULL, observed_ms INTEGER NOT NULL, repeat_of TEXT);
    CREATE TABLE IF NOT EXISTS estimates(
        id TEXT PRIMARY KEY, situation_id TEXT NOT NULL, observation_id TEXT NOT NULL,
        hypothesis_id TEXT NOT NULL, from_ms INTEGER NOT NULL, to_ms INTEGER NOT NULL,
        committed_ms INTEGER NOT NULL, current INTEGER NOT NULL, placement TEXT NOT NULL, reason TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS checks(
        id TEXT PRIMARY KEY, situation_id TEXT NOT NULL, kind TEXT NOT NULL, separates TEXT NOT NULL,
        from_ms INTEGER NOT NULL, to_ms INTEGER NOT NULL, created_ms INTEGER NOT NULL, status TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS questions(
        id TEXT PRIMARY KEY, situation_id TEXT, kind TEXT NOT NULL, source TEXT NOT NULL,
        from_ms INTEGER NOT NULL, to_ms INTEGER NOT NULL, detail TEXT NOT NULL, created_ms INTEGER NOT NULL,
        status TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS traces(
        job_id TEXT NOT NULL, attempt INTEGER NOT NULL, source TEXT NOT NULL, session TEXT NOT NULL,
        sequence_from INTEGER NOT NULL, sequence_to INTEGER NOT NULL, mission_version INTEGER NOT NULL,
        watch_revision INTEGER NOT NULL, context_version INTEGER NOT NULL, owner_epoch INTEGER NOT NULL,
        model_id TEXT NOT NULL, queue_ms INTEGER NOT NULL, inference_ms INTEGER NOT NULL,
        manifest_ref TEXT NOT NULL, outcome TEXT NOT NULL, estimate_id TEXT, reduced_ms INTEGER NOT NULL,
        PRIMARY KEY(job_id, attempt));
    CREATE TABLE IF NOT EXISTS search_index(
        entry INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, predicate TEXT NOT NULL,
        hypothesis_id TEXT NOT NULL, from_ms INTEGER NOT NULL, to_ms INTEGER NOT NULL,
        observation_id TEXT NOT NULL, indexed_ms INTEGER NOT NULL);
"""
LEVEL_RANK = {"inspected": 3, "signal": 2, "captured": 1, "unknown": 0}


class ReplayStore:
    """Durable records plus guarded readers. Writers live in ReplayCore."""

    def __init__(self, path: str | Path, clock: CausalClock):
        self.clock = clock
        self.db = sqlite3.connect(path, isolation_level=None, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        if self.db.execute("PRAGMA user_version").fetchone()[0] not in {0, 1}:
            raise ContractError("unsupported_store_version")
        self.db.executescript(SCHEMA)
        self.db.execute("INSERT OR IGNORE INTO meta VALUES('context_version','0')")
        self.db.execute("PRAGMA user_version=1")

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def close(self) -> None:
        self.db.close()

    def row(self, sql: str, args: tuple = ()) -> sqlite3.Row | None:
        return self.db.execute(sql, args).fetchone()

    def rows(self, sql: str, args: tuple = ()) -> list[sqlite3.Row]:
        return self.db.execute(sql, args).fetchall()

    def meta(self, key: str, default: str | None = None) -> str | None:
        found = self.row("SELECT value FROM meta WHERE key=?", (key,))
        return default if found is None else found["value"]

    def set_meta(self, key: str, value: str) -> None:
        self.db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, value))

    # Guarded reads: each names the time its data became readable and checks it against the cutoff.

    def frames(self, source: str, from_ms: int, to_ms: int) -> list[sqlite3.Row]:
        self.clock.check(to_ms, f"frames:{source}")
        return self.rows("""SELECT * FROM frames WHERE source=? AND captured_ms BETWEEN ? AND ? AND published_ms<=?
                            ORDER BY captured_ms, sequence""", (source, from_ms, to_ms, self.clock.now_ms))

    def evidence(self, handle: str) -> sqlite3.Row:
        found = self.row("SELECT * FROM evidence WHERE handle=?", (handle,))
        if found is None:
            raise ContractError("unknown_evidence")
        self.clock.check(found["available_from_ms"], f"evidence:{handle}")
        return found

    def result(self, job_id: str, attempt: int) -> dict | None:
        """A stored raw result, readable only once inference has finished on the replay timeline."""
        found = self.row("SELECT available_ms, raw_result FROM attempts WHERE job_id=? AND attempt=?", (job_id, attempt))
        if found is None or found["raw_result"] is None:
            return None
        self.clock.check(found["available_ms"], f"result:{job_id}:{attempt}")
        return json.loads(found["raw_result"])

    def coverage(self, source: str, from_ms: int, to_ms: int) -> list[tuple[int, int, str]]:
        """Segments of [from_ms, to_ms] at their highest recorded level; uncovered parts are `unknown`."""
        self.clock.check(to_ms, f"coverage:{source}")
        covered = self.rows("""SELECT level, from_ms, to_ms FROM coverage WHERE source=? AND recorded_ms<=?
                               AND to_ms>? AND from_ms<?""", (source, self.clock.now_ms, from_ms, to_ms))
        points = sorted({from_ms, to_ms, *(max(from_ms, r["from_ms"]) for r in covered),
                         *(min(to_ms, r["to_ms"]) for r in covered)})
        segments: list[tuple[int, int, str]] = []
        for start, end in zip(points, points[1:]):
            level = max((r["level"] for r in covered if r["from_ms"] <= start and r["to_ms"] >= end),
                        key=LEVEL_RANK.get, default="unknown")
            if segments and segments[-1][2] == level:
                segments[-1] = (segments[-1][0], end, level)
            else:
                segments.append((start, end, level))
        return segments

    def search(self, source: str, predicate: str, until_ms: int) -> list[sqlite3.Row]:
        """The incremental observation index: entries exist only once the reducer committed them."""
        self.clock.check(until_ms, f"index:{source}:{predicate}")
        return self.rows("""SELECT * FROM search_index WHERE source=? AND predicate=? AND to_ms<=? AND indexed_ms<=?
                            ORDER BY entry""", (source, predicate, until_ms, self.clock.now_ms))

    # Read models over committed state.

    def current_estimate(self, merge_key: str) -> sqlite3.Row | None:
        return self.row("""SELECT e.* FROM estimates e JOIN situations s ON s.id=e.situation_id
                           WHERE s.merge_key=? AND e.current=1""", (merge_key,))

    def history(self, merge_key: str) -> list[sqlite3.Row]:
        return self.rows("""SELECT e.* FROM estimates e JOIN situations s ON s.id=e.situation_id
                            WHERE s.merge_key=? AND e.current=0 ORDER BY e.committed_ms, e.id""", (merge_key,))

    def observations(self, merge_key: str) -> list[sqlite3.Row]:
        return self.rows("""SELECT o.* FROM observations o JOIN situations s ON s.id=o.situation_id
                            WHERE s.merge_key=? ORDER BY o.observed_ms, o.id""", (merge_key,))

    def alternatives(self, merge_key: str) -> dict[str, dict]:
        found = self.rows("""SELECT a.* FROM alternatives a JOIN situations s ON s.id=a.situation_id
                             WHERE s.merge_key=? ORDER BY a.hypothesis_id""", (merge_key,))
        return {r["hypothesis_id"]: {**dict(r), "supporting": json.loads(r["supporting"]),
                                     "contradicting": json.loads(r["contradicting"])} for r in found}

    def questions(self, kind: str | None = None) -> list[dict]:
        found = self.rows("SELECT * FROM questions WHERE (?1 IS NULL OR kind=?1) ORDER BY created_ms, id", (kind,))
        return [{**dict(r), "detail": json.loads(r["detail"])} for r in found]

    def checks(self) -> list[dict]:
        found = self.rows("SELECT * FROM checks ORDER BY created_ms, id")
        return [{**dict(r), "separates": json.loads(r["separates"])} for r in found]

    def traces(self) -> list[Trace]:
        return [Trace(*row) for row in self.rows(
            "SELECT * FROM traces WHERE reduced_ms<=? ORDER BY reduced_ms, job_id, attempt", (self.clock.now_ms,))]
