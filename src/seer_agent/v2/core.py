"""ReplayCore owns the state. Sources and workers return data; only this object validates and commits."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from typing import Any

from ..contracts import ContractError, digest, encoded, validate
from .records import (INSUFFICIENT, PRIORITIES, RESULT_SCHEMA, UNKNOWN, Frame, InferenceRequest, Trace, Watch,
                      pick, ranked)
from .source import BoundedSource, EvidenceLost
from .store import ReplayStore
from .worker import Worker

MAX_ATTEMPTS = 2
SHARED_ERROR_GROUP = "one_camera_one_model"


@dataclass(frozen=True)
class Verdict:
    """Step 1 outcome. `ok` false stops the reduction; `applicable` false keeps the result as history."""
    ok: bool
    applicable: bool
    code: str


@dataclass(frozen=True)
class Placement:
    """Step 4 outcome: where the result landed and which estimate was current before it."""
    kind: str
    estimate_id: str | None
    reason: str
    previous: Any = None


class ReplayCore:
    """The single writer. Frames, signals, worker results and the clock come from outside; every
    record in the store is written here, inside a transaction, after validation."""

    def __init__(self, store: ReplayStore, worker: Worker, *, owner_epoch: int = 1,
                 pin_budget_bytes: int = 1 << 20, period_ms: int = 500, retention_ms: int = 10_000):
        if owner_epoch < 1 or pin_budget_bytes < 1 or period_ms < 1 or retention_ms < period_ms:
            raise ContractError("invalid_core_settings")
        self.store, self.worker, self.clock = store, worker, store.clock
        self.owner_epoch, self.pin_budget_bytes = owner_epoch, pin_budget_bytes
        self.period_ms, self.retention_ms = period_ms, retention_ms
        self.sources: dict[tuple[str, str], BoundedSource] = {}
        self.watches: dict[str, Watch] = {}
        for row in store.rows("SELECT body FROM watches"):
            body = json.loads(row["body"])
            self.watches[body["id"]] = Watch(**{**body, "candidates": tuple(body["candidates"])})
        # One job at a time survives a restart: a stored result whose availability time has not passed still occupies the worker.
        busy = store.row("SELECT COALESCE(MAX(available_ms),0) AS until FROM attempts WHERE status='stored' AND reduced=0")
        self._busy_until_ms = busy["until"]
        self._recover()

    @property
    def now(self) -> int:
        return self.clock.now_ms

    @property
    def context_version(self) -> int:
        return int(self.store.meta("context_version", "0"))

    def advance(self, now_ms: int) -> int:
        return self.clock.advance(now_ms)

    def attach(self, source: BoundedSource) -> None:
        self.sources[(source.source, source.session)] = source
        for row in self.store.rows("SELECT sequence FROM evidence WHERE source=? AND session=? AND pinned_by IS NOT NULL",
                                   (source.source, source.session)):
            source.pin(row["sequence"])

    def register_watch(self, watch: Watch) -> None:
        body = encoded(asdict(watch))
        known = self.store.row("SELECT revision, body FROM watches WHERE id=?", (watch.id,))
        if known is not None and known["revision"] > watch.revision:
            raise ContractError("watch_revision_regressed")
        if known is not None and known["revision"] == watch.revision and known["body"] != body:
            raise ContractError("watch_changed_without_new_revision")
        with self.store.transaction():
            self.store.db.execute("INSERT OR REPLACE INTO watches VALUES(?,?,?)", (watch.id, watch.revision, body))
        self.watches[watch.id] = watch

    # Intake: frames and CPU signals become records and coverage. Nothing here touches estimates.

    def ingest(self, frame: Frame) -> str:
        """Commit one published frame: frame record, evidence handle and `captured` coverage."""
        self.clock.check(frame.published_ms, f"frame:{frame.handle}")
        with self.store.transaction():
            known = self.store.row("SELECT * FROM frames WHERE handle=?", (frame.handle,))
            if known is not None:
                if any(known[k] != getattr(frame, k) for k in ("captured_ms", "published_ms", "size_bytes", "content")):
                    raise ContractError("frame_identity_conflict")
                return "duplicate"
            self.store.db.execute("INSERT INTO frames VALUES(?,?,?,?,?,?,?,?)", (
                frame.handle, frame.source, frame.session, frame.sequence, frame.captured_ms, frame.published_ms,
                frame.size_bytes, frame.content))
            self.store.db.execute("INSERT INTO evidence VALUES(?,?,?,?,?,?,?,?,NULL)", (
                frame.handle, frame.source, frame.session, frame.sequence, frame.captured_ms, frame.published_ms,
                frame.published_ms + self.retention_ms, frame.size_bytes))
            self._cover(frame.source, "captured", max(0, frame.captured_ms - self.period_ms), frame.captured_ms, frame.handle)
            return "accepted"

    def pull(self) -> int:
        """Ingest every frame the attached sources have published up to the cutoff."""
        accepted = 0
        for source in self.sources.values():
            for frame in source.readable(self.clock):
                accepted += self.ingest(frame) == "accepted"
                source.acknowledge(frame.sequence)
        return accepted

    def signal(self, source: str, from_ms: int, to_ms: int, kind: str, published_ms: int) -> None:
        """A CPU signal (change gate, zone meter) covers its interval at level `signal`."""
        if to_ms < from_ms or published_ms < to_ms:
            raise ContractError("invalid_signal")
        self.clock.check(published_ms, f"signal:{source}:{kind}")
        with self.store.transaction():
            self._cover(source, "signal", from_ms, to_ms, digest(["signal", source, kind, from_ms, to_ms]))

    def _cover(self, source: str, level: str, from_ms: int, to_ms: int, ref: str) -> None:
        self.store.db.execute("INSERT OR IGNORE INTO coverage VALUES(?,?,?,?,?,?)",
                              (source, level, from_ms, to_ms, self.now, ref))

    # Jobs: a request pins evidence within the budget and queues one logical job; refusals are explicit.

    def request(self, watch_id: str, from_ms: int, to_ms: int, *, deadline_ms: int, kind: str = "verify",
                priority: str = "discriminating_check") -> dict:
        watch = self.watches.get(watch_id)
        if watch is None:
            raise ContractError("unknown_watch")
        if to_ms < from_ms or priority not in PRIORITIES or deadline_ms < self.now:
            raise ContractError("invalid_request")
        self.clock.check(to_ms, f"request:{watch_id}")
        now = self.now
        with self.store.transaction():
            frames = self.store.frames(watch.source, from_ms, to_ms)
            if not frames:
                self._question(None, "not_observed", watch.source, from_ms, to_ms, {"watch": watch_id})
                return {"status": "refused", "reason": "not_observed"}
            evidence = [self.store.evidence(f["handle"]) for f in frames]
            lost = [e for e in evidence if e["pinned_by"] is None and e["available_until_ms"] < now]
            if lost:
                self._question(None, "evidence_lost_before_inspection", watch.source, lost[0]["captured_ms"],
                               lost[-1]["captured_ms"], {"reason": "retention_expired", "watch": watch_id,
                                                         "handles": [e["handle"] for e in lost]})
            kept = [e for e in evidence if e["pinned_by"] is not None or e["available_until_ms"] >= now]
            if not kept:
                return {"status": "refused", "reason": "evidence_lost_before_inspection"}
            handles = [e["handle"] for e in kept]
            job_id = digest(["job", watch.id, watch.revision, kind, handles])
            if self.store.row("SELECT 1 FROM jobs WHERE id=?", (job_id,)) is not None:
                return {"status": "known", "job_id": job_id}
            needed = sum(e["size_bytes"] for e in kept if e["pinned_by"] is None)
            pinned = self.store.row("SELECT COALESCE(SUM(size_bytes),0) AS total FROM evidence WHERE pinned_by IS NOT NULL")["total"]
            if pinned + needed > self.pin_budget_bytes:
                self._question(None, "evidence_lost_before_inspection", watch.source, from_ms, to_ms, {
                    "reason": "pin_budget_full", "watch": watch_id, "needed_bytes": needed,
                    "pinned_bytes": pinned, "budget_bytes": self.pin_budget_bytes})
                return {"status": "refused", "reason": "pin_budget_full"}
            manifest_ref = digest(handles)
            self.store.db.execute("INSERT OR IGNORE INTO manifests VALUES(?,?,?,?)",
                                  (manifest_ref, encoded(handles), sum(e["size_bytes"] for e in kept), now))
            for e in kept:
                if e["pinned_by"] is None:
                    self.store.db.execute("UPDATE evidence SET pinned_by=? WHERE handle=?", (job_id, e["handle"]))
                source = self.sources.get((e["source"], e["session"]))
                if source is not None:
                    source.pin(e["sequence"])
            self.store.db.execute("INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                job_id, watch.id, watch.revision, watch.mission_version, self.context_version, self.owner_epoch,
                kind, PRIORITIES[priority], watch.source, kept[0]["session"], kept[0]["sequence"],
                kept[-1]["sequence"], from_ms, to_ms, manifest_ref, now, deadline_ms, "pending"))
            return {"status": "queued", "job_id": job_id, "handles": len(handles)}

    def run_pending(self) -> dict | None:
        """Run at most one job on the worker and store its raw result. Nothing is reduced here."""
        now = self.now
        if now < self._busy_until_ms:
            return None
        with self.store.transaction():
            job = self.store.row("SELECT * FROM jobs WHERE status='pending' ORDER BY priority, deadline_ms, created_ms LIMIT 1")
            if job is None:
                return None
            if job["deadline_ms"] < now:
                self._close_job(job, "expired", "missed_deadline")
                return {"job_id": job["id"], "status": "expired"}
            handles = json.loads(self.store.row("SELECT handles FROM manifests WHERE ref=?", (job["manifest_ref"],))["handles"])
            try:
                for handle in handles:
                    self._read_evidence(handle)
            except EvidenceLost as error:
                self._close_job(job, "failed", "evidence_lost_before_inspection", {"handle": str(error)})
                return {"job_id": job["id"], "status": "failed"}
            attempt = 1 + self.store.row("SELECT COALESCE(MAX(attempt),0) AS n FROM attempts WHERE job_id=?", (job["id"],))["n"]
            watch = self.watches[job["watch_id"]]
            request = InferenceRequest(job["id"], attempt, watch.predicate, watch.candidates, tuple(handles),
                                       job["from_ms"], job["to_ms"], job["deadline_ms"])
            self.store.db.execute("INSERT INTO attempts(job_id,attempt,owner_epoch,status,started_ms,queue_ms) VALUES(?,?,?,?,?,?)",
                                  (job["id"], attempt, self.owner_epoch, "running", now, now - job["created_ms"]))
            self.store.db.execute("UPDATE jobs SET status='running' WHERE id=?", (job["id"],))
        try:
            result = self.worker.infer(request)
        except Exception as error:
            with self.store.transaction():
                self.store.db.execute("UPDATE attempts SET status='failed', reduce_outcome=? WHERE job_id=? AND attempt=?",
                                      (f"worker_failed:{type(error).__name__}", job["id"], attempt))
                self._retry_or_close(job, attempt, "inference_failed")
            return {"job_id": job["id"], "attempt": attempt, "status": "failed"}
        latency_ms = max(0, int(result.latency_ms))
        with self.store.transaction():
            self.store.db.execute("""UPDATE attempts SET status='stored', available_ms=?, inference_ms=?, model_id=?, raw_result=?
                                     WHERE job_id=? AND attempt=?""",
                                  (now + latency_ms, latency_ms, result.model_id, encoded(result.payload), job["id"], attempt))
            self.store.db.execute("UPDATE jobs SET status='stored' WHERE id=?", (job["id"],))
        self._busy_until_ms = now + latency_ms
        return {"job_id": job["id"], "attempt": attempt, "status": "stored", "available_ms": now + latency_ms}

    def _read_evidence(self, handle: str):
        evidence = self.store.evidence(handle)
        source = self.sources.get((evidence["source"], evidence["session"]))
        if source is not None:
            return source.read(evidence["sequence"], self.clock)
        if evidence["pinned_by"] is None and evidence["available_until_ms"] < self.now:
            raise EvidenceLost(handle)
        return evidence

    def _retry_or_close(self, job, attempt: int, reason: str) -> None:
        """Job ledger rule: one more local attempt of the same logical job within its deadline, never more."""
        if attempt < MAX_ATTEMPTS and job["deadline_ms"] >= self.now:
            self.store.db.execute("UPDATE jobs SET status='pending' WHERE id=?", (job["id"],))
        else:
            self._close_job(job, "failed", reason)

    def _close_job(self, job, status: str, reason: str, detail: dict | None = None) -> None:
        self.store.db.execute("UPDATE jobs SET status=? WHERE id=?", (status, job["id"]))
        self._release(job["id"])
        self._question(None, reason, job["source"], job["from_ms"], job["to_ms"], {"job": job["id"], **(detail or {})})
        attempt = self.store.row("SELECT COALESCE(MAX(attempt),0) AS n FROM attempts WHERE job_id=?", (job["id"],))["n"]
        self._trace(job, attempt, "", self.now - job["created_ms"], 0, f"{status}:{reason}", None)

    def _release(self, job_id: str) -> None:
        job = self.store.row("SELECT manifest_ref FROM jobs WHERE id=?", (job_id,))
        handles = json.loads(self.store.row("SELECT handles FROM manifests WHERE ref=?", (job["manifest_ref"],))["handles"])
        still_needed: set[str] = set()
        for row in self.store.rows("""SELECT m.handles FROM jobs j JOIN manifests m ON m.ref=j.manifest_ref
                                      WHERE j.status IN ('pending','running','stored') AND j.id<>?""", (job_id,)):
            still_needed.update(json.loads(row["handles"]))
        for handle in handles:
            if handle in still_needed:
                continue
            evidence = self.store.row("SELECT * FROM evidence WHERE handle=?", (handle,))
            if evidence["pinned_by"] is None:
                continue
            self.store.db.execute("UPDATE evidence SET pinned_by=NULL WHERE handle=?", (handle,))
            source = self.sources.get((evidence["source"], evidence["session"]))
            if source is not None:
                source.release(evidence["sequence"])

    def _recover(self) -> None:
        """Attempts left running by a previous owner have an unknown outcome; they are never assumed finished."""
        with self.store.transaction():
            for row in self.store.rows("SELECT * FROM attempts WHERE status='running'"):
                self.store.db.execute("UPDATE attempts SET status='unknown', reduce_outcome='owner_restarted' WHERE job_id=? AND attempt=?",
                                      (row["job_id"], row["attempt"]))
                job = self.store.row("SELECT * FROM jobs WHERE id=?", (row["job_id"],))
                self._retry_or_close(job, row["attempt"], "unknown_at_restart")

    # The reducer: six named steps, one transaction, one trace per stored result. Safe to repeat.

    def reduce_pending(self) -> list[Trace]:
        """Reduce stored results whose availability time has passed, oldest first."""
        rows = self.store.rows("""SELECT a.job_id, a.attempt FROM attempts a JOIN jobs j ON j.id=a.job_id
                                  WHERE a.status='stored' AND a.reduced=0 AND a.available_ms<=?
                                  ORDER BY a.available_ms, j.created_ms, a.attempt""", (self.now,))
        return [self._reduce(r["job_id"], r["attempt"]) for r in rows]

    def tick(self) -> dict:
        pulled, ran, traces = self.pull(), self.run_pending(), self.reduce_pending()
        return {"now_ms": self.now, "frames": pulled, "ran": ran, "reduced": [t.outcome for t in traces]}

    def _reduce(self, job_id: str, attempt_no: int) -> Trace:
        with self.store.transaction():
            job = self.store.row("SELECT * FROM jobs WHERE id=?", (job_id,))
            attempt = self.store.row("SELECT * FROM attempts WHERE job_id=? AND attempt=?", (job_id, attempt_no))
            watch = self.watches.get(job["watch_id"])
            payload = json.loads(attempt["raw_result"])
            verdict = self._step1_validate(job, attempt, watch, payload)
            estimate_id = None
            if verdict.ok:
                interval = self._step2_attach_interval(job)
                situation = self._situation(watch, interval)
                observation = self._step3_observe(job, attempt, watch, situation, interval, payload)
                placement = self._step4_update_estimate(watch, situation, observation, interval, verdict)
                self._step5_keep_alternatives(watch, situation, interval, placement)
                self._step6_open_questions(watch, situation, observation, interval, placement)
                estimate_id, status = placement.estimate_id, "valid"
                outcome = {"current": f"current:{observation['picked']}",
                           "history": f"history:{placement.reason}:{observation['picked']}",
                           "repeat": "repeat"}.get(placement.kind, f"undecided:{observation['picked']}")
            else:
                outcome, status = f"rejected:{verdict.code}", "rejected"
            self.store.db.execute("UPDATE attempts SET reduced=1, status=?, reduce_outcome=? WHERE job_id=? AND attempt=?",
                                  (status, outcome, job_id, attempt_no))
            self.store.db.execute("UPDATE jobs SET status='reduced' WHERE id=?", (job_id,))
            self._release(job_id)
            self.store.set_meta("context_version", str(self.context_version + 1))
            return self._trace(job, attempt_no, attempt["model_id"] or "", attempt["queue_ms"],
                               attempt["inference_ms"] or 0, outcome, estimate_id)

    def _step1_validate(self, job, attempt, watch: Watch | None, payload: dict) -> Verdict:
        """Step 1: provenance, availability cutoff, quality, job identity and watch revision."""
        if payload.get("job_id") != job["id"] or payload.get("attempt") != attempt["attempt"]:
            return Verdict(False, False, "job_identity_mismatch")
        if payload.get("model_id") != attempt["model_id"]:
            return Verdict(False, False, "provenance_mismatch")
        self.clock.check(attempt["available_ms"], f"result:{job['id']}:{attempt['attempt']}")
        handles = json.loads(self.store.row("SELECT handles FROM manifests WHERE ref=?", (job["manifest_ref"],))["handles"])
        if any(self.store.evidence(h)["available_from_ms"] > attempt["started_ms"] for h in handles):
            return Verdict(False, False, "evidence_not_readable_at_inspection")
        try:
            validate(RESULT_SCHEMA, payload)
        except ContractError:
            return Verdict(False, False, "invalid_result")
        if watch is None:
            return Verdict(False, False, "unknown_watch")
        if payload["predicate"] != watch.predicate or set(payload["scores"]) != set(watch.candidates):
            return Verdict(False, False, "candidate_set_mismatch")
        if self.store.row("SELECT 1 FROM attempts WHERE job_id=? AND status='valid'", (job["id"],)) is not None:
            return Verdict(False, False, "duplicate_result")
        if job["watch_revision"] != watch.revision:
            return Verdict(True, False, "stale_watch_revision")
        if attempt["owner_epoch"] != self.owner_epoch:
            return Verdict(True, False, "foreign_owner_epoch")
        return Verdict(True, True, "ok")

    def _step2_attach_interval(self, job) -> tuple[int, int]:
        """Step 2: the result belongs to the capture span of the inspected frames, not to when inference finished."""
        handles = json.loads(self.store.row("SELECT handles FROM manifests WHERE ref=?", (job["manifest_ref"],))["handles"])
        captured = [self.store.evidence(h)["captured_ms"] for h in handles]
        interval = (max(0, min(captured) - self.period_ms), max(captured))
        self._cover(job["source"], "inspected", interval[0], interval[1], job["id"])
        return interval

    def _step3_observe(self, job, attempt, watch: Watch, situation, interval: tuple[int, int], payload: dict) -> dict:
        """Step 3: support or contradiction per hypothesis. The same frames read again are not a new observation."""
        picked = pick(payload["scores"], watch.threshold)
        observation_id = digest(["observation", job["id"], attempt["attempt"]])
        repeat = self.store.row("""SELECT id FROM observations WHERE situation_id=? AND frames_digest=? AND id<>?
                                   ORDER BY observed_ms, id LIMIT 1""", (situation["id"], job["manifest_ref"], observation_id))
        self.store.db.execute("INSERT OR IGNORE INTO observations VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (
            observation_id, situation["id"], job["id"], attempt["attempt"], job["manifest_ref"], interval[0], interval[1],
            picked, encoded(payload["scores"]), payload["score_kind"], self.now, repeat["id"] if repeat else None))
        if repeat is None:
            for hypothesis in watch.candidates:
                alternative = self._alternative(situation, hypothesis)
                supporting, contradicting = json.loads(alternative["supporting"]), json.loads(alternative["contradicting"])
                if hypothesis == picked:
                    supporting.append(observation_id)
                elif picked not in (UNKNOWN, INSUFFICIENT):
                    contradicting.append(observation_id)
                self.store.db.execute("""UPDATE alternatives SET supporting=?, contradicting=?, score_kind=?, score=?,
                                         last_verified_ms=? WHERE situation_id=? AND hypothesis_id=?""",
                                      (encoded(supporting), encoded(contradicting), payload["score_kind"],
                                       payload["scores"][hypothesis], interval[1], situation["id"], hypothesis))
            self.store.db.execute("""INSERT INTO search_index(source,predicate,hypothesis_id,from_ms,to_ms,observation_id,indexed_ms)
                                     VALUES(?,?,?,?,?,?,?)""",
                                  (watch.source, watch.predicate, picked, interval[0], interval[1], observation_id, self.now))
        return {"id": observation_id, "picked": picked, "scores": payload["scores"], "repeat_of": repeat["id"] if repeat else None}

    def _step4_update_estimate(self, watch: Watch, situation, observation: dict, interval: tuple[int, int],
                               verdict: Verdict) -> Placement:
        """Step 4: the current estimate changes only for an applicable, fresh result; everything else is history."""
        if observation["repeat_of"] is not None:
            return Placement("repeat", None, "same_frames_already_observed")
        if observation["picked"] in (UNKNOWN, INSUFFICIENT):
            return Placement("none", None, f"undecided:{observation['picked']}")
        current = self.store.current_estimate(situation["merge_key"])
        if not verdict.applicable:
            reason = verdict.code
        elif current is not None and interval[1] < current["to_ms"]:
            reason = "older_than_current"
        elif self.now - interval[1] > watch.freshness_ms:
            reason = "stale"
        else:
            reason = None
        estimate_id = digest(["estimate", observation["id"]])
        if reason is None:
            self.store.db.execute("UPDATE estimates SET current=0, placement='history', reason='superseded' WHERE situation_id=? AND current=1",
                                  (situation["id"],))
        self.store.db.execute("INSERT OR IGNORE INTO estimates VALUES(?,?,?,?,?,?,?,?,?,?)", (
            estimate_id, situation["id"], observation["id"], observation["picked"], interval[0], interval[1], self.now,
            int(reason is None), "current" if reason is None else "history", reason or "fresh"))
        return Placement("current" if reason is None else "history", estimate_id, reason or "fresh", current)

    def _step5_keep_alternatives(self, watch: Watch, situation, interval: tuple[int, int], placement: Placement) -> None:
        """Step 5: competing explanations stay. The candidate set is fixed, no alternative is removed by one
        result, and the situation revision records why it changed."""
        for hypothesis in watch.candidates:
            self._alternative(situation, hypothesis)
        self.store.db.execute("""UPDATE situations SET from_ms=MIN(from_ms,?), to_ms=MAX(to_ms,?), coverage_level='inspected',
                                 mission_version=?, context_version=?, revision=revision+1, revision_reason=? WHERE id=?""",
                              (interval[0], interval[1], watch.mission_version, self.context_version,
                               f"{placement.kind}:{placement.reason}", situation["id"]))

    def _step6_open_questions(self, watch: Watch, situation, observation: dict, interval: tuple[int, int],
                              placement: Placement) -> None:
        """Step 6: when the decision could change, open a discriminating check or an unresolved question."""
        if observation["repeat_of"] is not None:
            return
        picked = observation["picked"]
        if picked in (UNKNOWN, INSUFFICIENT):
            self._question(situation["id"], "not_decidable", watch.source, interval[0], interval[1],
                           {"picked": picked, "observation": observation["id"]})
            return
        best, second = ranked(observation["scores"])[:2]
        if observation["scores"][best] - observation["scores"][second] < watch.margin:
            self._check(situation, "recheck_next_frame", [best, second], interval)
        if placement.kind == "current" and placement.previous is not None and placement.previous["hypothesis_id"] != picked:
            self._check(situation, "prior_free_verify", [placement.previous["hypothesis_id"], picked], interval)

    # Record helpers used inside the owner's transactions.

    def _situation(self, watch: Watch, interval: tuple[int, int]):
        found = self.store.row("SELECT * FROM situations WHERE merge_key=?", (watch.merge_key,))
        if found is None:
            self.store.db.execute("INSERT INTO situations VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (
                digest(["situation", watch.merge_key]), watch.merge_key, watch.source, watch.region, watch.predicate,
                interval[0], interval[1], "inspected", watch.mission_version, self.context_version, 0, "created"))
            found = self.store.row("SELECT * FROM situations WHERE merge_key=?", (watch.merge_key,))
        return found

    def _alternative(self, situation, hypothesis: str):
        found = self.store.row("SELECT * FROM alternatives WHERE situation_id=? AND hypothesis_id=?", (situation["id"], hypothesis))
        if found is None:
            self.store.db.execute("INSERT INTO alternatives VALUES(?,?,'[]','[]','none',NULL,NULL,?)",
                                  (situation["id"], hypothesis, SHARED_ERROR_GROUP))
            found = self.store.row("SELECT * FROM alternatives WHERE situation_id=? AND hypothesis_id=?", (situation["id"], hypothesis))
        return found

    def _question(self, situation_id: str | None, kind: str, source: str, from_ms: int, to_ms: int, detail: dict) -> None:
        body = encoded(detail)
        self.store.db.execute("INSERT OR IGNORE INTO questions VALUES(?,?,?,?,?,?,?,?,'open')", (
            digest(["question", kind, source, from_ms, to_ms, body]), situation_id, kind, source, from_ms, to_ms, body, self.now))

    def _check(self, situation, kind: str, separates: list[str], interval: tuple[int, int]) -> None:
        self.store.db.execute("INSERT OR IGNORE INTO checks VALUES(?,?,?,?,?,?,?,'open')", (
            digest(["check", situation["id"], kind, separates, list(interval)]), situation["id"], kind, encoded(separates),
            interval[0], interval[1], self.now))

    def _trace(self, job, attempt: int, model_id: str, queue_ms: int, inference_ms: int, outcome: str,
               estimate_id: str | None) -> Trace:
        trace = Trace(job["id"], attempt, job["source"], job["session"], job["sequence_from"], job["sequence_to"],
                      job["mission_version"], job["watch_revision"], job["context_version"], job["owner_epoch"],
                      model_id, queue_ms, inference_ms, job["manifest_ref"], outcome, estimate_id, self.now)
        self.store.db.execute("INSERT OR REPLACE INTO traces VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                              tuple(asdict(trace).values()))
        return trace
