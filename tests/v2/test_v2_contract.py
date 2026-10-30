"""Causal contract: reads past the cutoff fail; evidence loss, refusals and rejected results are explicit."""
from dataclasses import replace

import pytest

from seer_agent import ContractError
from seer_agent.v2 import INSUFFICIENT, BoundedSource, CausalClock, CausalViolation, EvidenceLost, FakeWorker, Frame
from v2_harness import LIQUID, SHADOW, WATCH, frame

KEY = WATCH.merge_key


def test_reads_past_the_cutoff_raise(harness):
    h = harness(FakeWorker(default=(100, LIQUID)))
    h.offer(frame(1, captured=500, published=2000), frame(2, captured=1000))
    h.run_until(1000)
    assert [f["sequence"] for f in h.store.frames("cam-a", 0, 1000)] == [2]
    with pytest.raises(CausalViolation):
        h.source.read(1, h.clock)  # published only at 2000
    with pytest.raises(CausalViolation):
        h.store.frames("cam-a", 0, 1001)
    with pytest.raises(CausalViolation):
        h.store.coverage("cam-a", 0, 1500)
    with pytest.raises(CausalViolation):
        h.store.search("cam-a", "liquid_on_floor", 1500)
    with pytest.raises(CausalViolation):
        h.core.request("leak-a", 0, 1500, deadline_ms=5000)
    with pytest.raises(CausalViolation):
        h.core.ingest(frame(3, captured=900, published=1200))
    with pytest.raises(CausalViolation):
        h.core.signal("cam-a", 0, 1000, "zone_change", published_ms=1100)
    with pytest.raises(CausalViolation):
        h.core.advance(999)
    with pytest.raises(CausalViolation):
        CausalClock(-1)
    with pytest.raises(ContractError, match="published_before_captured"):
        Frame("cam-a", "s1", 9, 1000, 900)
    # A stored result is not readable before inference finished on the replay timeline.
    job = h.core.request("leak-a", 0, 1000, deadline_ms=5000)["job_id"]
    assert h.core.run_pending()["available_ms"] == 1100
    with pytest.raises(CausalViolation):
        h.store.result(job, 1)
    assert h.core.reduce_pending() == []
    h.core.advance(1100)
    assert h.store.result(job, 1)["scores"] == LIQUID
    assert [t.outcome for t in h.core.reduce_pending()] == ["current:liquid"]


def test_full_pin_budget_refuses_and_records_evidence_lost(harness):
    h = harness(FakeWorker(default=(100, SHADOW)), budget=350)
    h.offer(*[frame(s, size=100) for s in range(1, 9)])
    h.run_until(4000)
    first = h.core.request("leak-a", 0, 1500, deadline_ms=30_000)  # three frames, 300 bytes
    assert first["status"] == "queued" and first["handles"] == 3
    assert h.core.request("leak-a", 3000, 4000, deadline_ms=30_000) == {"status": "refused", "reason": "pin_budget_full"}
    lost = h.store.questions("evidence_lost_before_inspection")
    assert len(lost) == 1 and (lost[0]["from_ms"], lost[0]["to_ms"], lost[0]["status"]) == (3000, 4000, "open")
    assert lost[0]["detail"] == {"reason": "pin_budget_full", "watch": "leak-a", "needed_bytes": 300,
                                 "pinned_bytes": 300, "budget_bytes": 350}
    assert h.store.row("SELECT COUNT(*) AS n FROM jobs")["n"] == 1
    h.run_until(4200)  # the first look is reduced and releases its pins
    assert h.store.row("SELECT COALESCE(SUM(size_bytes),0) AS n FROM evidence WHERE pinned_by IS NOT NULL")["n"] == 0
    assert h.core.request("leak-a", 3000, 4000, deadline_ms=30_000)["status"] == "queued"


def test_evidence_lost_to_retention_is_recorded_and_the_rest_is_inspected(harness):
    h = harness(FakeWorker(default=(100, SHADOW)), retention=2000)
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    queued = h.core.request("leak-a", 0, 4000, deadline_ms=30_000)
    assert queued["status"] == "queued" and queued["handles"] == 5  # frames 4 to 8 are still within retention
    lost = h.store.questions("evidence_lost_before_inspection")
    assert [(q["from_ms"], q["to_ms"]) for q in lost] == [(500, 1500)]
    assert lost[0]["detail"]["reason"] == "retention_expired" and len(lost[0]["detail"]["handles"]) == 3
    h.run_until(4500)
    observation = h.store.observations(KEY)[0]
    assert (observation["from_ms"], observation["to_ms"]) == (1500, 4000)  # the interval actually inspected
    assert h.store.coverage("cam-a", 0, 4000) == [(0, 1500, "captured"), (1500, 4000, "inspected")]


def test_stale_watch_revision_is_decided_under_its_own_revision_and_invalid_results_are_rejected(harness):
    borderline = {"liquid": 0.65, "shadow": 0.3, INSUFFICIENT: 0.05}  # clears threshold 0.6, not 0.7
    h = harness(FakeWorker({(0, 2000): (100, borderline), (2500, 4000): (100, borderline),
                            (0, 1000): (100, {"liquid": "high"})}))
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    h.core.request("leak-a", 0, 2000, deadline_ms=30_000)
    h.core.run_pending()
    h.core.register_watch(replace(WATCH, revision=2, threshold=0.7))  # the allowed checks changed before reduction
    h.run_until(4500)
    # Decided with the threshold it was requested under (0.6 picks liquid), then filed as history under revision 2.
    assert [t.outcome for t in h.store.traces()] == ["history:stale_watch_revision:liquid"]
    assert h.store.current_estimate(KEY) is None and h.store.history(KEY)[0]["reason"] == "stale_watch_revision"
    # A look requested under revision 2 is decided with 0.7, so the same scores are undecided.
    assert h.core.request("leak-a", 2500, 4000, deadline_ms=30_000)["status"] == "queued"
    h.run_until(5000)
    assert (h.store.traces()[-1].outcome, h.store.traces()[-1].watch_revision) == ("undecided:unknown", 2)
    assert [(q["from_ms"], q["to_ms"]) for q in h.store.questions("not_decidable")] == [(2000, 4000)]
    bad = h.core.request("leak-a", 0, 1000, deadline_ms=30_000)
    h.run_until(5500)
    assert h.store.traces()[-1].outcome == "rejected:invalid_result"
    assert [a["status"] for a in h.store.rows("SELECT status FROM attempts ORDER BY started_ms")] == ["valid", "valid", "rejected"]
    assert len(h.store.observations(KEY)) == 2  # a rejected result is not evidence
    assert h.store.row("SELECT status FROM jobs WHERE id=?", (bad["job_id"],))["status"] == "reduced"  # not retried
    assert h.worker.calls == 3
    with pytest.raises(ContractError, match="watch_revision_regressed"):
        h.core.register_watch(WATCH)


def test_undecided_and_close_results_open_questions_and_checks(harness):
    close = {"liquid": 0.7, "shadow": 0.6, INSUFFICIENT: 0.0}
    dark = {"liquid": 0.1, "shadow": 0.1, INSUFFICIENT: 0.8}
    h = harness(FakeWorker({(0, 2000): (100, close), (2500, 4000): (100, dark)}))
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    h.core.signal("cam-a", 1000, 2000, "zone_change", published_ms=2000)
    assert h.store.coverage("cam-a", 0, 2500) == [(0, 1000, "captured"), (1000, 2000, "signal"), (2000, 2500, "captured")]
    h.core.request("leak-a", 0, 2000, deadline_ms=30_000)
    h.core.request("leak-a", 2500, 4000, deadline_ms=30_000)
    h.run_until(5000)
    assert sorted(t.outcome for t in h.store.traces()) == sorted(["current:liquid", f"undecided:{INSUFFICIENT}"])
    assert [(c["kind"], c["separates"]) for c in h.store.checks()] == [("recheck_next_frame", ["liquid", "shadow"])]
    question = h.store.questions("not_decidable")[0]
    assert (question["from_ms"], question["to_ms"], question["detail"]["picked"]) == (2000, 4000, INSUFFICIENT)
    assert h.store.current_estimate(KEY)["hypothesis_id"] == "liquid"  # a visibility gap is not "liquid stopped"
    assert h.store.coverage("cam-a", 0, 4000) == [(0, 4000, "inspected")]  # frame 5 stands for [2000, 2500]
    alternatives = h.store.alternatives(KEY)
    assert len(alternatives[INSUFFICIENT]["supporting"]) == 1 and alternatives["liquid"]["contradicting"] == []
    assert set(alternatives) == set(WATCH.candidates)


def test_bounded_source_dedupes_orders_and_evicts_unpinned_frames_first():
    clock = CausalClock(0)
    source = BoundedSource("cam-a", "s1", capacity=3, period_ms=500, retention_ms=1000)
    assert [source.offer(frame(s)) for s in (2, 1, 2)] == ["accepted", "accepted", "duplicate"]
    with pytest.raises(ContractError, match="frame_identity_conflict"):
        source.offer(frame(2, size=7))
    assert source.readable(clock) == []  # nothing is published yet
    clock.advance(1000)
    assert [f.sequence for f in source.readable(clock)] == [1, 2]
    assert [f.sequence for f in source.readable(clock)] == [1, 2]  # nothing acknowledged yet
    source.acknowledge(1)
    source.acknowledge(2)
    assert source.readable(clock) == []
    assert source.pin(1) and not source.pin(99)
    source.offer(frame(3))
    source.offer(frame(4))  # capacity 3: the oldest unpinned frame (2) leaves
    assert source.evictions == 1 and len(source) == 3
    with pytest.raises(EvidenceLost):
        source.read(2, clock)
    clock.advance(2500)
    assert source.read(1, clock).sequence == 1  # pinned frames outlive retention
    source.release(1)
    with pytest.raises(EvidenceLost):
        source.read(1, clock)
    with pytest.raises(ContractError, match="frame_from_another_session"):
        source.offer(Frame("cam-b", "s1", 1, 0, 0))
