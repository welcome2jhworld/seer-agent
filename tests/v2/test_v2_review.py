"""Regressions for the v2-core review findings: one test per fix (H1, M1 to M5, L2 to L5; L1 is in the backlog test)."""
import pytest

from seer_agent import ContractError
from seer_agent.v2 import (INSUFFICIENT, BoundedSource, CausalClock, CausalViolation, EvidenceLost, FakeWorker,
                           InferenceResult, ReplayCore, ReplayStore, Watch)
from v2_harness import LIQUID, SHADOW, WATCH, frame

KEY = WATCH.merge_key
OUTAGE = [frame(s) for s in (*range(1, 9), *range(19, 25))]  # captured 500..4000 and 9500..12000, nothing between


def test_look_straddling_an_outage_is_refused_and_the_gap_stays_unknown(harness):  # H1
    h = harness(FakeWorker(default=(100, SHADOW)), retention=20_000)
    h.offer(*OUTAGE)
    h.run_until(12_000)
    refused = h.core.request("leak-a", 3800, 9500, deadline_ms=30_000)  # keeps frames 4000 and 9500 only
    assert refused == {"status": "refused", "reason": "not_contiguous", "segments": [[3500, 4000], [9000, 9500]]}
    assert [(q["from_ms"], q["to_ms"]) for q in h.store.questions("not_observed")] == [(4000, 9000)]
    assert h.store.row("SELECT COUNT(*) AS n FROM jobs")["n"] == 0
    assert h.core.request("leak-a", 3800, 4000, deadline_ms=30_000)["status"] == "queued"
    h.run_until(12_500)
    assert h.store.coverage("cam-a", 0, 12_000) == [(0, 3500, "captured"), (3500, 4000, "inspected"),
                                                    (4000, 9000, "unknown"), (9000, 12_000, "captured")]
    current = h.store.current_estimate(KEY)
    assert (current["from_ms"], current["to_ms"]) == (3500, 4000)


def test_expired_job_can_be_requested_again_and_runs(harness):  # M1
    h = harness(FakeWorker(default=(100, LIQUID)))
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    first = h.core.request("leak-a", 0, 4000, deadline_ms=4100)
    h.core.advance(4200)
    assert h.core.run_pending() == {"job_id": first["job_id"], "status": "expired"}
    assert h.store.questions("missed_deadline")[0]["detail"]["job"] == first["job_id"] and h.worker.calls == 0
    second = h.core.request("leak-a", 0, 4000, deadline_ms=30_000)
    assert second["status"] == "queued" and second["job_id"] != first["job_id"]
    h.run_until(4700)
    assert h.worker.calls == 1 and h.store.current_estimate(KEY)["hypothesis_id"] == "liquid"
    assert h.core.request("leak-a", 0, 4000, deadline_ms=30_000) == {"status": "known", "job_id": second["job_id"]}
    assert [t.outcome for t in h.store.traces()] == ["expired:missed_deadline", "current:liquid"]


def test_restart_with_a_raised_epoch_adopts_stored_results_and_audits_unknown_epochs(harness):  # M2
    h = harness(FakeWorker(default=(1000, LIQUID)))
    assert h.core.owner_epoch == 1
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    h.core.request("leak-a", 0, 2000, deadline_ms=30_000)
    h.core.run_pending()
    core = h.restart(5000, owner_epoch=3)  # a successor takes a higher epoch and adopts the store's own prior work
    assert core.owner_epoch == 3 and core.known_epochs == {1, 3}
    traces = core.reduce_pending()
    assert [t.outcome for t in traces] == ["current:liquid"] and traces[0].owner_epoch == 1 and h.worker.calls == 1
    for epoch in (3, 2):  # reusing or lowering the epoch is refused
        with pytest.raises(ContractError, match="owner_epoch_not_advanced"):
            ReplayCore(h.store, h.worker, owner_epoch=epoch)
    # A result stored under an epoch this store never had is audited only: no observation, no estimate.
    job = core.request("leak-a", 2500, 4000, deadline_ms=30_000)["job_id"]
    core.run_pending()
    h.store.db.execute("UPDATE attempts SET owner_epoch=99 WHERE job_id=?", (job,))
    h.run_until(6500)
    assert h.store.traces()[-1].outcome == "audited:foreign_owner_epoch"
    assert h.store.row("SELECT status FROM attempts WHERE job_id=?", (job,))["status"] == "audited"
    assert len(h.store.observations(KEY)) == 1 and h.store.current_estimate(KEY)["to_ms"] == 2000


def test_added_candidate_in_a_newer_revision_does_not_reject_an_older_result(harness):  # M3
    h = harness(FakeWorker(default=(100, LIQUID)))
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    h.core.request("leak-a", 0, 2000, deadline_ms=30_000)
    h.core.run_pending()
    wider = Watch("leak-a", "cam-a", "liquid_on_floor", ("liquid", "shadow", "reflection", INSUFFICIENT), revision=2)
    h.core.register_watch(wider)
    h.run_until(4500)
    assert [t.outcome for t in h.store.traces()] == ["history:stale_watch_revision:liquid"]
    alternatives = h.store.alternatives(KEY)
    assert set(alternatives) == set(wider.candidates) and alternatives["reflection"]["supporting"] == []
    assert len(h.store.observations(KEY)) == 1
    # A look under revision 2 is scored against the wider candidate set.
    h.worker.default = (100, {**LIQUID, "reflection": 0.0})
    assert h.core.request("leak-a", 2500, 4000, deadline_ms=30_000)["status"] == "queued"
    h.run_until(5000)
    assert (h.store.traces()[-1].outcome, h.store.traces()[-1].watch_revision) == ("current:liquid", 2)


def test_successor_with_an_earlier_clock_is_refused(harness):  # M4
    h = harness(FakeWorker(default=(100, LIQUID)))
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(5000)
    assert h.store.meta("cutoff_ms") == "5000"
    h.store.close()
    with pytest.raises(CausalViolation, match="clock_behind_committed_cutoff"):
        ReplayStore(h.path, CausalClock(1000))
    store = ReplayStore(h.path, CausalClock(5000))  # the committed cutoff itself is allowed
    assert len(store.frames("cam-a", 0, 5000)) == 8


def test_current_estimate_outside_the_freshness_window_is_demoted(harness):  # M5
    h = harness(FakeWorker(default=(100, LIQUID)), retention=60_000)
    h.offer(*OUTAGE)
    h.run_until(12_000)
    h.core.request("leak-a", 0, 4000, deadline_ms=30_000)
    h.run_until(12_500)
    assert h.store.current_estimate(KEY)["to_ms"] == 4000
    h.run_until(45_000)
    h.core.request("leak-a", 9000, 12_000, deadline_ms=60_000)
    h.run_until(45_500)
    assert h.store.current_estimate(KEY) is None  # nothing older than freshness_ms stays current
    assert [(e["to_ms"], e["reason"]) for e in h.store.history(KEY)] == [(4000, "expired"), (12_000, "stale")]
    assert h.store.traces()[-1].outcome == "history:stale:liquid"


def test_unserializable_result_is_rejected_without_wedging_the_job(harness):  # L2
    h = harness(FakeWorker(default=(100, {"liquid": float("nan"), "shadow": 0.1, INSUFFICIENT: 0.1})))
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    job = h.core.request("leak-a", 0, 4000, deadline_ms=30_000)["job_id"]
    assert h.core.run_pending() == {"job_id": job, "attempt": 1, "status": "rejected"}
    assert h.store.row("SELECT status FROM jobs WHERE id=?", (job,))["status"] == "reduced"
    assert h.store.rows("SELECT handle FROM evidence WHERE pinned_by IS NOT NULL") == []
    assert [t.outcome for t in h.store.traces()] == ["rejected:unserializable_result"]
    assert h.core.run_pending() is None and h.worker.calls == 1


def test_result_finishing_after_the_deadline_is_history_with_a_missed_deadline_record(harness):  # L3
    h = harness(FakeWorker(default=(1000, LIQUID)))
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    job = h.core.request("leak-a", 0, 4000, deadline_ms=4600)["job_id"]
    h.run_until(5000)
    assert [t.outcome for t in h.store.traces()] == ["history:missed_deadline:liquid"]
    assert h.store.current_estimate(KEY) is None and h.store.history(KEY)[0]["reason"] == "missed_deadline"
    assert h.store.questions("missed_deadline")[0]["detail"] == {"job": job, "attempt": 1, "available_ms": 5000, "deadline_ms": 4600}


def test_result_from_an_undeclared_model_is_rejected(harness):  # L4
    h = harness(FakeWorker(default=(100, LIQUID, {"model_id": "other-model"})))
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    h.core.request("leak-a", 0, 4000, deadline_ms=30_000)
    h.run_until(4500)
    assert [t.outcome for t in h.store.traces()] == ["rejected:provenance_mismatch"]
    assert h.store.traces()[0].model_id == "fake-verify-1"  # the trace names the worker that was dispatched


def test_ring_full_of_pinned_frames_drops_the_new_frame():  # L5
    clock = CausalClock(2000)
    source = BoundedSource("cam-a", "s1", capacity=2, period_ms=500, retention_ms=1000)
    source.offer(frame(2))
    source.offer(frame(3))
    assert source.offer(frame(1)) == "dropped" and source.dropped == 1  # older than everything the ring holds
    source.pin(2)
    source.pin(3)
    assert source.offer(frame(4)) == "dropped" and len(source) == 2 and source.pinned == frozenset({2, 3})
    with pytest.raises(EvidenceLost):
        source.read(4, clock)
    source.release(2)
    assert source.offer(frame(4)) == "accepted" and source.evictions == 1
    assert [f.sequence for f in source.readable(clock)] == [3, 4]


def test_restart_past_the_deadline_releases_ring_pins(harness):  # L5
    h = harness(FakeWorker(default=(1000, LIQUID)))
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    job = h.core.request("leak-a", 0, 4000, deadline_ms=4500)["job_id"]
    h.core.run_pending()
    h.store.db.execute("UPDATE attempts SET status='running', raw_result=NULL, available_ms=NULL WHERE job_id=?", (job,))
    h.store.db.execute("UPDATE jobs SET status='running' WHERE id=?", (job,))
    assert h.source.pinned == frozenset(range(1, 9))
    h.restart(5000)  # past the deadline: the unknown attempt closes the job and its pins go with it
    assert h.store.row("SELECT status FROM jobs WHERE id=?", (job,))["status"] == "failed"
    assert h.source.pinned == frozenset() and h.store.rows("SELECT handle FROM evidence WHERE pinned_by IS NOT NULL") == []
    assert h.store.questions("unknown_at_restart")[0]["detail"]["job"] == job


def test_superseded_owner_cannot_commit_and_successor_reduce_is_idempotent(harness):  # F1
    h = harness(FakeWorker(default=(1000, LIQUID)))  # owner A, epoch 1
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    a = h.core
    job = a.request("leak-a", 0, 4000, deadline_ms=30_000)["job_id"]
    a.run_pending()  # stored while A is the owner, available at 5000
    store_b = ReplayStore(h.path, CausalClock(9000))
    b = ReplayCore(store_b, h.worker)  # owner B opens the same file later with the next epoch
    assert b.owner_epoch == 2 and store_b.meta("cutoff_ms") == "9000"
    a.advance(4100)
    with pytest.raises(CausalViolation, match="clock_behind_committed_cutoff"):  # a stale owner with an older clock
        a.ingest(frame(9, captured=4100))
    a.advance(9000)
    for action in (lambda: a.ingest(frame(9, captured=4100)), a.run_pending, a.reduce_pending,
                   lambda: a.request("leak-a", 0, 2000, deadline_ms=30_000)):
        with pytest.raises(ContractError, match="owner_superseded"):
            action()
    assert h.store.rows("SELECT handle FROM frames WHERE sequence=9") == []
    traces = b.reduce_pending()  # B adopts the result A committed while A was still the owner
    assert [t.outcome for t in traces] == ["current:liquid"] and traces[0].owner_epoch == 1 and h.worker.calls == 1
    assert b._reduce(job, 1) is None and b.reduce_pending() == []  # a second reduction is a no-op, never a rewrite
    assert store_b.row("SELECT status FROM attempts WHERE job_id=?", (job,))["status"] == "valid"
    assert b.request("leak-a", 0, 4000, deadline_ms=30_000) == {"status": "known", "job_id": job}


class OddWorker:
    """A worker whose payload is JSON-serializable but not an object."""
    model_id = "fake-verify-1"

    def __init__(self, payload):
        self.payload, self.calls = payload, 0

    def infer(self, request):
        self.calls += 1
        return InferenceResult(request.job_id, request.attempt, self.model_id, 100, self.payload)


@pytest.mark.parametrize("payload", [[1, 2, 3], None])
def test_non_object_result_is_rejected_and_does_not_block_reduction(harness, payload):  # F2
    h = harness(OddWorker(payload))
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    job = h.core.request("leak-a", 0, 4000, deadline_ms=30_000)["job_id"]
    assert h.core.run_pending() == {"job_id": job, "attempt": 1, "status": "rejected"}
    assert h.core.reduce_pending() == [] and h.core.tick()["reduced"] == []
    assert [t.outcome for t in h.store.traces()] == ["rejected:invalid_result"]
    assert tuple(h.store.row("SELECT status, reduced FROM attempts")) == ("rejected", 1)
    assert h.store.rows("SELECT handle FROM evidence WHERE pinned_by IS NOT NULL") == []
    h.core.worker = FakeWorker(default=(100, LIQUID))  # a later look is not blocked by the rejected result
    assert h.core.request("leak-a", 0, 2000, deadline_ms=30_000)["status"] == "queued"
    h.run_until(4500)
    assert h.store.traces()[-1].outcome == "current:liquid"


def test_expired_estimate_is_demoted_by_an_undecided_look(harness):  # F3
    dark = {"liquid": 0.1, "shadow": 0.1, INSUFFICIENT: 0.8}
    h = harness(FakeWorker({(0, 4000): (100, LIQUID), (9000, 12_000): (100, dark)}), retention=60_000)
    h.offer(*OUTAGE)
    h.run_until(12_000)
    h.core.request("leak-a", 0, 4000, deadline_ms=30_000)
    h.run_until(12_500)
    assert h.store.current_estimate(KEY)["to_ms"] == 4000
    h.run_until(45_000)
    h.core.request("leak-a", 9000, 12_000, deadline_ms=60_000)
    h.run_until(45_500)
    assert h.store.traces()[-1].outcome == f"undecided:{INSUFFICIENT}"
    assert h.store.current_estimate(KEY) is None
    assert [(e["to_ms"], e["reason"], e["demoted_ms"]) for e in h.store.history(KEY)] == [(4000, "expired", 45_500)]


def test_capture_jitter_within_half_a_period_is_one_span(harness):  # F4
    h = harness(FakeWorker(default=(100, LIQUID)))
    h.offer(frame(1, captured=500), frame(2, captured=1001), frame(3, captured=1500), frame(4, captured=2000))
    h.run_until(2000)
    assert h.store.coverage("cam-a", 0, 2000) == [(0, 500, "captured"), (500, 501, "unknown"), (501, 2000, "captured")]
    assert h.core.coverage("cam-a", 0, 2000) == [(0, 2000, "captured")]  # read with the jitter tolerance
    assert h.core.request("leak-a", 0, 2000, deadline_ms=30_000)["status"] == "queued"
    h.run_until(2500)
    assert h.store.coverage("cam-a", 0, 2000) == [(0, 2000, "inspected")]
    assert h.store.questions("not_observed") == []


def test_capture_gap_beyond_half_a_period_stays_unknown(harness):  # F4
    h = harness(FakeWorker(default=(100, LIQUID)))
    h.offer(frame(1, captured=500), frame(2, captured=1300))
    h.run_until(2000)
    assert h.core.request("leak-a", 0, 1300, deadline_ms=30_000) == {
        "status": "refused", "reason": "not_contiguous", "segments": [[0, 500], [800, 1300]]}
    assert h.core.coverage("cam-a", 0, 1300) == [(0, 500, "captured"), (500, 800, "unknown"), (800, 1300, "captured")]
    assert [(q["from_ms"], q["to_ms"]) for q in h.store.questions("not_observed")] == [(500, 800)]


def test_ring_pins_survive_a_rolled_back_release(harness, monkeypatch):  # F5
    h = harness(FakeWorker(default=(100, LIQUID)))
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    h.core.request("leak-a", 0, 4000, deadline_ms=30_000)
    h.core.run_pending()
    h.core.advance(4100)

    def fail(*args, **kwargs):
        raise RuntimeError("trace_failed")

    monkeypatch.setattr(h.core, "_trace", fail)  # fails after _release inside the reduction transaction
    with pytest.raises(RuntimeError):
        h.core.reduce_pending()
    assert h.source.pinned == frozenset(range(1, 9))
    assert len(h.store.rows("SELECT handle FROM evidence WHERE pinned_by IS NOT NULL")) == 8
    monkeypatch.undo()
    assert [t.outcome for t in h.core.reduce_pending()] == ["current:liquid"]
    assert h.source.pinned == frozenset() and h.store.rows("SELECT handle FROM evidence WHERE pinned_by IS NOT NULL") == []
