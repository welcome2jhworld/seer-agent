"""The four Slice 1 scenarios plus the restart retry rule of the job ledger."""
import pytest

from seer_agent import ContractError
from seer_agent.v2 import FakeWorker
from v2_harness import LIQUID, SHADOW, WATCH, frame

KEY = WATCH.merge_key


def test_duplicate_and_out_of_order_frames_produce_one_observation_each(harness):
    h = harness(FakeWorker(default=(1000, SHADOW)))
    # Frames 4 and 1 are sent twice; frame 5 (captured at 2500) is published late, at 4000.
    for seq in (1, 3, 2, 4, 4, 1, 6, 5):
        h.source.offer(frame(seq, published=4000 if seq == 5 else None))
    assert h.source.duplicates == 2
    h.run_until(3000)
    assert h.store.coverage("cam-a", 0, 3000) == [(0, 2000, "captured"), (2000, 2500, "unknown"), (2500, 3000, "captured")]
    assert [f["sequence"] for f in h.store.frames("cam-a", 0, 3000)] == [1, 2, 3, 4, 6]
    h.run_until(4000)
    assert h.store.coverage("cam-a", 0, 3000) == [(0, 3000, "captured")]
    assert [f["sequence"] for f in h.store.frames("cam-a", 0, 3000)] == [1, 2, 3, 4, 5, 6]
    assert h.core.ingest(frame(2)) == "duplicate"
    with pytest.raises(ContractError, match="frame_identity_conflict"):
        h.core.ingest(frame(2, captured=1001))
    first = h.core.request("leak-a", 0, 3000, deadline_ms=20_000)
    assert first["status"] == "queued" and first["handles"] == 6
    assert h.core.request("leak-a", 0, 3000, deadline_ms=20_000) == {"status": "known", "job_id": first["job_id"]}
    h.run_until(6000)
    observations = h.store.observations(KEY)
    assert len(observations) == 1 and observations[0]["picked"] == "shadow" and observations[0]["repeat_of"] is None
    assert (observations[0]["from_ms"], observations[0]["to_ms"]) == (0, 3000)
    assert h.store.coverage("cam-a", 0, 3000) == [(0, 3000, "inspected")]
    # A different job over the same frames is not a new observation: support does not grow.
    crop = h.core.request("leak-a", 0, 3000, kind="crop", deadline_ms=20_000)
    assert crop["status"] == "queued" and crop["job_id"] != first["job_id"]
    h.run_until(8000)
    observations = h.store.observations(KEY)
    assert len(observations) == 2 and observations[1]["repeat_of"] == observations[0]["id"]
    assert h.store.alternatives(KEY)["shadow"]["supporting"] == [observations[0]["id"]]
    assert [t.outcome for t in h.store.traces()] == ["current:shadow", "repeat"]
    assert h.worker.calls == 2


def test_outage_gap_stays_unknown_and_gets_no_estimate(harness):
    h = harness(FakeWorker(default=(500, SHADOW)))
    h.offer(*[frame(s) for s in (*range(1, 9), *range(19, 25))])  # 500..4000, nothing, 9500..12000
    h.run_until(12_000)
    assert h.store.coverage("cam-a", 0, 12_000) == [(0, 4000, "captured"), (4000, 9000, "unknown"), (9000, 12_000, "captured")]
    assert h.core.request("leak-a", 4500, 8500, deadline_ms=30_000) == {"status": "refused", "reason": "not_observed"}
    assert [(q["from_ms"], q["to_ms"], q["status"]) for q in h.store.questions("not_observed")] == [(4500, 8500, "open")]
    assert h.core.request("leak-a", 9000, 12_000, deadline_ms=30_000)["status"] == "queued"
    h.run_until(13_000)
    assert h.store.coverage("cam-a", 0, 12_000) == [(0, 4000, "captured"), (4000, 9000, "unknown"), (9000, 12_000, "inspected")]
    current = h.store.current_estimate(KEY)
    assert (current["hypothesis_id"], current["from_ms"], current["to_ms"]) == ("shadow", 9000, 12_000)
    assert all(e["to_ms"] <= 4000 or e["from_ms"] >= 9000 for e in h.store.rows("SELECT from_ms, to_ms FROM estimates"))
    assert [r["from_ms"] for r in h.store.search("cam-a", "liquid_on_floor", 12_000)] == [9000]


def test_restart_replays_a_stored_result_without_rerunning_inference(harness):
    h = harness(FakeWorker(default=(1000, LIQUID)))
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    assert h.core.request("leak-a", 0, 4000, deadline_ms=30_000)["status"] == "queued"
    stored = h.core.run_pending()
    assert stored["status"] == "stored" and stored["available_ms"] == 5000 and h.worker.calls == 1
    assert [tuple(a) for a in h.store.rows("SELECT status, reduced FROM attempts")] == [("stored", 0)]
    assert h.store.traces() == [] and h.store.current_estimate(KEY) is None
    core = h.restart(5000)  # the process stopped after the result was stored and before reduction
    traces = core.reduce_pending()
    assert h.worker.calls == 1
    assert len(traces) == 1 and traces[0].outcome == "current:liquid"
    assert (traces[0].sequence_from, traces[0].sequence_to, traces[0].model_id) == (1, 8, "fake-verify-1")
    assert (traces[0].queue_ms, traces[0].inference_ms, traces[0].watch_revision, traces[0].owner_epoch) == (0, 1000, 1, 1)
    assert core.reduce_pending() == [] and core.tick()["reduced"] == []
    assert len(h.store.observations(KEY)) == 1 and h.store.current_estimate(KEY)["hypothesis_id"] == "liquid"
    assert h.store.coverage("cam-a", 0, 4000) == [(0, 4000, "inspected")]
    assert h.store.rows("SELECT handle FROM evidence WHERE pinned_by IS NOT NULL") == []
    assert core.context_version == 1 and h.worker.calls == 1


def test_restart_marks_a_running_attempt_unknown_and_retries_once_within_deadline(harness):
    h = harness(FakeWorker(default=(1000, LIQUID)))
    h.offer(*[frame(s) for s in range(1, 9)])
    h.run_until(4000)
    job = h.core.request("leak-a", 0, 4000, deadline_ms=30_000)["job_id"]
    h.core.run_pending()
    # Simulate a crash during inference: the attempt is still running and nothing was stored.
    h.store.db.execute("UPDATE attempts SET status='running', raw_result=NULL, available_ms=NULL WHERE job_id=?", (job,))
    h.store.db.execute("UPDATE jobs SET status='running' WHERE id=?", (job,))
    core = h.restart(4500)
    assert [tuple(a) for a in h.store.rows("SELECT attempt, status, reduce_outcome FROM attempts")] == [(1, "unknown", "owner_restarted")]
    assert core.run_pending()["attempt"] == 2 and h.worker.calls == 2
    h.run_until(6000)
    assert [a["status"] for a in h.store.rows("SELECT status FROM attempts ORDER BY attempt")] == ["unknown", "valid"]
    assert [(t.attempt, t.outcome) for t in h.store.traces()] == [(2, "current:liquid")]


def test_backlog_late_result_for_older_interval_becomes_history(harness):
    h = harness(FakeWorker({(0, 4000): (3000, SHADOW), (9000, 12_000): (500, LIQUID)}), retention=20_000)
    h.offer(*[frame(s) for s in (*range(1, 9), *range(19, 25))])
    h.run_until(12_000)
    older = h.core.request("leak-a", 0, 4000, deadline_ms=40_000, priority="discriminating_check")
    newer = h.core.request("leak-a", 9000, 12_000, deadline_ms=20_000, priority="reserved_coverage")
    assert older["status"] == newer["status"] == "queued"
    h.run_until(12_500)  # the reserved look runs first and is reduced as soon as its result is available
    current = h.store.current_estimate(KEY)
    assert (current["hypothesis_id"], current["from_ms"], current["to_ms"]) == ("liquid", 9000, 12_000)
    assert h.store.history(KEY) == []
    h.run_until(16_000)  # the older look finishes later and must not overwrite the newer estimate
    assert h.store.current_estimate(KEY)["id"] == current["id"]
    history = h.store.history(KEY)
    assert [(e["hypothesis_id"], e["reason"], e["from_ms"], e["to_ms"]) for e in history] == [("shadow", "older_than_current", 0, 4000)]
    assert [t.outcome for t in h.store.traces()] == ["current:liquid", "history:older_than_current:shadow"]
    assert [r["from_ms"] for r in h.store.search("cam-a", "liquid_on_floor", 16_000)] == [9000, 0]
    assert h.store.checks() == []  # a late review is history and does not reopen the decision
    alternatives = h.store.alternatives(KEY)
    assert len(alternatives["liquid"]["supporting"]) == 1 and len(alternatives["shadow"]["supporting"]) == 1
