import asyncio
from dataclasses import replace
import json
from pathlib import Path

import pytest

from seer_agent import ContractError, Event, Evaluation, Mission, Runtime, Store, Tool, ToolResult, ToolSpec
from seer_agent.builtins import demo_registry
from seer_agent.contracts import object_schema

EXAMPLES = Path(__file__).parents[1] / "examples"


def scenario(name="person_detection"):
    root = EXAMPLES / name
    return json.loads((root/"mission.json").read_text()), [Event.from_dict(json.loads(line)) for line in (root/"events.jsonl").read_text().splitlines()]


def setup(tmp_path, *, name="person_detection", document=None, registry=None, allow_external=False):
    doc, events = scenario(name)
    doc = document or doc
    clock = [0]
    store = Store(tmp_path/"test.sqlite")
    runtime = Runtime(Mission.from_dict(doc), registry or demo_registry(), store,
                      clock=lambda: clock[0], allow_external=allow_external)
    return runtime, store, clock, events


async def feed(runtime, clock, events):
    for event in events:
        clock[0] = max(clock[0], event.observed_at_ms)
        runtime.ingest(event)
        await runtime.flush()


@pytest.mark.parametrize("name", ["person_detection", "oil_leak"])
def test_complete_workflow_and_restart_keeps_original_receipts(tmp_path, name):
    runtime, store, clock, events = setup(tmp_path, name=name)
    asyncio.run(feed(runtime, clock, events))
    transitions = [r for r in store.history(runtime.key) if r["transition"]]
    assert [r["transition"] for r in transitions] == ["opened", "cleared"]
    assert json.loads(transitions[0]["state"])["last_observed_ms"] == 3000
    assert {"sop-1", "operator-1"}.issubset(json.loads(transitions[0]["evidence"]))
    before = store.operations(runtime.key)
    assert len(before) == 8
    assert len([r for r in before if r["role"] == "act"]) == 2
    store.close()
    store = Store(tmp_path/"test.sqlite")
    reopened = Runtime(runtime.mission, demo_registry(), store, clock=lambda: clock[0])
    asyncio.run(feed(reopened, clock, events))
    assert before == store.operations(runtime.key)
    assert reopened.state["active"] is False


@pytest.mark.parametrize("name,field", [("person_detection", "authorized_visit"), ("oil_leak", "maintenance_active")])
def test_authorized_context_changes_assessment(tmp_path, name, field):
    runtime, store, clock, events = setup(tmp_path, name=name)
    events[1] = replace(events[1], data={**events[1].data, field: True})
    asyncio.run(feed(runtime, clock, events))
    assert [r["transition"] for r in store.history(runtime.key) if r["transition"]] == ["review", "cleared"]


def test_unrelated_future_and_foreign_context_is_excluded(tmp_path):
    runtime, store, clock, events = setup(tmp_path)
    foreign = replace(events[1], id="foreign", source="foreign", data={"area": "gate", "authorized_visit": True})
    assert runtime.ingest(foreign)["status"] == "rejected"
    future = replace(events[1], id="future", observed_at_ms=100000)
    assert runtime.ingest(future)["status"] == "rejected"
    events[1] = replace(events[1], data={"area": "another-area", "authorized_visit": True})
    asyncio.run(feed(runtime, clock, events))
    opening = next(r for r in store.history(runtime.key) if r["transition"] == "opened")
    assert "operator-1" not in json.loads(opening["evidence"])
    assert "foreign" not in {r.event.id for r in runtime.memory().query(limit=1000)}


def test_stale_document_requires_review(tmp_path):
    doc, _ = scenario()
    doc["remember"]["max_age_ms"] = 1000
    runtime, store, clock, events = setup(tmp_path, document=doc)
    asyncio.run(feed(runtime, clock, events))
    opening = next(r for r in store.history(runtime.key) if r["transition"])
    assert opening["transition"] == "review"
    assert "sop-1" not in json.loads(opening["evidence"])


def test_unknown_never_clears_active_incident(tmp_path):
    runtime, store, clock, events = setup(tmp_path)
    asyncio.run(feed(runtime, clock, events[:5]))
    assert runtime.state["active"]
    unknown = replace(events[-1], id="outage", observed_at_ms=4000, quality="unknown")
    asyncio.run(feed(runtime, clock, [unknown]))
    assert runtime.state["active"] and runtime.state["health"] == "unknown"


def test_gap_breaks_continuous_presence(tmp_path):
    runtime, store, clock, events = setup(tmp_path)
    events[3] = replace(events[3], observed_at_ms=4000)
    events[4] = replace(events[4], observed_at_ms=5000)
    asyncio.run(feed(runtime, clock, events[:5]))
    assert not runtime.state.get("active")


def test_oil_reference_change_latches_unknown(tmp_path):
    runtime, store, clock, events = setup(tmp_path, name="oil_leak")
    asyncio.run(feed(runtime, clock, events[:6]))
    assert runtime.state["active"]
    last = replace(events[-1], data={"fixture_result": {"change": "unchanged", "confidence": 1, "reference_id": "new-baseline"}})
    asyncio.run(feed(runtime, clock, [last]))
    assert runtime.state["active"] and runtime.state["reference_invalid"]


def test_conflicting_duplicate_is_rejected_atomically(tmp_path):
    runtime, store, clock, events = setup(tmp_path)
    runtime.ingest(events[0])
    with pytest.raises(ContractError, match="event_identity_conflict"):
        runtime.ingest(replace(events[0], data={"text": "changed"}))
    assert len(store.history(runtime.key)) == 1


def test_backpressure_rolls_back_input_and_state(tmp_path):
    doc, _ = scenario()
    doc["policy"]["max_pending"] = 1
    runtime, store, clock, events = setup(tmp_path, document=doc)
    clock[0] = 2000
    runtime.ingest(events[2])
    with pytest.raises(ContractError, match="backpressure"):
        runtime.ingest(events[3])
    assert store.event(runtime.key, events[3].id) is None
    asyncio.run(runtime.flush())
    runtime.ingest(events[3])


def external_registry(execute):
    registry = demo_registry()
    registry.tool(Tool(ToolSpec("notify", "1", "external", object_schema({"text": {"type": "string"}}), {"type": "object"}), execute))
    return registry


def external_document():
    doc, _ = scenario()
    doc["act"] = [{"on": "opened", "tool": "notify", "arguments": {"text": "$summary"}}]
    doc["policy"]["allowed_tools"].append("notify")
    doc["policy"]["allow_external"] = True
    return doc


def test_external_needs_host_and_mission_authority(tmp_path):
    calls = []
    async def send(context, args):
        calls.append(context.operation_id)
        return ToolResult("accepted", {}, "provider-1")
    runtime, store, clock, events = setup(tmp_path, document=external_document(), registry=external_registry(send))
    asyncio.run(feed(runtime, clock, events))
    assert not calls
    assert next(r for r in store.operations(runtime.key) if r["role"] == "act")["status"] == "denied"


def test_timeout_does_not_resend_external_effect(tmp_path):
    calls = []
    async def send(context, args):
        calls.append(context.operation_id)
        await asyncio.sleep(0.1)
        return ToolResult("accepted", {}, "provider-1")
    doc = external_document()
    doc["policy"]["timeout_ms"] = 10
    runtime, store, clock, events = setup(tmp_path, document=doc, registry=external_registry(send), allow_external=True)
    asyncio.run(feed(runtime, clock, events[:5]))
    assert len(calls) == 1
    assert next(r for r in store.operations(runtime.key) if r["role"] == "act")["status"] == "unknown"
    asyncio.run(runtime.flush())
    assert len(calls) == 1


def test_crash_after_claim_expires_unknown_and_resolves_without_send(tmp_path):
    calls = []
    async def send(context, args):
        calls.append(context.operation_id)
        return ToolResult("accepted", {}, "receipt")
    registry = external_registry(send)
    runtime, store, clock, events = setup(tmp_path, document=external_document(), registry=registry, allow_external=True)
    asyncio.run(feed(runtime, clock, events[:4]))
    clock[0] = 3000
    runtime.ingest(events[4])
    asyncio.run(runtime.drain())
    action = next(r for r in store.operations(runtime.key) if r["role"] == "act")
    assert store.claim(action["id"], clock[0])
    # Simulate an effect applied followed by process death before finish().
    calls.append(action["id"])
    store.close()
    store = Store(tmp_path/"test.sqlite")
    clock[0] = action["deadline"]+1
    recovered = Runtime(runtime.mission, registry, store, clock=lambda: clock[0], allow_external=True)
    asyncio.run(recovered.flush())
    assert len(calls) == 1
    assert next(r for r in store.operations(runtime.key) if r["role"] == "act")["status"] == "unknown"
    store.resolve(runtime.key, action["id"], "succeeded", "verified-provider-receipt", clock[0])
    assert len(calls) == 1


def test_two_connections_claim_only_once(tmp_path):
    calls = []
    async def send(context, args):
        calls.append(context.operation_id)
        await asyncio.sleep(0.01)
        return ToolResult("accepted", {}, "receipt")
    registry = external_registry(send)
    runtime, store, clock, events = setup(tmp_path, document=external_document(), registry=registry, allow_external=True)
    asyncio.run(feed(runtime, clock, events[:4]))
    clock[0] = 3000
    runtime.ingest(events[4])
    asyncio.run(runtime.drain())
    other = Store(tmp_path/"test.sqlite")
    second = Runtime(runtime.mission, registry, other, clock=lambda: clock[0], allow_external=True)
    async def both():
        await asyncio.gather(runtime.flush(), second.flush())
    asyncio.run(both())
    assert len(calls) == 1
    other.close()


def test_definition_change_requires_new_version(tmp_path):
    runtime, store, clock, events = setup(tmp_path)
    document = json.loads(json.dumps(runtime.doc))
    document["purpose"] = "Changed purpose"
    with pytest.raises(ContractError, match="without_new_version"):
        Runtime(Mission.from_dict(document), demo_registry(), store)
    registry = demo_registry()
    old = registry.tools["fixture.detect"]
    registry.tools["fixture.detect"] = Tool(replace(old.spec, version="2"), old.execute)
    with pytest.raises(ContractError, match="without_new_version"):
        Runtime(runtime.mission, registry, store)


def test_custom_condition_and_tool_without_runtime_changes(tmp_path):
    registry = demo_registry()
    class Temperature:
        version = "1"
        def __init__(self, config): pass
        def evaluate(self, event, state, memory):
            return Evaluation({"hot": True}, "opened", "High temperature", (event.id,))
    registry.condition("temperature", Temperature)
    doc, events = scenario()
    doc["condition"] = {"type": "temperature", "config": {}}
    doc["observe"] = []
    runtime, store, clock, _ = setup(tmp_path, document=doc, registry=registry)
    runtime.ingest(events[0])
    asyncio.run(runtime.flush())
    assert runtime.state == {"hot": True}
    assert store.operations(runtime.key)[0]["status"] == "succeeded"


def test_invalid_model_output_is_unknown_evidence(tmp_path):
    registry = demo_registry()
    old = registry.tools["fixture.detect"]
    async def invalid(context, args): return ToolResult("succeeded", {"detections": "bad"})
    registry.tools["fixture.detect"] = Tool(old.spec, invalid)
    runtime, store, clock, events = setup(tmp_path, registry=registry)
    asyncio.run(feed(runtime, clock, events[:3]))
    assert runtime.state["health"] == "unknown"
    assert not runtime.state.get("active")


def test_crash_after_result_before_derived_event_is_recoverable(tmp_path):
    runtime, store, clock, events = setup(tmp_path)
    clock[0] = 1000
    runtime.ingest(events[2])
    row = store.operations(runtime.key)[0]
    asyncio.run(runtime._execute(row))
    assert not store.operations(runtime.key)[0]["applied"]
    reopened = Runtime(runtime.mission, demo_registry(), store, clock=lambda: clock[0])
    asyncio.run(reopened.flush())
    assert reopened.state["positive_since"] == 1000
    assert store.operations(runtime.key)[0]["applied"]


def test_tool_result_cannot_recursively_trigger_observer(tmp_path):
    doc, _ = scenario()
    doc["observe"][0]["emits"] = "camera.frame"
    runtime, store, clock, events = setup(tmp_path, document=doc)
    asyncio.run(feed(runtime, clock, events[:3]))
    assert len(store.operations(runtime.key)) == 1


def test_pending_cancel_prevents_effect(tmp_path):
    runtime, store, clock, events = setup(tmp_path)
    clock[0] = 1000
    runtime.ingest(events[2])
    row = store.operations(runtime.key)[0]
    runtime.cancel(row["id"])
    asyncio.run(runtime.flush())
    assert store.operations(runtime.key)[0]["status"] == "cancelled"
    assert runtime.state["health"] == "unknown"


def test_batch_ingestion_preserves_observation_order(tmp_path):
    runtime, store, clock, events = setup(tmp_path)
    clock[0] = 6000
    for event in events:
        runtime.ingest(event)
    asyncio.run(runtime.flush())
    assert [r["transition"] for r in store.history(runtime.key) if r["transition"]] == ["opened", "cleared"]


def test_policy_mutation_requires_new_version(tmp_path):
    runtime, store, clock, events = setup(tmp_path)
    runtime.doc["policy"]["allow_external"] = True
    with pytest.raises(ContractError, match="mission_mutated"):
        runtime.ingest(events[0])


def test_late_or_malformed_oil_reference_cannot_poison_baseline(tmp_path):
    runtime, store, clock, events = setup(tmp_path, name="oil_leak")
    asyncio.run(feed(runtime, clock, events[:6]))
    before = runtime.state
    late = replace(events[3], id="late-baseline", data={"fixture_result": {"change": "changed", "confidence": 1, "reference_id": "old-baseline"}})
    asyncio.run(feed(runtime, clock, [late]))
    assert runtime.state == before
    malformed = replace(events[6], id="malformed-baseline", data={"fixture_result": {"change": "changed", "confidence": 1, "reference_id": "bad reference"}})
    asyncio.run(feed(runtime, clock, [malformed]))
    assert not runtime.state.get("reference_invalid")
    asyncio.run(feed(runtime, clock, events[-2:]))
    assert runtime.state["active"] is False


def test_unknown_context_does_not_resurrect_older_sensor_reading(tmp_path):
    runtime, store, clock, events = setup(tmp_path, name="oil_leak")
    unknown = replace(events[2], id="sensor-outage", observed_at_ms=30,
                      quality="unknown", data={"area": "pump", "reason": "disconnected"})
    asyncio.run(feed(runtime, clock, events[:3]+[unknown]+events[3:]))
    opening = next(r for r in store.history(runtime.key) if r["transition"])
    assert opening["transition"] == "review"
    assert "sensor-1" not in json.loads(opening["evidence"])


def test_derived_fanout_waits_for_capacity_without_deadlock(tmp_path):
    doc, events = scenario()
    doc["condition"]["config"]["hold_ms"] = 0
    doc["policy"].update(max_pending=2, max_concurrency=1)
    doc["act"] = [{"on": "review", "tool": "record", "arguments": {"text": "$summary"}}]*2
    runtime, store, clock, _ = setup(tmp_path, document=doc)
    clock[0] = 2000
    runtime.ingest(events[2])
    runtime.ingest(events[3])
    asyncio.run(asyncio.wait_for(runtime.flush(), 1))
    actions = [r for r in store.operations(runtime.key) if r["role"] == "act"]
    assert len(actions) == 2 and all(a["status"] == "succeeded" for a in actions)


def test_concurrency_budget_is_shared_between_connections(tmp_path):
    registry = demo_registry()
    entered, release, calls = asyncio.Event(), asyncio.Event(), []
    async def slow(context, args):
        calls.append(context.operation_id)
        entered.set()
        await release.wait()
        return ToolResult("succeeded", {"detections": []})
    original = registry.tools["fixture.detect"]
    registry.tools["fixture.detect"] = Tool(original.spec, slow)
    doc, events = scenario()
    doc["policy"]["max_concurrency"] = 1
    runtime, store, clock, _ = setup(tmp_path, document=doc, registry=registry)
    clock[0] = 2000
    runtime.ingest(events[2]); runtime.ingest(events[3])
    other = Store(tmp_path/"test.sqlite")
    second = Runtime(runtime.mission, registry, other, clock=lambda: clock[0])
    async def run():
        first = asyncio.create_task(runtime.drain())
        await entered.wait()
        assert await second.drain() == 0
        assert len(calls) == 1
        release.set()
        await first
        await second.flush()
    asyncio.run(asyncio.wait_for(run(), 1))
    assert len(calls) == 2


def test_camera_traffic_does_not_evict_other_source_context(tmp_path):
    runtime, store, clock, events = setup(tmp_path)
    asyncio.run(feed(runtime, clock, events[:2]))
    many = [replace(events[2], id=f"frame-{i}", observed_at_ms=i*1000) for i in range(1, 101)]
    asyncio.run(feed(runtime, clock, many))
    ids = {r.event.id for r in runtime.memory().query(limit=1000)}
    assert {"sop-1", "operator-1"}.issubset(ids)
    assert len(ids) <= runtime.doc["remember"]["max_items"]


def test_action_result_can_drive_bounded_fallback_workflow(tmp_path):
    registry = demo_registry()
    class Fallback:
        version = "1"
        def __init__(self, config): pass
        def evaluate(self, event, state, memory):
            if event.kind == "message":
                return Evaluation(state, "notify", "Request review", (event.id,))
            if event.kind == "notification.result" and event.data["status"] == "unknown":
                return Evaluation({"fallback": True}, "fallback", "Primary channel uncertain; record for review", (event.id,))
            return Evaluation(state)
    async def uncertain(context, args):
        return ToolResult("unknown", {"reason": "provider_timeout"})
    registry.condition("fallback", Fallback)
    registry.tool(Tool(ToolSpec("notify", "1", "local", {"type": "object"}, {"type": "object"}), uncertain))
    doc, events = scenario()
    doc["condition"] = {"type": "fallback", "config": {}}
    doc["observe"] = []
    doc["policy"]["allowed_tools"].append("notify")
    doc["act"] = [{"on": "notify", "tool": "notify", "arguments": {}, "emits": "notification.result"},
                  {"on": "fallback", "tool": "record", "arguments": {"text": "$summary"}}]
    runtime, store, clock, _ = setup(tmp_path, document=doc, registry=registry)
    asyncio.run(feed(runtime, clock, events[:2]))
    assert runtime.state == {"fallback": True}
    assert [r["status"] for r in store.operations(runtime.key)] == ["unknown", "succeeded"]


def test_cyclic_result_workflow_stops_at_depth_budget(tmp_path):
    registry = demo_registry()
    class Loop:
        version = "1"
        def __init__(self, config): pass
        def evaluate(self, event, state, memory):
            return Evaluation(state, "again", "Record", (event.id,))
    registry.condition("loop", Loop)
    doc, events = scenario()
    doc["condition"] = {"type": "loop", "config": {}}
    doc["observe"] = []
    doc["policy"]["max_tool_depth"] = 3
    doc["act"] = [{"on": "again", "tool": "record", "arguments": {"text": "$summary"}, "emits": "action.result"}]
    runtime, store, clock, _ = setup(tmp_path, document=doc, registry=registry)
    asyncio.run(feed(runtime, clock, events[:1]))
    operations = store.operations(runtime.key)
    assert len(operations) == 4
    assert operations[-1]["status"] == "denied" and operations[-1]["emits"] is None


@pytest.mark.parametrize("concurrent", [False, True])
def test_reconciled_outcome_emits_new_immutable_evidence(tmp_path, concurrent):
    registry = demo_registry()
    class TrackOutcome:
        version = "1"
        def __init__(self, config): pass
        def evaluate(self, event, state, memory):
            if event.kind == "message":
                return Evaluation(state, "notify", "Review", (event.id,))
            if event.kind == "notification.result":
                return Evaluation({"outcome": event.data["status"]})
            return Evaluation(state)
    async def accepted(context, args): return ToolResult("accepted", {}, "submitted-1")
    registry.condition("track", TrackOutcome)
    registry.tool(Tool(ToolSpec("notify", "1", "local", {"type": "object"}, {"type": "object"}), accepted))
    doc, events = scenario()
    doc["condition"] = {"type": "track", "config": {}}
    doc["observe"] = []
    doc["policy"]["allowed_tools"].append("notify")
    doc["act"] = [{"on": "notify", "tool": "notify", "arguments": {}, "emits": "notification.result"}]
    runtime, store, clock, _ = setup(tmp_path, document=doc, registry=registry)
    other = Store(tmp_path/"test.sqlite")
    if concurrent:
        original_ingest = runtime.ingest
        def ingest_then_resolve(event, **kwargs):
            result = original_ingest(event, **kwargs)
            if event.kind == "notification.result" and event.data["status"] == "accepted":
                other.resolve(runtime.key, event.data["operation_id"], "succeeded", "verified-delivery", clock[0])
            return result
        runtime.ingest = ingest_then_resolve
    asyncio.run(feed(runtime, clock, events[:2]))
    action = store.operations(runtime.key)[0]
    if not concurrent:
        assert runtime.state["outcome"] == "accepted"
        other.resolve(runtime.key, action["id"], "succeeded", "verified-delivery", clock[0])
    asyncio.run(runtime.flush())
    assert runtime.state["outcome"] == "succeeded"
    prior = store.event(runtime.key, "tool:"+action["id"])
    latest = store.event(runtime.key, "tool:"+action["id"]+":2")
    assert json.loads(prior["body"])["data"]["status"] == "accepted"
    assert json.loads(latest["body"])["data"]["status"] == "succeeded"
    other.close()


def test_resolution_keeps_original_receipt_without_outcome_emission(tmp_path):
    runtime, store, clock, events = setup(tmp_path)
    asyncio.run(feed(runtime, clock, events[:4]))
    clock[0] = events[4].observed_at_ms
    runtime.ingest(events[4])
    asyncio.run(runtime.drain())
    action = next(row for row in store.operations(runtime.key) if row["role"] == "act")
    assert action["status"] == "pending" and action["emits"] is None
    assert store.claim(action["id"], clock[0])
    assert store.finish(action["id"], "accepted", {}, "provider-message-123", clock[0])
    store.resolve(runtime.key, action["id"], "succeeded", "operator-verification-456", clock[0])
    history = [json.loads(row[0]) for row in store.db.execute(
        "SELECT result FROM operation_history WHERE operation=? ORDER BY revision", (action["id"],))]
    assert any(row.get("receipt") == "provider-message-123" for row in history)
    assert any(row.get("verified_receipt") == "operator-verification-456" for row in history)


def test_large_model_result_becomes_bounded_unknown_not_stuck(tmp_path):
    registry = demo_registry()
    original = registry.tools["fixture.detect"]
    async def huge(context, args): return ToolResult("succeeded", {"value": "x"*262050})
    registry.tools["fixture.detect"] = Tool(replace(original.spec, output_schema={"type": "object"}), huge)
    runtime, store, clock, events = setup(tmp_path, registry=registry)
    asyncio.run(feed(runtime, clock, events[:3]))
    operation = store.operations(runtime.key)[0]
    assert operation["status"] == "unknown" and operation["applied"]
    assert runtime.state["health"] == "unknown"


def test_new_camera_work_cannot_starve_older_action(tmp_path):
    doc, events = scenario()
    doc["condition"]["config"]["hold_ms"] = 0
    doc["policy"]["max_concurrency"] = 1
    runtime, store, clock, _ = setup(tmp_path, document=doc)
    asyncio.run(feed(runtime, clock, events[:2]))
    clock[0] = 1000
    runtime.ingest(events[2])
    asyncio.run(runtime.drain())
    action = next(r for r in store.operations(runtime.key) if r["role"] == "act")
    assert action["status"] == "pending"
    clock[0] = 2000
    runtime.ingest(events[3])
    asyncio.run(runtime.drain())
    action = next(r for r in store.operations(runtime.key) if r["role"] == "act")
    assert action["status"] == "succeeded"


def test_late_receipt_after_expiry_is_durable_without_redispatch(tmp_path):
    runtime, store, clock, events = setup(tmp_path)
    clock[0] = 1000
    runtime.ingest(events[2])
    operation = store.operations(runtime.key)[0]
    assert store.claim(operation["id"], clock[0])
    clock[0] = operation["deadline"]+1
    store.expire(runtime.key, clock[0])
    asyncio.run(runtime.flush())
    assert store.finish(operation["id"], "succeeded", {"detections": []}, "late-model-receipt", clock[0])
    asyncio.run(runtime.flush())
    saved = store.operations(runtime.key)[0]
    assert saved["receipt"] == "late-model-receipt" and saved["outcome_revision"] == 2
    assert store.event(runtime.key, "tool:"+operation["id"]) is not None
    assert store.event(runtime.key, "tool:"+operation["id"]+":2") is not None
