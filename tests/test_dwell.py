import asyncio

from seer_agent import Event, Mission, Runtime, Store
from seer_agent.builtins import builtin_registry

ZONE = [[0.2, 0.2], [0.8, 0.2], [0.8, 0.8], [0.2, 0.8]]


def mission(dwell_ms=5000):
    return {
        "schema_version": 1, "id": "dwell", "version": 1, "purpose": "Alert once per object that stays in the zone.",
        "sources": {"camera": ["vision.tracks", "coverage"]},
        "observe": [], "remember": {"sources": ["camera"], "max_items": 50, "max_age_ms": 86400000},
        "condition": {"type": "object_dwell", "config": {
            "source": "camera", "kind": "vision.tracks", "labels": ["person", "car"], "zone": ZONE,
            "dwell_ms": dwell_ms}},
        "act": [{"on": "opened", "tool": "record", "arguments": {"text": "$summary"}},
                {"on": "cleared", "tool": "record", "arguments": {"text": "$summary"}}],
        "policy": {"allowed_tools": ["record"], "allow_external": False, "max_pending": 64,
                   "max_concurrency": 4, "timeout_ms": 30000, "max_event_age_ms": 60000},
    }


def frame(at, *objects, session="s1"):
    detections = [{"label": label, "confidence": 0.9, "box": box, "track_id": track} for track, label, box in objects]
    return Event(f"{session}:{at}", "camera", "vision.tracks", at, {"session_id": session, "detections": detections},
                 provenance="synthetic")


INSIDE = [0.4, 0.3, 0.1, 0.3]       # bottom center (0.45, 0.6)
STRADDLE = [0.65, 0.5, 0.2, 0.2]    # crosses the right edge, bottom center (0.75, 0.7) inside
OUTSIDE_FOOT = [0.3, 0.6, 0.2, 0.3]  # overlaps the zone, bottom center (0.4, 0.9) below it


def run(tmp_path, events, dwell_ms=5000):
    clock = [0]
    store = Store(tmp_path/"dwell.sqlite")
    runtime = Runtime(Mission.from_dict(mission(dwell_ms)), builtin_registry(), store, clock=lambda: clock[0])
    transitions = []
    for event in events:
        clock[0] = max(clock[0], event.observed_at_ms)
        result = runtime.ingest(event)
        asyncio.run(runtime.flush())
        if result.get("transition"):
            transitions.append((event.observed_at_ms, result["transition"], result["summary"]))
    store.close()
    return transitions


def visible(track, label, box, start, end, step=1000, session="s1"):
    return [frame(at, (track, label, box), session=session) for at in range(start, end + 1, step)]


def empty(start, end, step=1000, session="s1"):
    return [frame(at, session=session) for at in range(start, end + 1, step)]


def test_hidden_eight_seconds_keeps_one_appearance(tmp_path):
    events = visible("t1", "person", INSIDE, 1000, 7000) + empty(8000, 15000) + visible("t1", "person", INSIDE, 16000, 20000)
    assert [t[:2] for t in run(tmp_path, events)] == [(6000, "opened")]


def test_hidden_twelve_seconds_is_a_new_appearance(tmp_path):
    events = (visible("t1", "person", INSIDE, 1000, 7000) + empty(8000, 19000)
              + visible("t1", "person", INSIDE, 20000, 26000))
    transitions = run(tmp_path, events)
    assert [t[:2] for t in transitions] == [(6000, "opened"), (18000, "cleared"), (25000, "opened")]
    assert transitions[0][2] == "Person stayed in the zone for 5 s."


def test_two_people_in_a_row_alert_separately(tmp_path):
    events = visible("a", "person", INSIDE, 1000, 7000) + visible("b", "person", INSIDE, 8000, 14000)
    assert [t[:2] for t in run(tmp_path, events)] == [(6000, "opened"), (13000, "opened")]


def test_bottom_center_decides_zone_membership(tmp_path):
    events = [frame(at, ("edge", "car", STRADDLE), ("foot", "person", OUTSIDE_FOOT)) for at in range(1000, 8001, 1000)]
    transitions = run(tmp_path, events)
    assert [t[:2] for t in transitions] == [(6000, "opened")]
    assert transitions[0][2] == "Car stayed in the zone for 5 s."


def test_unselected_classes_and_low_confidence_are_ignored(tmp_path):
    events = [frame(at, ("d", "dog", INSIDE), ("p", "person", INSIDE)) for at in range(1000, 9001, 1000)]
    for event in events:
        event.data["detections"][1]["confidence"] = 0.3
    assert run(tmp_path, events) == []


def test_reconnect_restarts_dwell(tmp_path):
    events = (visible("t1", "person", INSIDE, 1000, 4000)
              + [Event("gap", "camera", "coverage", 4500, {"from_ms": 4000, "to_ms": 4500, "observed": False},
                       "unknown", "sensor")]
              + visible("t1", "person", INSIDE, 5000, 12000, session="s2"))
    assert [t[:2] for t in run(tmp_path, events)] == [(10000, "opened")]


def test_appear_alert_uses_zero_dwell(tmp_path):
    transitions = run(tmp_path, visible("t1", "car", INSIDE, 1000, 2000), dwell_ms=0)
    assert transitions == [(1000, "opened", "Car appeared in the zone.")]
