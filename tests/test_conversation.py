import asyncio
import json

import pytest

from seer_agent import ContractError, Event, Mission, Runtime, Store
from seer_agent.builtins import builtin_registry
from seer_agent.contracts import object_schema
from seer_agent.conversation import (ASK_WINDOW_MS, CONTRACTS, SCHEMAS, analysis_offer, answer_ask, ask_scope,
                                     make_plan, run_analysis, search_coverage)
from seer_agent.host import Host

CAMERAS = [{"id": "cam-main", "name": "Main gate"}, {"id": "cam-side", "name": "Side gate"}]
GATE = {"name": "Gate area", "polygon": [[0.37, 0.46], [0.77, 0.46], [0.77, 0.9], [0.37, 0.9]]}
NOW = 1_800_000_000_000


def plan_request(**body):
    return {"contract": "seer.plan_request", "version": 1, "conversation": "t1", "tier": "free", "cameras": CAMERAS, **body}


def example(**extra):
    return {"kind": "time_in_area", "labels": ["person"], "camera": "cam-main", "zone": GATE, "dwell_s": 10, **extra}


def run(coroutine):
    return asyncio.run(coroutine)


# Task plan

def test_free_example_builds_the_plan_card_and_a_mission_that_alerts(tmp_path):
    card = run(make_plan(plan_request(example=example())))
    assert card["status"] == "ready" and card["revision"] == 1 and card["local_only"] is True
    assert card["goal"] == "Alert when a person stays in Gate area for 10 seconds or more."
    assert [t["id"] for t in card["tools"]] == ["person_detection", "time_in_area"]
    assert card["input"] == {"camera": "cam-main", "camera_name": "Main gate", "zone": GATE, "hours": {"all_day": True}}
    assert card["question"]["id"] == "response" and [r["label"] for r in card["question"]["replies"]] == ["Alert me", "Only record"]
    store = Store(tmp_path/"plan.sqlite")
    runtime = Runtime(Mission.from_dict(card["mission"]), builtin_registry(), store, clock=lambda: NOW)
    inside = {"label": "person", "confidence": 0.9, "box": [0.5, 0.5, 0.1, 0.3], "track_id": "p1"}
    transitions = []
    for at in range(0, 12000, 1000):
        event = Event(f"s1:{at}", "cam-main", "vision.tracks", NOW - 60000 + at,
                      {"session_id": "s1", "detections": [inside]}, provenance="synthetic")
        transitions.append(runtime.ingest(event).get("transition"))
    assert transitions.count("opened") == 1 and transitions.index("opened") == 10
    store.close()


def test_a_quick_reply_answers_the_one_question_without_a_model():
    first = run(make_plan(plan_request(example=example())))
    second = run(make_plan(plan_request(previous=first, reply={"question": "response", "choice": "record"})))
    assert second["revision"] == 2 and second["response"] == "record" and second["question"] is None
    assert second["mission"]["version"] == 2 and second["goal"] == first["goal"]


def test_free_words_need_seer_cloud_and_offer_examples_from_the_users_cameras():
    card = run(make_plan(plan_request(text="Tell me if someone stays at the main gate for a while.")))
    assert card["status"] == "needs_cloud" and card["notice"]["code"] == "needs_cloud"
    assert [e["camera"] for e in card["examples"]] == ["cam-main", "cam-side", "cam-main"]
    assert "mission" not in card


def cloud_candidate(**change):
    candidate = {"status": "ready", "title": "Person at gate",
                 "goal": "Alert when a person stays at the main gate for 10 seconds or more.",
                 "input": {"camera": "cam-main", "camera_name": "Main gate", "zone": GATE, "hours": {"all_day": True}},
                 "tools": [{"id": "person_detection"}, {"id": "time_in_area"}, {"id": "clip_review", "label": "Review"}],
                 "reasoning": "\"A while\" means stopping, not passing through. Bikes and cars are ignored.",
                 "question": None}
    return {**candidate, **change}


def cloud_plan(candidate):
    async def planner(payload):
        assert payload["request"]["text"] and payload["revision"] == 1
        return candidate
    return run(make_plan(plan_request(tier="cloud", text="Tell me if someone stays at the main gate."), planner))


def test_a_cloud_plan_uses_catalog_labels_and_reports_cloud_tools():
    card = cloud_plan(cloud_candidate())
    assert card["status"] == "ready" and card["local_only"] is False and card["response"] == "alert"
    assert [t["label"] for t in card["tools"]] == ["Person detection", "Time in area", "Clip review"]


@pytest.mark.parametrize("change", [
    {"goal": "Alert on a person. Then call me."},
    {"reasoning": "One. Two. Three."},
    {"input": {"camera": "cam-nowhere", "camera_name": "Main gate", "hours": {"all_day": True}}},
    {"input": {"camera": "cam-main", "camera_name": "Wrong name", "hours": {"all_day": True}}},
    {"tools": [{"id": "face_recognition"}]},
    {"tools": [{"id": "time_in_area"}, {"id": "time_in_area"}]},
    {"extra": "field"},
])
def test_a_card_that_breaks_a_rule_becomes_a_plan_failure(change):
    card = cloud_plan(cloud_candidate(**change))
    assert card["status"] == "failed" and card["failure"]["code"] == "plan_failed"
    assert "goal" not in card


def test_an_unsupported_condition_gets_the_fixed_reason_and_an_observable_alternative():
    card = cloud_plan({"status": "unsupported", "suggestion": "I can alert when a person stays at the gate for 10 seconds instead.",
                       "example": example()})
    assert card["status"] == "unsupported" and card["alternative"]["reason"]["code"] == "cannot_watch_reliably"
    assert card["alternative"]["example"]["camera"] == "cam-main"


def test_a_plan_request_asks_for_exactly_one_thing():
    with pytest.raises(ContractError):
        run(make_plan(plan_request()))
    with pytest.raises(ContractError):
        run(make_plan(plan_request(text="x", example=example())))
    with pytest.raises(ContractError):
        run(make_plan(plan_request(example=example(camera="cam-nowhere"))))


# Ask on Live

def ask_request(**body):
    return {"contract": "seer.ask_request", "version": 1, "conversation": "a1", "now_ms": NOW,
            "text": "Is anyone near the side gate?", "cameras": CAMERAS, **body}


def clip(start_min, length_min, camera="cam-main"):
    start = NOW - start_min * 60000
    return {"kind": "clip", "camera": camera, "from_ms": start, "to_ms": start + length_min * 60000}


def test_ask_looks_at_the_last_ten_minutes_or_what_was_attached():
    scope, early = ask_scope(ask_request())
    assert early is None and [(r["camera"], r["to_ms"] - r["from_ms"]) for r in scope] == [("cam-main", ASK_WINDOW_MS), ("cam-side", ASK_WINDOW_MS)]
    scope, _ = ask_scope(ask_request(attachments=[{"kind": "camera", "camera": "cam-side"}]))
    assert [r["camera"] for r in scope] == ["cam-side"]
    attached = clip(16, 10)
    scope, _ = ask_scope(ask_request(attachments=[attached, {"kind": "photo", "id": "photo-1"}]))
    assert scope == [{"camera": "cam-main", "from_ms": attached["from_ms"], "to_ms": attached["to_ms"]}]


def test_a_clip_longer_than_the_window_goes_to_analysis_without_a_model_call():
    async def model(payload):
        raise AssertionError("not called")
    answer = run(answer_ask(ask_request(attachments=[clip(40, 30)]), model))
    assert answer["status"] == "handoff" and answer["handoff"]["reason"]["code"] == "clip_longer_than_ask_window"


def moment(minutes_ago, camera="cam-main", text="Waits at the gate"):
    at = NOW - minutes_ago * 60000
    return {"at_ms": at, "camera": camera, "frame": {"camera": camera, "at_ms": at}, "text": text}


def test_an_answer_reports_progress_moments_in_order_and_what_it_watched():
    attached = clip(16, 10)
    progress = []

    async def model(payload):
        assert payload["scope"][0]["from_ms"] == attached["from_ms"]
        return {"status": "answered", "text": "Yes. The person on the left waits, then walks in.",
                "moments": [moment(8, text="Walks inside"), moment(12, text="Arrives with a bike")],
                "follow_ups": ["Where did they go next?"]}
    answer = run(answer_ask(ask_request(text="Is this person in the clip?", attachments=[attached]), model, progress.append))
    assert progress and progress[0]["kind"] == "progress" and progress[0]["done_ms"] == 0 and progress[0]["total_ms"] == 600000
    assert answer["status"] == "answered" and [m["text"] for m in answer["moments"]] == ["Arrives with a bike", "Walks inside"]
    assert answer["coverage"]["watched"] == [{"camera": "cam-main", "from_ms": attached["from_ms"], "to_ms": attached["to_ms"]}]


def test_a_moment_outside_what_ask_watched_is_refused():
    async def model(payload):
        return {"status": "answered", "text": "Someone came an hour ago.", "moments": [moment(60)]}
    answer = run(answer_ask(ask_request(), model))
    assert answer["status"] == "failed" and answer["failure"]["code"] == "ask_failed" and "moments" not in answer


def test_the_model_can_hand_an_older_question_to_analysis():
    async def model(payload):
        return {"status": "handoff", "query": "Who came to the main gate this morning?"}
    answer = run(answer_ask(ask_request(text="Who came this morning?"), model))
    assert answer["status"] == "handoff" and answer["handoff"]["to"] == "analysis"
    assert answer["handoff"]["reason"]["code"] == "older_than_ask_window"


def test_ask_without_a_cloud_model_says_it_needs_seer_cloud():
    answer = run(answer_ask(ask_request()))
    assert answer["status"] == "needs_cloud" and answer["notice"]["code"] == "needs_cloud"


# Analysis

HOUR = 3600000


def analysis_request(**body):
    return {"contract": "seer.analysis_request", "version": 1, "conversation": "n1", "now_ms": NOW,
            "text": "When did a truck arrive at the main gate today?", "cameras": CAMERAS,
            "scope": {"cameras": ["cam-main", "cam-side"], "from_ms": NOW - 10 * HOUR, "to_ms": NOW},
            "index": [{"camera": "cam-main", "indexed_from_ms": NOW - 4 * HOUR}, {"camera": "cam-side", "indexed_from_ms": None}],
            **body}


def test_the_first_search_offers_the_last_24_hours_with_a_credit_estimate():
    offer = analysis_offer(analysis_request(credits_per_camera_hour=5))
    assert offer["status"] == "offer_indexing" and offer["offer"]["cameras"] == ["cam-side"]
    assert offer["offer"]["credits"] == 120 and offer["offer"]["to_ms"] - offer["offer"]["from_ms"] == 24 * HOUR
    assert analysis_offer(analysis_request())["offer"]["credits"] is None
    started = analysis_offer(analysis_request(choice="last_24_hours"))
    assert started["status"] == "indexing" and started["indexing"]["cameras"] == ["cam-side"]


def test_coverage_tells_searched_from_not_indexed():
    coverage = search_coverage(analysis_request(choice="from_now"))
    assert coverage["searched"] == [{"camera": "cam-main", "from_ms": NOW - 4 * HOUR, "to_ms": NOW}]
    assert coverage["not_indexed"] == [{"camera": "cam-main", "from_ms": NOW - 10 * HOUR, "to_ms": NOW - 4 * HOUR},
                                       {"camera": "cam-side", "from_ms": NOW - 10 * HOUR, "to_ms": NOW}]


def found(hours_ago, label="Truck at the gate"):
    at = NOW - int(hours_ago * HOUR)
    return {"id": f"clip-{at}", "camera": "cam-main", "at_ms": at, "length_ms": 31000,
            "frame": {"camera": "cam-main", "at_ms": at}, "label": label}


def test_answers_with_clips_no_matches_and_clips_outside_coverage():
    async def answering(payload):
        return {"text": "A truck arrived at 09:14.", "clips": [found(1), found(3)], "follow_ups": ["Same for the side gate"]}
    answer = run(run_analysis(analysis_request(choice="from_now"), answering))
    assert answer["status"] == "answered" and [c["at_ms"] for c in answer["clips"]] == sorted(c["at_ms"] for c in answer["clips"])
    assert answer["coverage"]["not_indexed"][1]["camera"] == "cam-side"

    async def empty(payload):
        return {"text": "No trucks in indexed footage.", "clips": []}
    assert run(run_analysis(analysis_request(choice="from_now"), empty))["status"] == "no_matches"

    async def outside(payload):
        return {"text": "A truck came early.", "clips": [found(8)]}
    failed = run(run_analysis(analysis_request(choice="from_now"), outside))
    assert failed["status"] == "failed" and failed["coverage"]["searched"]


def test_nothing_indexed_is_not_the_same_as_no_matches():
    request = analysis_request(choice="from_now", scope={"cameras": ["cam-side"], "from_ms": NOW - HOUR, "to_ms": NOW})
    assert run(run_analysis(request))["status"] == "not_indexed"


# Host protocol

MODEL = {"version": "1", "effect": "read", "input_schema": {"type": "object"}, "output_schema": {"type": "object"}}


class Harness:
    def __init__(self, tmp_path, tools=()):
        self.out = []
        self.store = Store(tmp_path/"host.sqlite")
        self.host = Host(builtin_registry(), self.store, self.out.append, clock=lambda: NOW)
        self.tools = [{"name": name, **MODEL} for name in tools]

    async def send(self, kind, body, ident="m"):
        await self.host.receive(json.dumps({"v": 1, "type": kind, "id": ident, "body": body}))
        for _ in range(20):
            await asyncio.sleep(0)

    def of(self, kind):
        return [m for m in self.out if m["type"] == kind]


def test_ready_lists_the_contract_versions_and_a_free_plan_needs_no_tool_call(tmp_path):
    async def go():
        h = Harness(tmp_path)
        await h.send("hello", {"protocol": 1, "tools": []}, "hello")
        assert h.of("ready")[0]["body"]["contracts"] == CONTRACTS
        await h.send("plan", plan_request(example=example()), "p1")
        result = h.of("plan.result")[0]
        assert result["reply_to"] == "p1" and result["body"]["status"] == "ready" and not h.of("tool.call")
        await h.send("plan", plan_request(tier="cloud", text="Watch the gate."), "p2")
        assert h.of("plan.result")[1]["body"]["failure"]["code"] == "cloud_unavailable"
        await h.send("plan", plan_request(), "bad")
        assert [m["body"]["code"] for m in h.out if m.get("reply_to") == "bad"] == ["plan_request_needs_one_of_text_example_reply"]
        h.store.close()
    asyncio.run(go())


def test_a_cloud_plan_round_trips_through_the_apps_model_tool(tmp_path):
    async def go():
        h = Harness(tmp_path, ["seer.model.plan"])
        await h.send("hello", {"protocol": 1, "tools": h.tools}, "hello")
        await h.send("plan", plan_request(tier="cloud", text="Tell me if someone stays at the main gate."), "p1")
        call = h.of("tool.call")[0]["body"]
        assert call["tool"] == "seer.model.plan" and call["arguments"]["revision"] == 1 and not h.of("plan.result")
        await h.send("tool.result", {"call": call["call"], "status": "succeeded", "data": cloud_candidate()}, "r1")
        result = h.of("plan.result")[0]
        assert result["reply_to"] == "p1" and result["body"]["status"] == "ready" and result["body"]["local_only"] is False
        h.store.close()
    asyncio.run(go())


def test_ask_relays_watching_progress_and_answers(tmp_path):
    async def go():
        h = Harness(tmp_path, ["seer.model.ask"])
        await h.send("hello", {"protocol": 1, "tools": h.tools}, "hello")
        await h.send("ask", ask_request(attachments=[clip(16, 10)]), "a1")
        call = h.of("tool.call")[0]["body"]
        await h.send("tool.progress", {"call": call["call"], "done_ms": 240000}, "g1")
        await h.send("tool.result", {"call": call["call"], "status": "succeeded",
                                     "data": {"status": "answered", "text": "Yes.", "moments": [moment(12)]}}, "r1")
        progress = [m["body"]["done_ms"] for m in h.of("ask.progress")]
        assert progress == [0, 240000] and all(m["reply_to"] == "a1" for m in h.of("ask.progress"))
        assert h.of("ask.result")[0]["body"]["status"] == "answered"
        await h.send("tool.progress", {"call": call["call"], "done_ms": 1}, "late")
        assert [m["body"]["code"] for m in h.out if m.get("reply_to") == "late"] == ["unknown_call"]
        h.store.close()
    asyncio.run(go())


def test_a_model_that_never_answers_becomes_a_failure_not_a_hang(tmp_path):
    async def go():
        h = Harness(tmp_path, ["seer.model.analysis"])
        await h.send("hello", {"protocol": 1, "tools": h.tools}, "hello")
        await h.send("analysis", {**analysis_request(choice="from_now"), "timeout_ms": 20}, "n1")
        assert h.of("tool.call") and not h.of("analysis.result")
        await asyncio.sleep(0.1)
        result = h.of("analysis.result")[0]["body"]
        assert result["status"] == "failed" and result["failure"]["code"] == "analysis_failed"
        h.store.close()
    asyncio.run(go())


def test_every_contract_schema_is_valid_json_schema():
    from jsonschema import Draft202012Validator
    for name, schema in SCHEMAS.items():
        Draft202012Validator.check_schema(schema)
    assert set(CONTRACTS) <= set(SCHEMAS) and object_schema({})["additionalProperties"] is False
