"""Versioned contracts for the app's conversation screens: the task plan card, Ask on Live and Analysis.

The harness owns the rules that do not need a model: request validation, the free example plans, the
Ask window, search coverage and the indexing offer, and the checks every model candidate must pass.
A model only fills content; a candidate that breaks a rule becomes a typed failure, never a partial card.
Fixed sentences carry a stable code with English text so the app can translate them. Times are epoch
milliseconds; the app formats them in the site's time zone."""
from __future__ import annotations

import copy
import re
from typing import Any

from .contracts import ID, TIME, ContractError, Mission, object_schema, validate

CONTRACTS = {"seer.task_plan": 1, "seer.ask": 1, "seer.analysis": 1}
ASK_WINDOW_MS = 600000
INDEX_OFFER_MS = 86400000

CONVERSATION = {**ID, "maxLength": 120}
LABEL = {"type": "string", "minLength": 1, "maxLength": 60}
LINE = {"type": "string", "minLength": 1, "maxLength": 200}
WORDS = {"type": "string", "minLength": 1, "maxLength": 2000}
CAMERA = object_schema({"id": ID, "name": LABEL})
CAMERAS = {"type": "array", "items": CAMERA, "minItems": 1, "maxItems": 64}
POINT = {"type": "array", "minItems": 2, "maxItems": 2, "items": {"type": "number", "minimum": 0, "maximum": 1}}
ZONE = object_schema({"name": {"type": "string", "minLength": 1, "maxLength": 40},
                      "polygon": {"type": "array", "items": POINT, "minItems": 3, "maxItems": 16}})
CLOCK = {"type": "string", "pattern": r"^([01][0-9]|2[0-3]):[0-5][0-9]$"}
HOURS = {"oneOf": [object_schema({"all_day": {"const": True}}), object_schema({"from": CLOCK, "to": CLOCK})]}
RANGE = object_schema({"camera": ID, "from_ms": TIME, "to_ms": TIME})
FRAME = object_schema({"camera": ID, "at_ms": TIME})
NOTE = object_schema({"code": ID, "text": LINE})
REPLY = object_schema({"id": ID, "label": {"type": "string", "minLength": 1, "maxLength": 40}})
QUESTION = object_schema({"id": ID, "text": LINE, "replies": {"type": "array", "items": REPLY, "minItems": 1, "maxItems": 4}})


def head(name: str) -> dict:
    return {"contract": {"const": name}, "version": {"const": CONTRACTS.get(name, 1)}}


def note(code: str, text: str) -> dict:
    return {"code": code, "text": text}


ABBREVIATIONS = {"a.m", "p.m", "e.g", "i.e", "etc", "vs", "mr", "mrs", "ms", "dr", "st", "no", "approx"}


def sentences(text: str) -> int:
    """A sentence ends at . ! or ? followed by space and anything but a lowercase Latin letter, unless the
    word before the period is a known abbreviation ("9 p.m. on weekdays", "e.g. Main gate")."""
    count, start = 1, 0
    text = text.strip()
    for match in re.finditer(r"[.!?]\s+(?=[^a-z])", text):
        word = text[start:match.start()].rsplit(None, 1)[-1].lower() if text[start:match.start()].strip() else ""
        if match.group(0)[0] == "." and word.rstrip(".") in ABBREVIATIONS:
            continue
        count, start = count + 1, match.end()
    return count if text else 0


def unique_cameras(cameras: list[dict]) -> set[str]:
    ids = [c["id"] for c in cameras]
    if len(set(ids)) != len(ids):
        raise ContractError("duplicate_camera")
    return set(ids)


def contained(item: dict, ranges: list[dict]) -> bool:
    """The whole range sits inside one range of the same camera."""
    return any(r["camera"] == item["camera"] and r["from_ms"] <= item["from_ms"] <= item["to_ms"] <= r["to_ms"] for r in ranges)


def ranges_valid(ranges: list[dict]) -> None:
    for item in ranges:
        if item["to_ms"] < item["from_ms"]:
            raise ContractError("invalid_range")


def within(at_ms: int, camera: str, ranges: list[dict]) -> bool:
    return any(r["camera"] == camera and r["from_ms"] <= at_ms <= r["to_ms"] for r in ranges)


# Task plan (D27, D35; free tier in PRD section 6)

TOOLS = {
    "person_detection": ("Person detection", "local"), "vehicle_detection": ("Vehicle detection", "local"),
    "bike_detection": ("Bike detection", "local"), "zone_entry": ("Zone entry", "local"),
    "time_in_area": ("Time in area", "local"), "direction": ("Direction", "local"),
    "change_detection": ("Change detection", "local"), "clip_review": ("Clip review", "cloud"),
}
EXAMPLE_LABELS = {"person": ("person_detection", ["person"], "a person", "Person"),
                  "vehicle": ("vehicle_detection", ["car", "motorcycle", "bus", "truck"], "a vehicle", "Vehicle"),
                  "bike": ("bike_detection", ["bicycle"], "a bike", "Bike")}
ACTION_QUESTION = {"id": "response", "text": "Should I alert you, or only record it?",
                   "replies": [{"id": "alert", "label": "Alert me"}, {"id": "record", "label": "Only record"}]}
WHOLE_VIEW = {"name": "Whole view", "polygon": [[0, 0], [1, 0], [1, 1], [0, 1]]}

TOOL_REF = object_schema({"id": {"enum": sorted(TOOLS)}, "label": LABEL})
CANDIDATE_TOOL = object_schema({"id": {"enum": sorted(TOOLS)}, "label": LABEL}, ["id"])
EXAMPLE = object_schema({
    "kind": {"enum": ["zone_entry", "time_in_area"]},
    "labels": {"type": "array", "items": {"enum": sorted(EXAMPLE_LABELS)}, "minItems": 1, "maxItems": 3, "uniqueItems": True},
    "camera": ID, "zone": ZONE, "dwell_s": {"type": "integer", "minimum": 1, "maximum": 3600}, "hours": HOURS,
    "response": {"enum": ["alert", "record"]},
}, ["kind", "labels", "camera"])
PLAN_INPUT = object_schema({"camera": ID, "camera_name": LABEL, "zone": ZONE, "hours": HOURS})
PLAN_BODY = {
    "title": LABEL, "goal": LINE, "input": PLAN_INPUT,
    "tools": {"type": "array", "items": TOOL_REF, "minItems": 1, "maxItems": 6},
    "reasoning": {"type": "string", "minLength": 1, "maxLength": 320},
    "question": {"oneOf": [{"type": "null"}, QUESTION]},
    "response": {"enum": ["alert", "record"]},
}
TASK_PLAN = object_schema({
    **head("seer.task_plan"), "conversation": CONVERSATION, "revision": {"type": "integer", "minimum": 1},
    "status": {"enum": ["ready", "unsupported", "needs_cloud", "failed"]},
    **PLAN_BODY, "local_only": {"type": "boolean"},
    "alternative": object_schema({"reason": NOTE, "suggestion": LINE, "example": EXAMPLE}, ["reason", "suggestion"]),
    "examples": {"type": "array", "items": EXAMPLE, "maxItems": 3}, "example": EXAMPLE,
    "failure": NOTE, "notice": NOTE, "mission": {"type": "object"},
}, ["contract", "version", "conversation", "revision", "status"])
PLAN_REQUEST = object_schema({
    **head("seer.plan_request"), "conversation": CONVERSATION, "tier": {"enum": ["free", "cloud"]}, "cameras": CAMERAS,
    "text": WORDS, "example": EXAMPLE, "previous": {"type": "object"},
    "reply": {"oneOf": [object_schema({"question": ID, "choice": ID}), object_schema({"text": WORDS})]},
}, ["contract", "version", "conversation", "tier", "cameras"])
# What a model returns for a cloud plan: the card body, or an unsupported condition with an observable alternative.
PLAN_CANDIDATE = {"oneOf": [
    object_schema({"status": {"const": "ready"}, **PLAN_BODY,
                   "tools": {"type": "array", "items": CANDIDATE_TOOL, "minItems": 1, "maxItems": 6}}, ["status", "title", "goal", "input", "tools", "reasoning", "question"]),
    object_schema({"status": {"const": "unsupported"}, "suggestion": LINE, "example": EXAMPLE}, ["status", "suggestion"]),
]}
CANNOT_WATCH = note("cannot_watch_reliably", "I can't watch for this reliably.")
PLAN_FAILED = note("plan_failed", "Couldn't make a plan.")
NEEDS_CLOUD = note("needs_cloud", "Describing a task in your own words needs Seer Cloud.")


def check_plan_request(request: dict) -> dict:
    """Everything that could fail later is checked here, so a valid request always gets a card."""
    validate(PLAN_REQUEST, request)
    ids = unique_cameras(request["cameras"])
    asks = [key for key in ("text", "example", "reply") if key in request]
    if len(asks) != 1:
        raise ContractError("plan_request_needs_one_of_text_example_reply")
    if "reply" in request and "previous" not in request:
        raise ContractError("reply_needs_previous_plan")
    if "previous" in request:
        previous = request["previous"]
        validate(TASK_PLAN, previous)
        if previous["conversation"] != request["conversation"]:
            raise ContractError("reply_to_another_conversation")
        reply = request.get("reply", {})
        if "choice" in reply:
            question = previous.get("question")
            if (not question or reply["question"] != question["id"]
                    or reply["choice"] not in {r["id"] for r in question["replies"]}):
                raise ContractError("unknown_reply")
    if "example" in request and request["example"]["camera"] not in ids:
        raise ContractError("unknown_camera")
    return request


def plan_revision(request: dict) -> int:
    return request["previous"]["revision"] + 1 if "previous" in request else 1


def plan(conversation: str, revision: int, status: str, **body) -> dict:
    value = {"contract": "seer.task_plan", "version": CONTRACTS["seer.task_plan"], "conversation": conversation,
             "revision": revision, "status": status, **body}
    validate(TASK_PLAN, value)
    return value


def examples(cameras: list[dict]) -> list[dict]:
    """Example tasks built from the user's own cameras, for the empty Tasks page and the free tier."""
    kinds = [("time_in_area", ["person"], 10), ("zone_entry", ["vehicle"], None), ("zone_entry", ["bike"], None)]
    found = []
    for index, (kind, labels, dwell) in enumerate(kinds):
        camera = cameras[index % len(cameras)]["id"]
        found.append({"kind": kind, "labels": labels, "camera": camera, **({"dwell_s": dwell} if dwell else {})})
    return found


def example_plan(request: dict, revision: int = 1) -> dict:
    """The free tier's plan card, built without a model from an example task."""
    example = request["example"]
    camera = next(c for c in request["cameras"] if c["id"] == example["camera"])
    zone = copy.deepcopy(example.get("zone", WHOLE_VIEW))
    labels = example["labels"]
    who = " or ".join(EXAMPLE_LABELS[label][2] for label in labels)
    title_who = " or ".join(EXAMPLE_LABELS[label][3] for label in labels)
    tools = [{"id": EXAMPLE_LABELS[label][0], "label": TOOLS[EXAMPLE_LABELS[label][0]][0]} for label in labels]
    place = "the view" if zone == WHOLE_VIEW else zone["name"]
    response = example.get("response")
    verb = "Record" if response == "record" else "Alert"
    if example["kind"] == "time_in_area":
        seconds = example.get("dwell_s", 10)
        span = f"{seconds // 60} minutes" if seconds >= 120 and seconds % 60 == 0 else f"{seconds} seconds"
        goal = f"{verb} when {who} stays in {place} for {span} or more."
        title = f"{title_who} stays in {place}"
        tools.append({"id": "time_in_area", "label": TOOLS["time_in_area"][0]})
        reasoning = (f"I start counting when the bottom of the box is inside {place}, and keep counting through "
                     f"up to 10 seconds of being hidden. Other objects are ignored.")
    else:
        goal = f"{verb} when {who} enters {place}."
        title = f"{title_who} in {place}"
        tools.append({"id": "zone_entry", "label": TOOLS["zone_entry"][0]})
        reasoning = f"An entry counts when the bottom of the box is inside {place}. Other objects are ignored."
    body = {"title": title[:60], "goal": goal, "input": {"camera": camera["id"], "camera_name": camera["name"],
            "zone": zone, "hours": example.get("hours", {"all_day": True})}, "tools": tools, "reasoning": reasoning,
            "question": None if response else copy.deepcopy(ACTION_QUESTION), "response": response or "alert",
            "local_only": True, "example": copy.deepcopy(example)}
    result = plan(request["conversation"], revision, "ready", **body)
    result["mission"] = compile_mission(result, example)
    return result


def check_plan_candidate(candidate: Any, request: dict, revision: int) -> dict:
    """Turns a model's card into the contract, or a failure the app shows as Couldn't make a plan."""
    conversation = request["conversation"]
    try:
        validate(PLAN_CANDIDATE, candidate)
        if candidate["status"] == "unsupported":
            alternative = {"reason": CANNOT_WATCH, "suggestion": candidate["suggestion"]}
            if "example" in candidate:
                if candidate["example"]["camera"] not in {c["id"] for c in request["cameras"]}:
                    raise ContractError("unknown_camera")
                alternative["example"] = candidate["example"]
            return plan(conversation, revision, "unsupported", alternative=alternative)
        if sentences(candidate["goal"]) != 1 or sentences(candidate["reasoning"]) > 2:
            raise ContractError("plan_text_too_long")
        question = candidate["question"]
        if question and question["id"] == "response" and not {r["id"] for r in question["replies"]} <= {"alert", "record"}:
            raise ContractError("reserved_question")
        names = {c["id"]: c["name"] for c in request["cameras"]}
        if candidate["input"]["camera"] not in names or candidate["input"]["camera_name"] != names[candidate["input"]["camera"]]:
            raise ContractError("unknown_camera")
        tools = [tool["id"] for tool in candidate["tools"]]
        if len(set(tools)) != len(tools):
            raise ContractError("duplicate_tool")
        body = {key: candidate[key] for key in PLAN_BODY if key in candidate}
        body["tools"] = [{"id": tool, "label": TOOLS[tool][0]} for tool in tools]
        body.setdefault("response", "alert")
        local = all(TOOLS[tool][1] == "local" for tool in tools)
        return plan(conversation, revision, "ready", **body, local_only=local)
    except ContractError:
        return plan(conversation, revision, "failed", failure=PLAN_FAILED)


def answer_plan(request: dict) -> dict | None:
    """Only a free card's alert-or-record reply is applied here: the card is rebuilt from its example, so the
    goal and the mission follow the answer. Every other reply goes to the planner."""
    previous, reply = request["previous"], request["reply"]
    if "choice" not in reply or reply["question"] != "response" or "example" not in previous:
        return None
    return example_plan({**request, "example": {**previous["example"], "response": reply["choice"]}}, previous["revision"] + 1)


def compile_mission(card: dict, example: dict) -> dict:
    """A free example plan becomes an object_dwell mission the app can open with Start watching. Watching
    hours stay with the app, which routes frames only inside them."""
    camera = card["input"]["camera"]
    labels = sorted({name for label in example["labels"] for name in EXAMPLE_LABELS[label][1]})
    dwell = example.get("dwell_s", 10) * 1000 if example["kind"] == "time_in_area" else 0
    mission = {
        "schema_version": 1, "id": f"task.{card['conversation']}", "version": card["revision"],
        "purpose": card["goal"], "sources": {camera: ["vision.tracks", "coverage"]}, "observe": [],
        "remember": {"sources": [camera], "max_items": 50, "max_age_ms": 86400000},
        "condition": {"type": "object_dwell", "config": {"source": camera, "kind": "vision.tracks", "labels": labels,
                      "zone": card["input"]["zone"]["polygon"], "dwell_ms": dwell}},
        "act": [{"on": "opened", "tool": "record", "arguments": {"text": "$summary"}},
                {"on": "cleared", "tool": "record", "arguments": {"text": "$summary"}}],
        "policy": {"allowed_tools": ["record"], "allow_external": False, "max_pending": 64,
                   "max_concurrency": 4, "timeout_ms": 30000, "max_event_age_ms": 60000},
    }
    return Mission.from_dict(mission).document


# Ask on Live (D33): now and the last 10 minutes, plus clips the user attached

ATTACHMENT = {"oneOf": [
    object_schema({"kind": {"const": "photo"}, "id": ID, "name": LABEL}, ["kind", "id"]),
    object_schema({"kind": {"const": "file"}, "id": ID, "name": LABEL, "media_type": LABEL}),
    object_schema({"kind": {"const": "camera"}, "camera": ID}),
    object_schema({"kind": {"const": "clip"}, "camera": ID, "from_ms": TIME, "to_ms": TIME}),
]}
TURN = object_schema({"role": {"enum": ["you", "seer"]}, "text": WORDS})
ASK_REQUEST = object_schema({
    **head("seer.ask_request"), "conversation": CONVERSATION, "now_ms": TIME, "text": WORDS, "cameras": CAMERAS,
    "attachments": {"type": "array", "items": ATTACHMENT, "maxItems": 8},
    "history": {"type": "array", "items": TURN, "maxItems": 20},
}, ["contract", "version", "conversation", "now_ms", "text", "cameras"])
MOMENT = object_schema({"at_ms": TIME, "camera": ID, "frame": FRAME, "text": {"type": "string", "minLength": 1, "maxLength": 160}})
FOLLOW_UPS = {"type": "array", "items": {"type": "string", "minLength": 1, "maxLength": 80}, "maxItems": 3}
ASK_COVERAGE = object_schema({"watched": {"type": "array", "items": RANGE, "maxItems": 64},
                              "not_available": {"type": "array", "items": RANGE, "maxItems": 64}})
ASK = object_schema({
    **head("seer.ask"), "conversation": CONVERSATION, "kind": {"enum": ["progress", "answer"]},
    "status": {"enum": ["watching", "answered", "handoff", "needs_cloud", "failed"]},
    "watching": RANGE, "done_ms": TIME, "total_ms": TIME,
    "text": {"type": "string", "minLength": 1, "maxLength": 1200},
    "moments": {"type": "array", "items": MOMENT, "maxItems": 12}, "coverage": ASK_COVERAGE, "follow_ups": FOLLOW_UPS,
    "handoff": object_schema({"to": {"const": "analysis"}, "query": WORDS, "reason": NOTE}), "failure": NOTE, "notice": NOTE,
}, ["contract", "version", "conversation", "kind", "status"])
ASK_CANDIDATE = {"oneOf": [
    object_schema({"status": {"const": "answered"}, "text": ASK["properties"]["text"], "moments": ASK["properties"]["moments"],
                   "not_available": {"type": "array", "items": RANGE, "maxItems": 64}, "follow_ups": FOLLOW_UPS},
                  ["status", "text", "moments"]),
    object_schema({"status": {"const": "handoff"}, "query": WORDS}),
]}
OLDER = note("older_than_ask_window", "Ask looks at now and the last 10 minutes. Analysis can search older footage.")
CLIP_TOO_LONG = note("clip_longer_than_ask_window", "Ask watches clips up to 10 minutes. Analysis can search longer ranges.")
ASK_FAILED = note("ask_failed", "Couldn't answer.")
ASK_NEEDS_CLOUD = note("needs_cloud", "Asking about your cameras needs Seer Cloud.")


def check_ask_request(request: dict) -> dict:
    validate(ASK_REQUEST, request)
    ids = unique_cameras(request["cameras"])
    for item in request.get("attachments", []):
        if item["kind"] in {"camera", "clip"} and item["camera"] not in ids:
            raise ContractError("unknown_camera")
        if item["kind"] == "clip" and not item["from_ms"] <= item["to_ms"] <= request["now_ms"]:
            raise ContractError("invalid_range")
    return request


def ask(conversation: str, kind: str, status: str, **body) -> dict:
    value = {"contract": "seer.ask", "version": CONTRACTS["seer.ask"], "conversation": conversation,
             "kind": kind, "status": status, **body}
    validate(ASK, value)
    return value


def ask_scope(request: dict) -> tuple[list[dict], dict | None]:
    """What Ask may look at: attached clips when there are any, otherwise the last 10 minutes of the
    attached cameras or every camera. A clip longer than the window goes to Analysis instead."""
    now, attachments = request["now_ms"], request.get("attachments", [])
    clips = [a for a in attachments if a["kind"] == "clip"]
    for clip in clips:
        if clip["to_ms"] - clip["from_ms"] > ASK_WINDOW_MS:
            return [], ask(request["conversation"], "answer", "handoff",
                           handoff={"to": "analysis", "query": request["text"], "reason": CLIP_TOO_LONG})
    if clips:
        return [{"camera": c["camera"], "from_ms": c["from_ms"], "to_ms": c["to_ms"]} for c in clips], None
    cameras = [a["camera"] for a in attachments if a["kind"] == "camera"] or [c["id"] for c in request["cameras"]]
    start = max(0, now - ASK_WINDOW_MS)
    return [{"camera": camera, "from_ms": start, "to_ms": now} for camera in dict.fromkeys(cameras)], None


def check_ask_candidate(candidate: Any, request: dict, scope: list[dict]) -> dict:
    """Moments must fall inside what Ask watched; a model that reaches outside it is refused."""
    conversation = request["conversation"]
    try:
        validate(ASK_CANDIDATE, candidate)
        if candidate["status"] == "handoff":
            return ask(conversation, "answer", "handoff",
                       handoff={"to": "analysis", "query": candidate["query"], "reason": OLDER})
        for moment in candidate["moments"]:
            if (moment["frame"]["camera"] != moment["camera"] or not within(moment["at_ms"], moment["camera"], scope)
                    or not within(moment["frame"]["at_ms"], moment["camera"], scope)):
                raise ContractError("moment_outside_scope")
        gaps = candidate.get("not_available", [])
        ranges_valid(gaps)
        if not all(contained(g, scope) for g in gaps):
            raise ContractError("gap_outside_scope")
        return ask(conversation, "answer", "answered", text=candidate["text"],
                   moments=sorted(candidate["moments"], key=lambda m: m["at_ms"]),
                   coverage={"watched": scope, "not_available": gaps}, follow_ups=candidate.get("follow_ups", []))
    except ContractError:
        return ask(conversation, "answer", "failed", failure=ASK_FAILED)


def ask_progress(request: dict, scope: list[dict], done_ms: int) -> dict:
    """done_ms counts through the watched ranges in order; watching is the range it has reached."""
    total = sum(r["to_ms"] - r["from_ms"] for r in scope)
    done, passed, current = min(done_ms, total), 0, scope[-1]
    for item in scope:
        if done < passed + (item["to_ms"] - item["from_ms"]):
            current = item
            break
        passed += item["to_ms"] - item["from_ms"]
    return ask(request["conversation"], "progress", "watching", watching=current, done_ms=done, total_ms=total)


# Analysis (D17, D38): searches indexed footage, says what was searched, offers to index the rest

INDEX = object_schema({"camera": ID, "indexed_from_ms": {"oneOf": [{"type": "null"}, TIME]}})
ANALYSIS_REQUEST = object_schema({
    **head("seer.analysis_request"), "conversation": CONVERSATION, "now_ms": TIME, "text": WORDS, "cameras": CAMERAS,
    "scope": object_schema({"cameras": {"type": "array", "items": ID, "minItems": 1, "maxItems": 64, "uniqueItems": True},
                            "from_ms": TIME, "to_ms": TIME}),
    "index": {"type": "array", "items": INDEX, "maxItems": 64},
    "choice": {"enum": ["last_24_hours", "from_now"]}, "first_search": {"type": "boolean"},
    "credits_per_camera_hour": {"type": "number", "minimum": 0, "maximum": 1000000},
}, ["contract", "version", "conversation", "now_ms", "text", "cameras", "scope", "index"])
CLIP = object_schema({"id": ID, "camera": ID, "at_ms": TIME, "length_ms": {"type": "integer", "minimum": 0, "maximum": 86400000},
                      "frame": FRAME, "label": {"type": "string", "minLength": 1, "maxLength": 80}})
SEARCH_COVERAGE = object_schema({"searched": {"type": "array", "items": RANGE, "maxItems": 64},
                                 "not_indexed": {"type": "array", "items": RANGE, "maxItems": 64}})
OFFER = object_schema({"cameras": {"type": "array", "items": ID, "minItems": 1, "maxItems": 64}, "from_ms": TIME, "to_ms": TIME,
                       "credits": {"oneOf": [{"type": "null"}, {"type": "integer", "minimum": 0}]},
                       "choices": {"type": "array", "items": {"enum": ["last_24_hours", "from_now"]}, "minItems": 2, "maxItems": 2}})
ANALYSIS = object_schema({
    **head("seer.analysis"), "conversation": CONVERSATION,
    "status": {"enum": ["answered", "no_matches", "not_indexed", "offer_indexing", "indexing", "needs_cloud", "failed"]},
    "text": {"type": "string", "minLength": 1, "maxLength": 1200}, "clips": {"type": "array", "items": CLIP, "maxItems": 24},
    "coverage": SEARCH_COVERAGE, "follow_ups": FOLLOW_UPS, "offer": OFFER, "failure": NOTE, "notice": NOTE,
    "indexing": object_schema({"cameras": OFFER["properties"]["cameras"], "from_ms": TIME, "to_ms": TIME}),
}, ["contract", "version", "conversation", "status"])
ANALYSIS_CANDIDATE = object_schema({"text": ANALYSIS["properties"]["text"], "clips": ANALYSIS["properties"]["clips"],
                                    "follow_ups": FOLLOW_UPS}, ["text", "clips"])
ANALYSIS_FAILED = note("analysis_failed", "Search couldn't finish.")
ANALYSIS_NEEDS_CLOUD = note("needs_cloud", "Searching recorded footage needs Seer Cloud.")


def check_analysis_request(request: dict) -> dict:
    validate(ANALYSIS_REQUEST, request)
    ids = unique_cameras(request["cameras"])
    scope, indexed = request["scope"], [i["camera"] for i in request["index"]]
    if not set(scope["cameras"]) <= ids or not set(indexed) <= ids:
        raise ContractError("unknown_camera")
    if len(set(indexed)) != len(indexed):
        raise ContractError("duplicate_index")
    if not scope["from_ms"] <= scope["to_ms"] <= request["now_ms"]:
        raise ContractError("invalid_range")
    return request


def analysis(conversation: str, status: str, **body) -> dict:
    value = {"contract": "seer.analysis", "version": CONTRACTS["seer.analysis"], "conversation": conversation,
             "status": status, **body}
    validate(ANALYSIS, value)
    return value


def search_coverage(request: dict) -> dict:
    """Per camera in scope: the indexed part is searched and the rest is listed as not indexed, so the
    answer can tell "No matches in indexed footage" from "Not indexed"."""
    scope, starts = request["scope"], {i["camera"]: i["indexed_from_ms"] for i in request["index"]}
    searched, missing = [], []
    for camera in scope["cameras"]:
        start = starts.get(camera)
        if start is None or start > scope["to_ms"]:
            missing.append({"camera": camera, "from_ms": scope["from_ms"], "to_ms": scope["to_ms"]})
            continue
        if start > scope["from_ms"]:
            missing.append({"camera": camera, "from_ms": scope["from_ms"], "to_ms": start})
        searched.append({"camera": camera, "from_ms": max(start, scope["from_ms"]), "to_ms": scope["to_ms"]})
    return {"searched": searched, "not_indexed": missing}


def analysis_offer(request: dict) -> dict | None:
    """The first search (the app says so with first_search) over cameras with no index at all offers the
    last 24 hours or from now (D17). Later searches answer from what is indexed and list the rest as not
    indexed. The credit estimate comes only from the rate the app supplies; the harness invents no prices."""
    conversation = request["conversation"]
    if not request.get("first_search") and "choice" not in request:
        return None
    unindexed = [c for c in request["scope"]["cameras"]
                 if next((i["indexed_from_ms"] for i in request["index"] if i["camera"] == c), None) is None]
    if not unindexed:
        return None
    start = max(0, request["now_ms"] - INDEX_OFFER_MS)
    choice = request.get("choice")
    if choice == "last_24_hours":
        return analysis(conversation, "indexing", indexing={"cameras": unindexed, "from_ms": start, "to_ms": request["now_ms"]})
    if choice == "from_now":
        return None
    rate = request.get("credits_per_camera_hour")
    hours = (request["now_ms"] - start) / 3600000
    credits = None if rate is None else round(rate * hours * len(unindexed))
    return analysis(conversation, "offer_indexing", offer={"cameras": unindexed, "from_ms": start, "to_ms": request["now_ms"],
                    "credits": credits, "choices": ["last_24_hours", "from_now"]})


def check_analysis_candidate(candidate: Any, request: dict, coverage: dict) -> dict:
    conversation = request["conversation"]
    try:
        validate(ANALYSIS_CANDIDATE, candidate)
        for clip in candidate["clips"]:
            span = {"camera": clip["camera"], "from_ms": clip["at_ms"], "to_ms": clip["at_ms"] + clip["length_ms"]}
            if (clip["frame"]["camera"] != clip["camera"] or not contained(span, coverage["searched"])
                    or not span["from_ms"] <= clip["frame"]["at_ms"] <= span["to_ms"]):
                raise ContractError("clip_outside_coverage")
        status = "answered" if candidate["clips"] else "no_matches"
        return analysis(conversation, status, text=candidate["text"], clips=sorted(candidate["clips"], key=lambda c: c["at_ms"]),
                        coverage=coverage, follow_ups=candidate.get("follow_ups", []))
    except ContractError:
        return analysis(conversation, "failed", failure=ANALYSIS_FAILED, coverage=coverage)


CLOUD_UNAVAILABLE = note("cloud_unavailable", "Couldn't reach Seer Cloud.")
STOPPED = note("stopped", "Seer stopped before answering.")


def failed(kind: str, request: dict, reason: dict) -> dict:
    """The contract's failure for a request that already passed its checks."""
    conversation = request["conversation"]
    if kind == "plan":
        return plan(conversation, plan_revision(request), "failed", failure=reason)
    if kind == "ask":
        return ask(conversation, "answer", "failed", failure=reason)
    return analysis(conversation, "failed", failure=reason)


async def make_plan(request: dict, planner=None) -> dict:
    """planner(payload) -> candidate is the cloud model, called only when no rule can answer."""
    check_plan_request(request)
    conversation, revision = request["conversation"], plan_revision(request)
    if "example" in request:
        return example_plan(request, revision)
    if "reply" in request and (quick := answer_plan(request)) is not None:
        return quick
    if request["tier"] == "free":
        return plan(conversation, revision, "needs_cloud", notice=NEEDS_CLOUD, examples=examples(request["cameras"]))
    if planner is None:
        return plan(conversation, revision, "failed", failure=CLOUD_UNAVAILABLE)
    try:
        candidate = await planner({"request": request, "revision": revision, "tools": sorted(TOOLS)})
    except Exception:
        return plan(conversation, revision, "failed", failure=PLAN_FAILED)
    return check_plan_candidate(candidate, request, revision)


async def answer_ask(request: dict, model=None, progress=None) -> dict:
    """progress(message) receives seer.ask progress while the model watches."""
    check_ask_request(request)
    scope, early = ask_scope(request)
    if early is not None:
        return early
    if model is None:
        return ask(request["conversation"], "answer", "needs_cloud", notice=ASK_NEEDS_CLOUD)
    if progress is not None:
        progress(ask_progress(request, scope, 0))
    try:
        candidate = await model({"request": request, "scope": scope})
    except Exception:
        return ask(request["conversation"], "answer", "failed", failure=ASK_FAILED)
    return check_ask_candidate(candidate, request, scope)


async def run_analysis(request: dict, model=None) -> dict:
    check_analysis_request(request)
    coverage = search_coverage(request)
    if model is None:
        return analysis(request["conversation"], "needs_cloud", notice=ANALYSIS_NEEDS_CLOUD, coverage=coverage)
    offer = analysis_offer(request)
    if offer is not None:
        return offer
    if not coverage["searched"]:
        return analysis(request["conversation"], "not_indexed", coverage=coverage)
    try:
        candidate = await model({"request": request, "coverage": coverage})
    except Exception:
        return analysis(request["conversation"], "failed", failure=ANALYSIS_FAILED, coverage=coverage)
    return check_analysis_candidate(candidate, request, coverage)


SCHEMAS = {"seer.plan_request": PLAN_REQUEST, "seer.task_plan": TASK_PLAN, "seer.plan_candidate": PLAN_CANDIDATE,
           "seer.ask_request": ASK_REQUEST, "seer.ask": ASK, "seer.ask_candidate": ASK_CANDIDATE,
           "seer.analysis_request": ANALYSIS_REQUEST, "seer.analysis": ANALYSIS,
           "seer.analysis_candidate": ANALYSIS_CANDIDATE}

__all__ = ["CONTRACTS", "SCHEMAS", "TOOLS", "ASK_WINDOW_MS", "make_plan", "answer_ask", "run_analysis", "failed", "STOPPED", "check_plan_request", "example_plan", "examples",
           "check_plan_candidate", "answer_plan", "compile_mission", "check_ask_request", "ask_scope",
           "check_ask_candidate", "ask_progress", "check_analysis_request", "search_coverage", "analysis_offer",
           "check_analysis_candidate", "plan", "ask", "analysis", "NEEDS_CLOUD", "ASK_NEEDS_CLOUD",
           "ANALYSIS_NEEDS_CLOUD", "PLAN_FAILED", "ASK_FAILED", "ANALYSIS_FAILED"]
