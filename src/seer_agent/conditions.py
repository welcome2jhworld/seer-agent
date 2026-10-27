"""Example conditions are replaceable; the runtime does not know their semantics."""
from __future__ import annotations

from .contracts import Event, Evaluation, ID, MemoryView, object_schema, validate


CONFIG = object_schema({
    "source": ID, "kind": ID, "area": ID,
    "hold_ms": {"type": "integer", "minimum": 0, "maximum": 86400000},
    "clear_ms": {"type": "integer", "minimum": 0, "maximum": 86400000},
    "max_gap_ms": {"type": "integer", "minimum": 1, "maximum": 86400000},
    "document_source": ID, "operator_source": ID, "sensor_source": ID,
    "region": {"type": "array", "minItems": 4, "maxItems": 4,
               "items": {"type": "number", "minimum": 0, "maximum": 1}},
}, ["source", "kind", "area", "hold_ms", "clear_ms", "max_gap_ms", "document_source", "operator_source"])


class Presence:
    version = "1"

    def __init__(self, config: dict):
        validate(CONFIG, config)
        self.config = config
        x, y, width, height = config.get("region", [0, 0, 1, 1])
        if width <= 0 or height <= 0 or x+width > 1 or y+height > 1:
            raise ValueError("invalid_region")

    def context(self, memory: MemoryView, source: str | None, kind: str):
        if source is None:
            return None
        rows = memory.query(source=source, kind=kind, limit=1000)
        applicable = [r for r in rows if r.event.data.get("area") == self.config["area"]
                      or (r.event.quality == "unknown" and "area" not in r.event.data)]
        latest = max(applicable, key=lambda r: (r.event.observed_at_ms, r.revision), default=None)
        return latest if latest and latest.event.quality == "known" else None

    def signal(self, event: Event) -> bool | None:
        if event.quality == "unknown":
            return None
        schema = object_schema({"detections": {"type": "array", "maxItems": 1000, "items": object_schema({
            "label": {"type": "string"}, "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "box": {"type": "array", "minItems": 4, "maxItems": 4,
                    "items": {"type": "number", "minimum": 0, "maximum": 1}},
        })}})
        try:
            validate(schema, event.data)
        except ValueError:
            return None
        x, y, width, height = self.config.get("region", [0, 0, 1, 1])
        for detection in event.data["detections"]:
            bx, by, bw, bh = detection["box"]
            if bw <= 0 or bh <= 0 or bx+bw > 1 or by+bh > 1:
                return None
            if detection["label"] == "person" and detection["confidence"] >= 0.5 and (
                x <= bx+bw/2 <= x+width and y <= by+bh/2 <= y+height):
                return True
        return False

    def describe(self, operator, sensor) -> tuple[str, str]:
        if operator and operator.event.data.get("authorized_visit") is True:
            return "review", "Person presence detected; an operator reported an expected visit. Verify the visit."
        return "opened", "Person presence detected in the monitored region."

    def evaluate(self, event: Event, state: dict, memory: MemoryView) -> Evaluation:
        config = self.config
        if event.source != config["source"] or event.kind != config["kind"]:
            return Evaluation(state)
        state = dict(state)
        now = event.observed_at_ms
        if now <= state.get("last_observed_ms", -1):
            return Evaluation(state, summary="Late or repeated observation retained as history only.")
        document = self.context(memory, config["document_source"], "document")
        operator = self.context(memory, config["operator_source"], "message")
        sensor = self.context(memory, config.get("sensor_source"), "sensor")
        evidence = tuple([event.id]+[r.event.id for r in (document, operator, sensor) if r])
        signal = self.signal(event)
        gap = now-state.get("last_observed_ms", now) > config["max_gap_ms"]
        if gap or signal is None:
            state.pop("positive_since", None)
            state.pop("negative_since", None)
        state["last_observed_ms"] = now
        state["health"] = "unknown" if signal is None else "observing"
        state["context"] = "available" if document else "missing"
        if signal is None:
            return Evaluation(state, summary="Observation unavailable; active incident remains unchanged.", evidence=evidence)
        if signal:
            state.pop("negative_since", None)
            state.setdefault("positive_since", now)
            if not state.get("active") and now-state["positive_since"] >= config["hold_ms"]:
                state["active"] = True
                state["episode"] = event.id
                transition, summary = self.describe(operator, sensor)
                if not document:
                    transition, summary = "review", summary+" Site document is unavailable; operator review is required."
                state["assessment"] = transition
                return Evaluation(state, transition, summary, evidence)
        else:
            state.pop("positive_since", None)
            state.setdefault("negative_since", now)
            if state.get("active") and now-state["negative_since"] >= config["clear_ms"]:
                state["active"] = False
                return Evaluation(state, "cleared", "The observed condition cleared. Incident history is retained.", evidence)
        return Evaluation(state, evidence=evidence)


class OilLeak(Presence):
    version = "1"

    def signal(self, event: Event) -> bool | None:
        if event.quality == "unknown":
            return None
        try:
            validate(object_schema({
                "change": {"enum": ["changed", "unchanged", "unknown"]},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "reference_id": ID,
            }), event.data)
        except ValueError:
            return None
        if event.data["change"] == "unknown" or event.data["confidence"] < 0.5:
            return None
        return event.data["change"] == "changed"

    def evaluate(self, event: Event, state: dict, memory: MemoryView) -> Evaluation:
        if event.source == self.config["source"] and event.kind == self.config["kind"]:
            if event.observed_at_ms <= state.get("last_observed_ms", -1):
                return Evaluation(state, summary="Late or repeated observation retained as history only.")
            state = dict(state)
            reference = event.data.get("reference_id") if self.signal(event) is not None else None
            if reference and state.get("reference_id") not in {None, reference}:
                # A new baseline cannot clear or continue evidence from the previous baseline.
                event = Event(event.id, event.source, event.kind, event.observed_at_ms,
                              {"reason": "reference_changed"}, "unknown", event.provenance, event.evidence)
                state["reference_invalid"] = True
            if state.get("reference_invalid"):
                event = Event(event.id, event.source, event.kind, event.observed_at_ms,
                              {"reason": "reference_changed"}, "unknown", event.provenance, event.evidence)
            elif reference:
                state["reference_id"] = reference
        return super().evaluate(event, state, memory)

    def describe(self, operator, sensor) -> tuple[str, str]:
        if operator and operator.event.data.get("maintenance_active") is True:
            return "review", "Persistent visible change during reported maintenance; inspect for suspected leakage."
        if sensor and sensor.event.data.get("oil_level") == "low":
            return "opened", "Suspected oil leak: persistent visible change and a low oil-level reading. Confirm on site."
        return "review", "Persistent visible change; fluid identity and leakage are unconfirmed. Inspect the area."


DWELL_CONFIG = object_schema({
    "source": ID, "kind": ID,
    "labels": {"type": "array", "items": {"type": "string", "minLength": 1, "maxLength": 64},
               "minItems": 1, "maxItems": 32, "uniqueItems": True},
    "zone": {"type": "array", "minItems": 3, "maxItems": 64, "items": {
        "type": "array", "minItems": 2, "maxItems": 2, "items": {"type": "number", "minimum": 0, "maximum": 1}}},
    "dwell_ms": {"type": "integer", "minimum": 0, "maximum": 86400000},
    "hidden_ms": {"type": "integer", "minimum": 0, "maximum": 600000},
    "min_confidence": {"type": "number", "minimum": 0, "maximum": 1},
}, ["source", "kind", "labels", "dwell_ms"])
TRACKED = object_schema({"session_id": ID, "detections": {"type": "array", "maxItems": 1000, "items": object_schema({
    "label": {"type": "string"}, "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    "box": {"type": "array", "minItems": 4, "maxItems": 4, "items": {"type": "number", "minimum": 0, "maximum": 1}},
    "track_id": ID,
})}}, ["detections"])


def inside(point, polygon) -> bool:
    x, y = point
    result = False
    for (x1, y1), (x2, y2) in zip(polygon, polygon[1:] + polygon[:1]):
        if (y1 > y) != (y2 > y) and x < x1 + (y - y1) * (x2 - x1) / (y2 - y1):
            result = not result
    return result


class ObjectDwell:
    """One alert per appearance of a tracked object in a zone. An object is in the zone when the
    bottom center of its box is inside the polygon. It keeps its appearance through up to
    hidden_ms (10 s by default) of not being seen there, and can alert again after a longer
    absence. An unknown observation or a new camera session restarts every dwell timer; objects
    already alerted stay alerted. The incident clears once no tracked object remains."""
    version = "1"

    def __init__(self, config: dict):
        validate(DWELL_CONFIG, config)
        self.config = config
        self.zone = [tuple(point) for point in config.get("zone", [[0, 0], [1, 0], [1, 1], [0, 1]])]
        self.hidden = config.get("hidden_ms", 10000)
        self.labels = set(config["labels"])
        self.minimum = config.get("min_confidence", 0.5)

    def present(self, event: Event) -> dict[str, str] | None:
        if event.quality == "unknown":
            return None
        try:
            validate(TRACKED, event.data)
        except ValueError:
            return None
        found = {}
        for detection in event.data["detections"]:
            bx, by, bw, bh = detection["box"]
            if bw <= 0 or bh <= 0 or bx+bw > 1 or by+bh > 1:
                return None
            if (detection["label"] in self.labels and detection["confidence"] >= self.minimum
                    and inside((bx+bw/2, by+bh), self.zone)):
                found[detection["track_id"]] = detection["label"]
        return found

    def evaluate(self, event: Event, state: dict, memory: MemoryView) -> Evaluation:
        config = self.config
        if event.source != config["source"] or event.kind not in {config["kind"], "coverage"}:
            return Evaluation(state)
        now = event.observed_at_ms
        if now <= state.get("last_observed_ms", -1):
            return Evaluation(state, summary="Late or repeated observation retained as history only.")
        state = {**state, "last_observed_ms": now}
        tracks = dict(state.get("tracks", {}))
        session = event.data.get("session_id") if event.kind == config["kind"] else None
        present = self.present(event) if event.kind == config["kind"] else None
        if present is None or (session and state.get("session_id") not in {None, session}):
            tracks = {key: {**value, "since": None} for key, value in tracks.items()}
        if session:
            state["session_id"] = session
        if present is None:
            state["health"] = "unknown"
            state["tracks"] = tracks
            return Evaluation(state, summary="Observation unavailable; dwell restarts when observation resumes.",
                              evidence=(event.id,))
        state["health"] = "observing"
        for key, label in present.items():
            track = tracks.get(key)
            if track is None or now - track["seen"] > self.hidden:
                track = {"label": label, "since": now, "seen": now, "alerted": False}
            elif track["since"] is None:
                track = {**track, "since": now}
            tracks[key] = {**track, "seen": now}
        gone = [key for key, track in tracks.items() if key not in present and now - track["seen"] > self.hidden]
        for key in gone:
            del tracks[key]
        if len(tracks) > 256:
            tracks = dict(sorted(tracks.items(), key=lambda item: item[1]["seen"])[-256:])
        due = sorted(key for key, track in tracks.items() if key in present and not track["alerted"]
                     and track["since"] is not None and now - track["since"] >= config["dwell_ms"])
        for key in due:
            tracks[key] = {**tracks[key], "alerted": True}
        state["tracks"] = tracks
        if due:
            labels = sorted({tracks[key]["label"] for key in due})
            seconds = config["dwell_ms"] / 1000
            stay = f" stayed in the zone for {seconds:g} s" if seconds else " appeared in the zone"
            state["active"] = True
            return Evaluation(state, "opened", f"{', '.join(labels).capitalize()}{stay}.", (event.id,))
        if state.get("active") and not tracks:
            state["active"] = False
            return Evaluation(state, "cleared", "The object left the zone. Incident history is retained.", (event.id,))
        return Evaluation(state, evidence=(event.id,))


def register_conditions(registry):
    registry.condition("person_presence", Presence)
    registry.condition("oil_leak", OilLeak)
    registry.condition("object_dwell", ObjectDwell)
