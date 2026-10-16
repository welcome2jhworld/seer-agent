"""Versioned public values; source data never grants execution authority."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
from typing import Any, Mapping

from jsonschema import Draft202012Validator


class ContractError(ValueError):
    pass


def encoded(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def validate(schema: dict, value: Any) -> None:
    try:
        Draft202012Validator(schema).validate(value)
        if len(encoded(value).encode()) > 262144:
            raise ContractError("payload_too_large")
    except ContractError:
        raise
    except Exception as error:
        raise ContractError("invalid_contract") from error


def object_schema(properties: dict, required: list[str] | None = None) -> dict:
    return {"type": "object", "properties": properties,
            "required": list(properties) if required is None else required, "additionalProperties": False}


ID = {"type": "string", "minLength": 1, "maxLength": 160, "pattern": r"^[A-Za-z0-9_.:/-]+$"}
TEXT = {"type": "string", "maxLength": 16000}
TIME = {"type": "integer", "minimum": 0, "maximum": 9007199254740991}
EVENT_SCHEMA = object_schema({
    "schema_version": {"const": 1}, "id": ID, "source": ID, "kind": ID,
    "observed_at_ms": TIME, "data": {"type": "object"},
    "quality": {"enum": ["known", "unknown"]},
    "provenance": {"enum": ["synthetic", "human", "model", "sensor", "unverified"]},
    "evidence": {"type": "array", "items": ID, "maxItems": 64, "uniqueItems": True},
})


@dataclass(frozen=True)
class Event:
    id: str
    source: str
    kind: str
    observed_at_ms: int
    data: dict[str, Any] = field(default_factory=dict)
    quality: str = "known"
    provenance: str = "unverified"
    evidence: tuple[str, ...] = ()
    schema_version: int = 1

    def to_dict(self) -> dict:
        value = asdict(self)
        value["evidence"] = list(self.evidence)
        return value

    @classmethod
    def from_dict(cls, value: Mapping) -> Event:
        data = dict(value)
        data.setdefault("schema_version", 1)
        data.setdefault("quality", "known")
        data.setdefault("provenance", "unverified")
        data.setdefault("evidence", [])
        data.setdefault("data", {})
        validate(EVENT_SCHEMA, data)
        data["evidence"] = tuple(data["evidence"])
        return cls(**data)


ARGUMENTS = {"type": "object"}
OBSERVER = object_schema({"source": ID, "kind": ID, "tool": ID, "emits": ID, "arguments": ARGUMENTS},
                         ["source", "kind", "tool", "emits"])
ACTION = object_schema({"on": ID, "tool": ID, "arguments": ARGUMENTS, "emits": ID}, ["on", "tool", "arguments"])
MISSION_SCHEMA = object_schema({
    "schema_version": {"const": 1}, "id": ID, "version": {"type": "integer", "minimum": 1},
    "purpose": {"type": "string", "minLength": 1, "maxLength": 4000},
    "sources": {"type": "object", "minProperties": 1, "maxProperties": 64,
                "propertyNames": ID, "additionalProperties": {"type": "array", "minItems": 1,
                    "maxItems": 32, "uniqueItems": True, "items": ID}},
    "observe": {"type": "array", "items": OBSERVER, "maxItems": 32},
    "remember": object_schema({
        "sources": {"type": "array", "items": ID, "maxItems": 64, "uniqueItems": True},
        "max_items": {"type": "integer", "minimum": 1, "maximum": 1000},
        "max_age_ms": {"type": "integer", "minimum": 0},
    }),
    "condition": object_schema({"type": ID, "config": ARGUMENTS}),
    "act": {"type": "array", "items": ACTION, "maxItems": 32},
    "policy": object_schema({
        "allowed_tools": {"type": "array", "items": ID, "uniqueItems": True, "maxItems": 64},
        "allow_external": {"type": "boolean"},
        "max_pending": {"type": "integer", "minimum": 1, "maximum": 10000},
        "max_concurrency": {"type": "integer", "minimum": 1, "maximum": 32},
        "timeout_ms": {"type": "integer", "minimum": 1, "maximum": 300000},
        "max_event_age_ms": {"type": "integer", "minimum": 0},
        "max_tool_depth": {"type": "integer", "minimum": 1, "maximum": 16},
    }),
})


@dataclass(frozen=True)
class Mission:
    document: dict

    @classmethod
    def from_dict(cls, value: dict) -> Mission:
        value = json.loads(encoded(value))
        if isinstance(value.get("policy"), dict):
            value["policy"].setdefault("max_tool_depth", 8)
        validate(MISSION_SCHEMA, value)
        if not set(value["remember"]["sources"]).issubset(value["sources"]):
            raise ContractError("memory_source_not_allowed")
        if value["remember"]["max_items"] < len(value["remember"]["sources"]):
            raise ContractError("memory_needs_one_slot_per_source")
        for transition in {a["on"] for a in value["act"]}:
            if sum(a["on"] == transition for a in value["act"]) > value["policy"]["max_pending"]:
                raise ContractError("action_fanout_exceeds_capacity")
        for route in value["observe"]:
            count = sum(o["source"] == route["source"] and o["kind"] == route["kind"] for o in value["observe"])
            if count > value["policy"]["max_pending"]:
                raise ContractError("observer_fanout_exceeds_capacity")
        for observer in value["observe"]:
            if observer["kind"] not in value["sources"].get(observer["source"], []):
                raise ContractError("observer_source_not_allowed")
        return cls(value)

    @property
    def key(self) -> str:
        return f'{self.document["id"]}:{self.document["version"]}'


@dataclass(frozen=True)
class MemoryRecord:
    event: Event
    revision: int
    available_at_ms: int


class MemoryView:
    """A fixed, bounded snapshot. Queries cannot widen its source/time scope."""
    def __init__(self, records: list[MemoryRecord]):
        self._records = tuple(records)

    def query(self, *, source: str | None = None, kind: str | None = None,
              limit: int = 20) -> tuple[MemoryRecord, ...]:
        if not 1 <= limit <= 1000:
            raise ContractError("invalid_memory_limit")
        return tuple(r for r in self._records if (source is None or r.event.source == source)
                     and (kind is None or r.event.kind == kind))[-limit:]


@dataclass(frozen=True)
class Evaluation:
    state: dict
    transition: str | None = None
    summary: str = ""
    evidence: tuple[str, ...] = ()
