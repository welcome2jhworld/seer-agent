"""Typed values of the causal replay core. They carry data; only ReplayCore commits them."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..contracts import ID, ContractError, object_schema

COVERAGE_LEVELS = ("captured", "signal", "inspected")
UNKNOWN = "unknown"
INSUFFICIENT = "insufficient_visibility"
SCORE_KINDS = ("raw", "logit", "prob", "none")
# Fixed scheduler classes (architecture section 7). A model cannot change them.
PRIORITIES = {"reserved_coverage": 1, "expiring_evidence": 2, "due_intention": 3,
              "discriminating_check": 4, "discover": 5, "housekeeping": 6}


@dataclass(frozen=True)
class Frame:
    """One captured frame.

    `captured_ms` is the capture instant and `published_ms` the time the frame became readable.
    A frame stands for the sampling period that ends at its capture instant, so the end of
    every interval it covers is at or before its publish time (end <= available_at).
    """
    source: str
    session: str
    sequence: int
    captured_ms: int
    published_ms: int
    size_bytes: int = 1
    content: str = ""

    def __post_init__(self):
        if self.sequence < 0 or self.captured_ms < 0 or self.size_bytes < 1:
            raise ContractError("invalid_frame")
        if self.published_ms < self.captured_ms:
            raise ContractError("published_before_captured")

    @property
    def handle(self) -> str:
        return f"{self.source}:{self.session}:{self.sequence}"


@dataclass(frozen=True)
class Watch:
    """A fixed predicate with a candidate set compiled before any inference runs."""
    id: str
    source: str
    predicate: str
    candidates: tuple[str, ...]
    revision: int = 1
    mission_version: int = 1
    threshold: float = 0.6
    margin: float = 0.2
    freshness_ms: int = 30_000
    region: str = "full"

    def __post_init__(self):
        if len(self.candidates) < 2 or len(set(self.candidates)) != len(self.candidates):
            raise ContractError("invalid_candidate_set")
        if INSUFFICIENT not in self.candidates:
            raise ContractError("candidates_must_include_insufficient_visibility")
        if self.revision < 1 or self.mission_version < 1 or self.freshness_ms < 0:
            raise ContractError("invalid_watch")

    @property
    def merge_key(self) -> str:
        """Situations merge on region x predicate per source; time extends the active interval."""
        return f"{self.source}:{self.region}:{self.predicate}"


@dataclass(frozen=True)
class InferenceRequest:
    job_id: str
    attempt: int
    predicate: str
    candidates: tuple[str, ...]
    handles: tuple[str, ...]
    from_ms: int
    to_ms: int
    deadline_ms: int


@dataclass(frozen=True)
class InferenceResult:
    """What a worker returns: data and its own cost. Nothing here is committed until reduced."""
    job_id: str
    attempt: int
    model_id: str
    latency_ms: int
    payload: dict[str, Any]


RESULT_SCHEMA = object_schema({
    "job_id": ID, "attempt": {"type": "integer", "minimum": 1}, "model_id": ID, "predicate": ID,
    "score_kind": {"enum": list(SCORE_KINDS)},
    "scores": {"type": "object", "minProperties": 2, "maxProperties": 32, "propertyNames": ID,
               "additionalProperties": {"type": "number"}},
})


def ranked(scores: dict[str, float]) -> list[str]:
    return sorted(scores, key=lambda name: (-scores[name], name))


def pick(scores: dict[str, float], threshold: float) -> str:
    """CPU threshold rule: the best candidate when it clears the threshold, otherwise unknown."""
    best = ranked(scores)[0]
    return best if scores[best] >= threshold else UNKNOWN


@dataclass(frozen=True)
class Trace:
    """One line from a source frame range to a committed decision. Column order matches the table."""
    job_id: str
    attempt: int
    source: str
    session: str
    sequence_from: int
    sequence_to: int
    mission_version: int
    watch_revision: int
    context_version: int
    owner_epoch: int
    model_id: str
    queue_ms: int
    inference_ms: int
    manifest_ref: str
    outcome: str
    estimate_id: str | None
    reduced_ms: int
