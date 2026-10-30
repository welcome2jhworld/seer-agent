"""Workers return data and their cost. They never write state."""
from __future__ import annotations

from typing import Protocol

from .records import InferenceRequest, InferenceResult


class Worker(Protocol):
    model_id: str

    def infer(self, request: InferenceRequest) -> InferenceResult: ...


class FakeWorker:
    """A scripted worker for tests and replay.

    `script` maps a requested interval `(from_ms, to_ms)` to `(latency_ms, scores)` or
    `(latency_ms, scores, payload_overrides)`; `default` serves every other request. The worker
    counts its calls so a test can prove that a restart replayed a stored result instead of
    running inference again.
    """

    def __init__(self, script: dict[tuple[int, int], tuple] | None = None, *,
                 default: tuple | None = None, model_id: str = "fake-verify-1"):
        self.script, self.default, self.model_id = dict(script or {}), default, model_id
        self.calls = 0
        self.requests: list[InferenceRequest] = []

    def infer(self, request: InferenceRequest) -> InferenceResult:
        self.calls += 1
        self.requests.append(request)
        entry = self.script.get((request.from_ms, request.to_ms), self.default)
        if entry is None:
            raise LookupError(f"no_scripted_result:{request.from_ms}:{request.to_ms}")
        latency_ms, scores, *rest = entry
        payload = {"job_id": request.job_id, "attempt": request.attempt, "model_id": self.model_id,
                   "predicate": request.predicate, "score_kind": "prob", "scores": dict(scores)}
        payload.update(rest[0] if rest else {})
        return InferenceResult(request.job_id, request.attempt, self.model_id, int(latency_ms), payload)
