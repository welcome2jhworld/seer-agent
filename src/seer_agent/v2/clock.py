"""The replay cutoff. No read past it may influence a commit."""
from __future__ import annotations


class CausalViolation(AssertionError):
    """A read reached past the replay cutoff, or the cutoff moved backwards."""


class CausalClock:
    """A monotone replay cutoff in milliseconds.

    Every reader of frames, evidence, results or the index receives this clock and calls
    `check` with the time its data became readable. The harness advances the cutoff; the
    core never does so on its own, and nothing is precomputed past it.
    """

    def __init__(self, now_ms: int = 0):
        if not isinstance(now_ms, int) or now_ms < 0:
            raise CausalViolation("invalid_cutoff")
        self._now_ms = now_ms

    @property
    def now_ms(self) -> int:
        return self._now_ms

    def advance(self, now_ms: int) -> int:
        if now_ms < self._now_ms:
            raise CausalViolation(f"cutoff_moved_backwards:{self._now_ms}>{now_ms}")
        self._now_ms = now_ms
        return now_ms

    def check(self, available_at_ms: int, what: str) -> None:
        """Assert that data readable from `available_at_ms` may be read at the current cutoff."""
        if available_at_ms > self._now_ms:
            raise CausalViolation(f"read_past_cutoff:{what}:available_at={available_at_ms}:cutoff={self._now_ms}")
