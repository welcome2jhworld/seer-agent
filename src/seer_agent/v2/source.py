"""A bounded frame source. Frames become readable at their publish time and leave the ring
when it is full or their retention ends, unless the core pinned them."""
from __future__ import annotations

from ..contracts import ContractError
from .clock import CausalClock
from .records import Frame


class EvidenceLost(LookupError):
    """The frame is no longer readable: evicted, past retention, or never held."""


class BoundedSource:
    """Ring buffer of published frames for one source session.

    `offer` accepts frames in any order and drops exact duplicates of frames still in the ring.
    `readable` lists frames only after their publish time, until the core acknowledges them after
    a durable commit. `read` refuses any frame not yet published and reports frames that left the
    ring before they were pinned. Nothing here advances the clock or writes state.
    """

    def __init__(self, source: str, session: str, *, capacity: int = 64, period_ms: int = 500,
                 retention_ms: int = 10_000):
        if capacity < 1 or period_ms < 1 or retention_ms < period_ms:
            raise ContractError("invalid_source_bounds")
        self.source, self.session = source, session
        self.capacity, self.period_ms, self.retention_ms = capacity, period_ms, retention_ms
        self._frames: dict[int, Frame] = {}
        self._delivered: set[int] = set()
        self._pinned: set[int] = set()
        self.duplicates = 0
        self.evictions = 0

    def __len__(self) -> int:
        return len(self._frames)

    def offer(self, frame: Frame) -> str:
        if (frame.source, frame.session) != (self.source, self.session):
            raise ContractError("frame_from_another_session")
        known = self._frames.get(frame.sequence)
        if known is not None:
            if known != frame:
                raise ContractError("frame_identity_conflict")
            self.duplicates += 1
            return "duplicate"
        self._frames[frame.sequence] = frame
        while len(self._frames) > self.capacity:
            unpinned = [s for s in self._frames if s not in self._pinned]
            if not unpinned:
                raise ContractError("ring_full_of_pinned_frames")
            oldest = min(unpinned, key=lambda s: (self._frames[s].published_ms, s))
            del self._frames[oldest]
            self._delivered.discard(oldest)
            self.evictions += 1
        return "accepted"

    def readable(self, clock: CausalClock) -> list[Frame]:
        """Frames published at or before the cutoff and not yet acknowledged, in capture order."""
        return sorted((f for s, f in self._frames.items()
                       if s not in self._delivered and f.published_ms <= clock.now_ms),
                      key=lambda f: (f.captured_ms, f.sequence))

    def acknowledge(self, sequence: int) -> None:
        """The core admitted the frame durably; it is not listed again."""
        self._delivered.add(sequence)

    def read(self, sequence: int, clock: CausalClock) -> Frame:
        frame = self._frames.get(sequence)
        if frame is None:
            raise EvidenceLost(f"{self.source}:{self.session}:{sequence}")
        clock.check(frame.published_ms, f"frame:{frame.handle}")
        if sequence not in self._pinned and clock.now_ms > frame.published_ms + self.retention_ms:
            raise EvidenceLost(frame.handle)
        return frame

    def pin(self, sequence: int) -> bool:
        if sequence not in self._frames:
            return False
        self._pinned.add(sequence)
        return True

    def release(self, sequence: int) -> None:
        self._pinned.discard(sequence)
