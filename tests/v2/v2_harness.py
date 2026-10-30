"""Synthetic fixtures for the v2 causal replay core. One camera, one session, no footage."""
from __future__ import annotations

from seer_agent.v2 import INSUFFICIENT, BoundedSource, CausalClock, Frame, ReplayCore, ReplayStore, Watch

WATCH = Watch("leak-a", "cam-a", "liquid_on_floor", ("liquid", "shadow", INSUFFICIENT),
              threshold=0.6, margin=0.2, freshness_ms=30_000)
SHADOW = {"liquid": 0.05, "shadow": 0.9, INSUFFICIENT: 0.05}
LIQUID = {"liquid": 0.9, "shadow": 0.05, INSUFFICIENT: 0.05}


def frame(sequence: int, *, captured: int | None = None, published: int | None = None, size: int = 100) -> Frame:
    """Frame `n` is captured at n*500 ms and, unless said otherwise, published at the same instant."""
    captured = sequence * 500 if captured is None else captured
    return Frame("cam-a", "s1", sequence, captured, captured if published is None else published, size, f"frame-{sequence}")


class Harness:
    def __init__(self, path, worker, *, now: int = 0, budget: int = 1 << 20, period: int = 500,
                 retention: int = 10_000, capacity: int = 64):
        self.path, self.worker, self.budget, self.period, self.retention = path, worker, budget, period, retention
        self.clock = CausalClock(now)
        self.store = ReplayStore(path, self.clock)
        self.core = ReplayCore(self.store, worker, pin_budget_bytes=budget, period_ms=period, retention_ms=retention)
        self.source = BoundedSource("cam-a", "s1", capacity=capacity, period_ms=period, retention_ms=retention)
        self.core.attach(self.source)
        self.core.register_watch(WATCH)

    def offer(self, *frames: Frame) -> None:
        for item in frames:
            self.source.offer(item)

    def run_until(self, until_ms: int, step_ms: int = 500) -> None:
        """Advance the cutoff in steps, ticking the core at each step and at the end."""
        for now in range(self.core.now, until_ms + 1, step_ms):
            self.core.advance(now)
            self.core.tick()
        if self.core.now < until_ms:
            self.core.advance(until_ms)
            self.core.tick()

    def restart(self, now_ms: int) -> ReplayCore:
        """Close the store and open a new owner on the same file, keeping the same fake worker."""
        self.store.close()
        self.clock = CausalClock(now_ms)
        self.store = ReplayStore(self.path, self.clock)
        self.core = ReplayCore(self.store, self.worker, pin_budget_bytes=self.budget, period_ms=self.period,
                               retention_ms=self.retention)
        self.core.attach(self.source)
        return self.core
