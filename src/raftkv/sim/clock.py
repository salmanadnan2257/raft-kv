"""Deterministic virtual clock.

A single event heap ordered by (time, sequence number). Everything in the
simulator (timers, message deliveries, client behavior, fault injection)
is an event on this heap, so a run is a pure function of the seed.
"""

from __future__ import annotations

import heapq
from typing import Callable


class SimTimer:
    __slots__ = ("when", "seq", "fn", "cancelled")

    def __init__(self, when: float, seq: int, fn: Callable[[], None]) -> None:
        self.when = when
        self.seq = seq
        self.fn = fn
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True

    def __lt__(self, other: "SimTimer") -> bool:
        return (self.when, self.seq) < (other.when, other.seq)


class SimClock:
    def __init__(self) -> None:
        self.now = 0.0
        self._seq = 0
        self._heap: list[SimTimer] = []

    def call_later(self, delay: float, fn: Callable[[], None]) -> SimTimer:
        self._seq += 1
        t = SimTimer(self.now + max(0.0, delay), self._seq, fn)
        heapq.heappush(self._heap, t)
        return t

    def step(self) -> bool:
        """Run the next event. Returns False when the heap is empty."""
        while self._heap:
            t = heapq.heappop(self._heap)
            if t.cancelled:
                continue
            self.now = t.when
            t.fn()
            return True
        return False

    def run_until(self, deadline: float, after_each: Callable[[], None] | None = None) -> None:
        while self._heap and self._heap[0].when <= deadline:
            if self.step() and after_each is not None:
                after_each()
        self.now = max(self.now, deadline)
