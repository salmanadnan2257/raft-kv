"""Simulated network transport.

Implements the same send/deliver contract as the TCP transport, but every
message is an event on the virtual clock, with seeded random delay, loss,
duplication, and partition filtering. Partitions are checked at delivery
time, so a partition that forms while a message is "in flight" drops it,
which mirrors a TCP connection dying mid-send.
"""

from __future__ import annotations

import random
from typing import Callable

from ..raft.messages import Message
from .clock import SimClock, SimTimer

Handler = Callable[[str, Message], None]


class SimNetwork:
    def __init__(self, clock: SimClock, rng: random.Random, *,
                 min_delay: float = 0.001, max_delay: float = 0.010,
                 drop_prob: float = 0.0, dup_prob: float = 0.0) -> None:
        self.clock = clock
        self.rng = rng
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.drop_prob = drop_prob
        self.dup_prob = dup_prob
        self._handlers: dict[str, Handler] = {}
        self._partitions: set[frozenset[str]] = set()
        self.messages_sent = 0
        self.messages_dropped = 0

    def register(self, node_id: str, handler: Handler) -> None:
        self._handlers[node_id] = handler

    def unregister(self, node_id: str) -> None:
        self._handlers.pop(node_id, None)

    # ---------------------------------------------------------- partitions

    def partition(self, group_a: set[str], group_b: set[str]) -> None:
        for a in group_a:
            for b in group_b:
                if a != b:
                    self._partitions.add(frozenset((a, b)))

    def heal(self) -> None:
        self._partitions.clear()

    def is_partitioned(self, a: str, b: str) -> bool:
        return frozenset((a, b)) in self._partitions

    # ------------------------------------------------------------- sending

    def send(self, src: str, dest: str, msg: Message) -> None:
        self.messages_sent += 1
        if self.rng.random() < self.drop_prob:
            self.messages_dropped += 1
            return
        copies = 2 if self.rng.random() < self.dup_prob else 1
        for _ in range(copies):
            delay = self.rng.uniform(self.min_delay, self.max_delay)
            self.clock.call_later(delay, lambda: self._deliver(src, dest, msg))

    def _deliver(self, src: str, dest: str, msg: Message) -> None:
        handler = self._handlers.get(dest)
        if handler is None or self.is_partitioned(src, dest):
            self.messages_dropped += 1
            return
        handler(src, msg)


class SimEnv:
    """Per-node Env implementation over the shared clock and network.

    timer_scale simulates clock skew: a node whose clock runs fast fires
    its election timers early (scale < 1) or late (scale > 1).
    """

    def __init__(self, node_id: str, clock: SimClock, network: SimNetwork,
                 rng: random.Random) -> None:
        self.node_id = node_id
        self.clock = clock
        self.network = network
        self.rng = rng
        self.timer_scale = 1.0
        self._epoch = 0
        self._live_timers: list[SimTimer] = []

    def now(self) -> float:
        return self.clock.now

    def call_later(self, delay: float, fn: Callable[[], None]) -> SimTimer:
        epoch = self._epoch

        def guarded() -> None:
            if epoch == self._epoch:
                fn()

        t = self.clock.call_later(delay * self.timer_scale, guarded)
        self._live_timers.append(t)
        if len(self._live_timers) > 64:
            self._live_timers = [x for x in self._live_timers if not x.cancelled]
        return t

    def send(self, dest: str, msg: Message) -> None:
        self.network.send(self.node_id, dest, msg)

    def kill(self) -> None:
        """Invalidate all outstanding timers (simulated crash)."""
        self._epoch += 1
        for t in self._live_timers:
            t.cancel()
        self._live_timers.clear()
