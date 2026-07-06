"""Environment abstraction that lets the same Raft code run on a real
asyncio event loop or inside the deterministic simulator.

The Raft node never touches wall-clock time, asyncio, sockets, or the global
random module. Everything side-effectful goes through this interface.
"""

from __future__ import annotations

import random
from typing import Callable, Protocol

from .messages import Message


class TimerHandle(Protocol):
    def cancel(self) -> None: ...


class Env(Protocol):
    rng: random.Random

    def now(self) -> float: ...
    def call_later(self, delay: float, fn: Callable[[], None]) -> TimerHandle: ...
    def send(self, dest: str, msg: Message) -> None: ...
