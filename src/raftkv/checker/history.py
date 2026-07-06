"""Concurrent operation histories for the linearizability checker.

A history is a set of operations, each with an invoke timestamp and,
if the client saw a response, a completion timestamp and result.
Operations whose response never arrived (client gave up, node crashed)
stay incomplete: they may or may not have taken effect, and the checker
must consider both possibilities.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Op:
    op_id: int
    client: str
    key: str
    kind: str                      # "get" | "put" | "delete" | "cas"
    args: dict[str, Any]
    invoke: float
    complete: float = math.inf     # inf while no response observed
    result: Any = None             # get: value|None, put/delete: "ok", cas: bool

    @property
    def is_complete(self) -> bool:
        return self.complete != math.inf


@dataclass
class HistoryRecorder:
    ops: list[Op] = field(default_factory=list)
    _next_id: int = 0

    def invoke(self, client: str, key: str, kind: str,
               args: dict[str, Any], now: float) -> Op:
        op = Op(self._next_id, client, key, kind, args, invoke=now)
        self._next_id += 1
        self.ops.append(op)
        return op

    def complete(self, op: Op, result: Any, now: float) -> None:
        op.complete = now
        op.result = result

    def by_key(self) -> dict[str, list[Op]]:
        out: dict[str, list[Op]] = {}
        for op in self.ops:
            out.setdefault(op.key, []).append(op)
        return out
