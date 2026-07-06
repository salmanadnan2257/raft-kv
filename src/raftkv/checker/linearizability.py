"""Linearizability checker for single-key register histories.

Algorithm: Wing & Gong's exhaustive search with Lowe's memoization.
At each step we may linearize any operation that is "minimal": no other
un-linearized operation completed before it was invoked (real-time order
must be respected). Applying the operation to a model register must
reproduce the result the client actually observed. Incomplete operations
(no response) branch two ways: they took effect at some point, or they
never did. Visited (linearized-set, register-value) pairs are memoized;
Lowe showed this prunes the exponential blowup dramatically in practice.

Honest limits: checking linearizability is NP-complete in general
(P-complete for a fixed number of processes is not the relevant bound here;
the search is worst-case exponential in concurrent width). We keep it
tractable by checking per key (KV keys are independent registers), by
memoizing, and by a node budget that reports UNKNOWN instead of hanging on
pathological histories. Chaos-run histories in this project stay well
inside the budget.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum

from .history import Op

_ABSENT = None  # model register value for "key not present"


class Outcome(Enum):
    OK = "ok"
    VIOLATION = "violation"
    UNKNOWN = "unknown"  # search budget exhausted


@dataclass
class CheckResult:
    outcome: Outcome
    key: str
    explored: int
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.outcome is Outcome.OK


def _step(value: str | None, op: Op) -> tuple[bool, str | None]:
    """Apply op to the model register. Returns (result_matches, new_value).

    For incomplete ops any outcome is acceptable, so only the state
    transition matters.
    """
    if op.kind == "get":
        ok = (not op.is_complete) or op.result == value
        return ok, value
    if op.kind == "put":
        return True, op.args["value"]
    if op.kind == "delete":
        return True, _ABSENT
    if op.kind == "cas":
        swapped = value == op.args["expected"]
        new_value = op.args["value"] if swapped else value
        ok = (not op.is_complete) or op.result == swapped
        return ok, new_value
    raise ValueError(f"unknown op kind: {op.kind}")


def check_key(key: str, ops: list[Op], max_states: int = 2_000_000) -> CheckResult:
    """Check one key's history. Initial register state is absent (None)."""
    ops = sorted(ops, key=lambda o: o.invoke)
    n = len(ops)
    if n == 0:
        return CheckResult(Outcome.OK, key, 0)
    if n > 63:
        # Bitmask-based memoization; split longer histories before checking.
        return CheckResult(Outcome.UNKNOWN, key, 0,
                           f"history too long for checker window ({n} ops)")
    complete_mask = 0
    for i, op in enumerate(ops):
        if op.is_complete:
            complete_mask |= 1 << i

    seen: set[tuple[int, str | None]] = set()
    explored = 0
    # Depth-first search over (linearized-mask, register value).
    stack: list[tuple[int, str | None]] = [(0, _ABSENT)]
    seen.add((0, _ABSENT))
    while stack:
        mask, value = stack.pop()
        if mask & complete_mask == complete_mask:
            # Every completed op linearized; leftovers are incomplete ops
            # we are free to declare "never happened".
            return CheckResult(Outcome.OK, key, explored)
        # An op is minimal if no un-linearized op completed before its invoke.
        min_complete = math.inf
        for i, op in enumerate(ops):
            if not mask & (1 << i):
                min_complete = min(min_complete, op.complete)
        for i, op in enumerate(ops):
            bit = 1 << i
            if mask & bit or op.invoke > min_complete:
                continue
            matches, new_value = _step(value, op)
            if not matches:
                continue
            state = (mask | bit, new_value)
            if state not in seen:
                seen.add(state)
                explored += 1
                if explored > max_states:
                    return CheckResult(Outcome.UNKNOWN, key, explored,
                                       "state budget exhausted")
                stack.append(state)
    return CheckResult(
        Outcome.VIOLATION, key, explored,
        f"no linearization of {n} ops on key {key!r} matches observed results")


def check_history(ops_by_key: dict[str, list[Op]],
                  max_states: int = 2_000_000) -> list[CheckResult]:
    """Check every key independently (keys are independent registers)."""
    return [check_key(k, ops, max_states) for k, ops in sorted(ops_by_key.items())]
