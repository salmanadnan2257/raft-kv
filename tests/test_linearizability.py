"""The linearizability checker itself: it must accept legal histories and,
crucially, reject known-bad ones."""

from __future__ import annotations

import math

from raftkv.checker.history import HistoryRecorder, Op
from raftkv.checker.linearizability import Outcome, check_history, check_key
from raftkv.sim.harness import SimConfig, run_simulation


def op(op_id: int, kind: str, invoke: float, complete: float | None,
       result=None, **args) -> Op:
    return Op(op_id, f"c{op_id}", "k", kind, args, invoke,
              math.inf if complete is None else complete, result)


def test_sequential_history_ok() -> None:
    ops = [
        op(0, "put", 0, 1, "ok", value="a"),
        op(1, "get", 2, 3, "a"),
        op(2, "cas", 4, 5, True, expected="a", value="b"),
        op(3, "get", 6, 7, "b"),
        op(4, "delete", 8, 9, "ok"),
        op(5, "get", 10, 11, None),
    ]
    assert check_key("k", ops).ok


def test_concurrent_overlap_ok() -> None:
    # get overlaps the put; either old or new value is legal.
    assert check_key("k", [
        op(0, "put", 0, 10, "ok", value="a"),
        op(1, "get", 5, 6, None),
    ]).ok
    assert check_key("k", [
        op(0, "put", 0, 10, "ok", value="a"),
        op(1, "get", 5, 6, "a"),
    ]).ok


def test_stale_read_is_violation() -> None:
    # put completed strictly before the get began, yet the get missed it.
    ops = [
        op(0, "put", 0, 1, "ok", value="a"),
        op(1, "get", 2, 3, None),
    ]
    result = check_key("k", ops)
    assert result.outcome is Outcome.VIOLATION


def test_value_from_nowhere_is_violation() -> None:
    ops = [op(0, "get", 0, 1, "ghost")]
    assert check_key("k", ops).outcome is Outcome.VIOLATION


def test_lost_update_is_violation() -> None:
    # Two non-overlapping cas from the same expected value both succeed:
    # the second should have observed the first's write.
    ops = [
        op(0, "cas", 0, 1, True, expected=None, value="a"),
        op(1, "cas", 2, 3, True, expected=None, value="b"),
    ]
    assert check_key("k", ops).outcome is Outcome.VIOLATION


def test_incomplete_op_may_or_may_not_apply() -> None:
    pending_put = [
        op(0, "put", 0, None, None, value="a"),  # no response ever
        op(1, "get", 5, 6, "a"),
    ]
    assert check_key("k", pending_put).ok
    pending_put_unseen = [
        op(0, "put", 0, None, None, value="a"),
        op(1, "get", 5, 6, None),
    ]
    assert check_key("k", pending_put_unseen).ok
    # But an incomplete put cannot excuse an impossible value.
    impossible = [
        op(0, "put", 0, None, None, value="a"),
        op(1, "get", 5, 6, "b"),
    ]
    assert check_key("k", impossible).outcome is Outcome.VIOLATION


def test_known_bad_interleaving_from_paper_shape() -> None:
    """Reads on both sides of a write disagree with real-time order."""
    ops = [
        op(0, "put", 0, 1, "ok", value="1"),
        op(1, "cas", 2, 3, True, expected="1", value="2"),
        op(2, "get", 4, 5, "1"),  # after the cas completed: stale
    ]
    assert check_key("k", ops).outcome is Outcome.VIOLATION


def test_window_limit_reports_unknown() -> None:
    ops = [op(i, "put", 0, None, None, value=str(i)) for i in range(64)]
    assert check_key("k", ops).outcome is Outcome.UNKNOWN


def test_budget_exhaustion_reports_unknown() -> None:
    # A violation buried under 20 fully-concurrent completed puts forces an
    # exhaustive search that a tiny budget cannot finish.
    ops = [op(i, "put", 0, 100, "ok", value=str(i)) for i in range(20)]
    ops.append(op(99, "get", 200, 201, "phantom"))
    assert check_key("k", ops, max_states=100).outcome is Outcome.UNKNOWN


def test_per_key_partitioning() -> None:
    rec = HistoryRecorder()
    a = rec.invoke("c1", "a", "put", {"value": "1"}, 0)
    rec.complete(a, "ok", 1)
    b = rec.invoke("c1", "b", "get", {}, 2)
    rec.complete(b, None, 3)  # key b never written: None is correct
    results = check_history(rec.by_key())
    assert [r.key for r in results] == ["a", "b"]
    assert all(r.ok for r in results)


def test_chaos_history_from_simulation_is_linearizable() -> None:
    result = run_simulation(SimConfig(seed=7))
    assert result.ok
    assert result.ops_completed > 20
    assert all(r.outcome is not Outcome.VIOLATION for r in result.linearizability)
