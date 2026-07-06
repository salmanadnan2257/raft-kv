"""Deterministic simulation: reproducibility, invariant enforcement, and
seed sweeps.

The long sweep is documented in docs/TESTING.md:

    raftkv sim --seeds 5000 --workers 8
"""

from __future__ import annotations

import pytest

from raftkv.raft.node import RaftNode
from raftkv.sim.harness import (InvariantViolation, SimConfig, run_simulation)


def _fingerprint(result) -> tuple:
    return (
        result.messages_sent,
        result.ops_completed,
        tuple((o.client, o.key, o.kind, o.invoke, o.complete, repr(o.result))
              for o in result.history.ops),
    )


def test_same_seed_same_run() -> None:
    a = run_simulation(SimConfig(seed=1234))
    b = run_simulation(SimConfig(seed=1234))
    assert _fingerprint(a) == _fingerprint(b)


def test_different_seeds_differ() -> None:
    a = run_simulation(SimConfig(seed=1))
    b = run_simulation(SimConfig(seed=2))
    assert _fingerprint(a) != _fingerprint(b)


def test_chaos_produces_faults_and_progress() -> None:
    result = run_simulation(SimConfig(seed=99))
    assert result.crashes + result.partitions > 0
    assert result.ops_completed > 20
    assert result.terms_seen >= 1


def test_quick_seed_sweep() -> None:
    for seed in range(25):
        result = run_simulation(SimConfig(seed=seed))
        assert result.ok, f"seed {seed} failed"


def test_broken_raft_is_caught(monkeypatch) -> None:
    """Sanity-check the harness has teeth: weaken quorum so split brain is
    possible and confirm an invariant violation fires, naming the seed."""
    monkeypatch.setattr(RaftNode, "_majority",
                        lambda self: max(1, len(self.config) // 2))
    caught = None
    for seed in range(40):
        try:
            run_simulation(SimConfig(seed=seed))
        except InvariantViolation as exc:
            caught = exc
            break
    assert caught is not None, "weakened quorum was never detected"
    assert "seed=" in str(caught)


@pytest.mark.slow
def test_seed_batch_200() -> None:
    failures = []
    for seed in range(200):
        try:
            result = run_simulation(SimConfig(seed=seed))
            if not result.ok:
                failures.append(seed)
        except InvariantViolation as exc:
            failures.append((seed, str(exc)))
    assert not failures, failures
