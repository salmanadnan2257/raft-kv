"""Linearizable reads via the ReadIndex protocol."""

from __future__ import annotations

from conftest import Harness

CONFIG3 = ["n1", "n2", "n3"]


def make3(harness: Harness) -> Harness:
    for nid in CONFIG3:
        harness.add_node(nid, CONFIG3)
    return harness


def test_read_sees_latest_committed_write(harness: Harness) -> None:
    make3(harness)
    led = harness.wait_leader()
    harness.put("k", "v1")
    got: list[int | None] = []
    led.read_index(got.append)
    harness.run(1.0)
    assert got and got[0] is not None
    assert harness.hosts[led.id].machine.get("k") == "v1"


def test_follower_refuses_read(harness: Harness) -> None:
    make3(harness)
    led = harness.wait_leader()
    follower = next(h.node for h in harness.hosts.values() if h.node is not led)
    got: list[int | None] = []
    follower.read_index(got.append)
    assert got == [None]


def test_stale_leader_cannot_serve_reads(harness: Harness) -> None:
    """A leader cut off from the majority must not confirm a ReadIndex:
    without the heartbeat quorum it could serve stale data."""
    make3(harness)
    old = harness.wait_leader()
    others = set(CONFIG3) - {old.id}
    harness.network.partition({old.id}, others)
    harness.run(0.5)
    got: list[int | None] = []
    old.read_index(got.append)
    harness.run(2.0)
    # Majority side moved on and wrote a newer value.
    new = harness.wait_leader(among=others)
    assert new.id != old.id
    harness.put("k", "fresh")
    # The stale leader never confirmed the read while partitioned; at most
    # it failed the read when it stepped down.
    assert got == [] or got == [None]
    harness.network.heal()
    harness.run(2.0)
    assert got == [None]  # step-down failed the pending read explicitly


def test_read_after_failover_sees_prior_writes(harness: Harness) -> None:
    make3(harness)
    led = harness.wait_leader()
    harness.put("durable", "yes")
    harness.crash(led.id)
    new = harness.wait_leader()
    got: list[int | None] = []
    new.read_index(got.append)
    harness.run(1.0)
    assert got and got[0] is not None
    assert harness.hosts[new.id].machine.get("durable") == "yes"
