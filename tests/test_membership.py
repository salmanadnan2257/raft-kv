"""Single-server membership changes: add, remove, remove-the-leader."""

from __future__ import annotations

import pytest
from conftest import Harness

from raftkv.raft.node import ConfigChangeInProgress, Role

CONFIG3 = ["n1", "n2", "n3"]


def make3(harness: Harness) -> None:
    for nid in CONFIG3:
        harness.add_node(nid, CONFIG3)


def test_add_server(harness: Harness) -> None:
    make3(harness)
    led = harness.wait_leader()
    harness.put("before", "add")
    # A joining node starts with an empty config: it never campaigns and
    # learns the cluster through replication.
    harness.add_node("n4", [])
    index, _ = led.add_server("n4")
    harness.wait_commit(led.id, index)
    harness.run(2.0)
    for nid in [*CONFIG3, "n4"]:
        assert set(harness.hosts[nid].node.config) == {"n1", "n2", "n3", "n4"}
    assert harness.hosts["n4"].machine.data.get("before") == "add"
    # The 4-node cluster keeps working.
    harness.put("after", "add")
    harness.run(1.0)
    assert harness.hosts["n4"].machine.data.get("after") == "add"


def test_remove_follower(harness: Harness) -> None:
    make3(harness)
    led = harness.wait_leader()
    victim = [n for n in CONFIG3 if n != led.id][0]
    index, _ = led.remove_server(victim)
    harness.wait_commit(led.id, index)
    harness.run(1.0)
    assert set(led.config) == set(CONFIG3) - {victim}
    # The removed node cannot start elections (it is not in its own config).
    harness.run(2.0)
    assert harness.hosts[victim].node.role is not Role.LEADER
    # Two-node cluster still commits.
    harness.put("still", "works")


def test_remove_leader_steps_down(harness: Harness) -> None:
    make3(harness)
    led = harness.wait_leader()
    index, _ = led.remove_server(led.id)
    harness.wait_commit(led.id, index)
    harness.run(2.0)
    assert led.role is not Role.LEADER
    new = harness.wait_leader()
    assert new.id != led.id
    assert set(new.config) == set(CONFIG3) - {led.id}


def test_one_change_at_a_time(harness: Harness) -> None:
    make3(harness)
    led = harness.wait_leader()
    harness.add_node("n4", [])
    harness.add_node("n5", [])
    led.add_server("n4")
    with pytest.raises(ConfigChangeInProgress):
        led.add_server("n5")  # first change not yet committed


def test_add_rejects_duplicate(harness: Harness) -> None:
    make3(harness)
    led = harness.wait_leader()
    with pytest.raises(ValueError):
        led.add_server("n2")
    with pytest.raises(ValueError):
        led.remove_server("nx")
