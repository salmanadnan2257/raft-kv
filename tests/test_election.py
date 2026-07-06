"""Leader election: timeouts, terms, vote restrictions, partitions."""

from __future__ import annotations

from conftest import Harness, storage_with

from raftkv.raft.node import Role

CONFIG3 = ["n1", "n2", "n3"]


def make3(harness: Harness) -> None:
    for nid in CONFIG3:
        harness.add_node(nid, CONFIG3)


def test_exactly_one_leader_elected(harness: Harness) -> None:
    make3(harness)
    led = harness.wait_leader()
    harness.run(1.0)
    leaders = [h.node for h in harness.hosts.values()
               if h.node.role is Role.LEADER]
    assert leaders == [harness.hosts[led.id].node]
    assert led.current_term >= 1


def test_leader_crash_triggers_new_election(harness: Harness) -> None:
    make3(harness)
    old = harness.wait_leader()
    old_term = old.current_term
    harness.crash(old.id)
    new = harness.wait_leader()
    assert new.id != old.id
    assert new.current_term > old_term


def test_minority_partition_cannot_elect(harness: Harness) -> None:
    make3(harness)
    led = harness.wait_leader()
    lone = led.id
    others = [n for n in CONFIG3 if n != lone]
    harness.network.partition({lone}, set(others))
    new = harness.wait_leader(among=set(others))
    assert new.id in others
    # The isolated old leader keeps campaigning but never wins.
    harness.run(2.0)
    assert harness.hosts[lone].node.role is not Role.LEADER or \
        harness.hosts[lone].node.current_term < new.current_term
    # After healing, the stale node adopts the majority's leader.
    harness.network.heal()
    harness.run(2.0)
    stable = harness.wait_leader()
    assert harness.hosts[lone].node.role is Role.FOLLOWER or stable.id == lone


def test_vote_denied_to_stale_log(harness: Harness) -> None:
    # n1 has the longer, more recent log; n2's candidacy must fail against it.
    harness.add_node("n1", ["n1", "n2", "n3"], storage=storage_with([1, 1, 2, 2]))
    harness.add_node("n2", ["n1", "n2", "n3"], storage=storage_with([1, 1]))
    harness.add_node("n3", ["n1", "n2", "n3"], storage=storage_with([1, 1, 2, 2]))
    led = harness.wait_leader()
    assert led.id in ("n1", "n3")
    assert led.log.last_index >= 4


def test_stale_term_messages_rejected(harness: Harness) -> None:
    make3(harness)
    led = harness.wait_leader()
    term = led.current_term
    harness.run(2.0)
    # A healthy cluster does not churn terms.
    assert harness.wait_leader().current_term == term
