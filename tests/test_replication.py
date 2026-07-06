"""Log replication and repair: the paper's Figure 7 follower states.

A leader for term 8 holds the log [1,1,1,4,4,5,5,6,6,6] (terms by index).
Each scenario pairs it with one follower in the states (a)-(f) from the
figure and asserts the follower's log converges to the leader's, with
missing entries filled in and conflicting suffixes truncated.
"""

from __future__ import annotations

import pytest
from conftest import Harness, storage_with

from raftkv.raft.node import RaftNode, Role

LEADER_TERMS = [1, 1, 1, 4, 4, 5, 5, 6, 6, 6]

FIGURE7 = {
    "a_missing_entry": [1, 1, 1, 4, 4, 5, 5, 6, 6],
    "b_missing_many": [1, 1, 1, 4],
    "c_extra_entry": [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6],
    "d_extra_entries": [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7],
    "e_missing_and_extra": [1, 1, 1, 4, 4, 4, 4],
    "f_divergent_history": [1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3],
}


def force_leader(node: RaftNode, term: int) -> None:
    """Install a node as leader directly (unit-test shortcut past voting)."""
    node.current_term = term
    node.voted_for = node.id
    node.storage.save_term_vote(term, node.id)
    node._become_leader()


@pytest.mark.parametrize("name,follower_terms", FIGURE7.items(), ids=FIGURE7)
def test_figure7_convergence(name: str, follower_terms: list[int]) -> None:
    harness = Harness(seed=hash(name) % 1000)
    config = ["n1", "n2"]
    lead = harness.add_node("n1", config, storage=storage_with(LEADER_TERMS, 7),
                            election_timeout=0.15)
    foll = harness.add_node("n2", config, storage=storage_with(follower_terms),
                            election_timeout=1000.0)  # never campaigns
    force_leader(lead, 8)
    harness.run(3.0)

    assert lead.role is Role.LEADER and lead.current_term == 8
    # Leader appended its term-8 no-op at index 11.
    assert lead.log.last_index == 11
    assert foll.log.entries == lead.log.entries
    assert foll.commit_index == lead.commit_index == 11
    # State machines applied identical commands.
    assert harness.hosts["n2"].machine.data == harness.hosts["n1"].machine.data


def test_leader_never_overwrites_own_entries() -> None:
    harness = Harness(seed=3)
    config = ["n1", "n2"]
    lead = harness.add_node("n1", config, storage=storage_with(LEADER_TERMS, 7))
    harness.add_node("n2", config,
                     storage=storage_with(FIGURE7["d_extra_entries"]),
                     election_timeout=1000.0)
    before = list(lead.log.entries)
    force_leader(lead, 8)
    harness.run(3.0)
    assert lead.log.entries[:10] == before  # appended, never rewritten


def test_commit_only_with_current_term_entry() -> None:
    """Section 5.4.2: a leader may not count replicas to commit entries
    from older terms; its no-op commits them transitively."""
    harness = Harness(seed=4)
    config = ["n1", "n2", "n3"]
    lead = harness.add_node("n1", config, storage=storage_with([1, 1]))
    harness.add_node("n2", config, storage=storage_with([1]),
                     election_timeout=1000.0)
    harness.add_node("n3", config, storage=storage_with([1]),
                     election_timeout=1000.0)
    force_leader(lead, 2)
    assert lead.commit_index == 0  # nothing commits before replication
    harness.run(2.0)
    # No-op at index 3 (term 2) committed, pulling indexes 1-2 with it.
    assert lead.commit_index == 3
    for nid in config:
        assert harness.hosts[nid].node.commit_index == 3


def test_replication_under_message_loss() -> None:
    harness = Harness(seed=5, drop_prob=0.3, dup_prob=0.2)
    config = ["n1", "n2", "n3"]
    for nid in config:
        harness.add_node(nid, config)
    led = harness.wait_leader()
    for i in range(20):
        harness.put(f"key{i}", f"v{i}")
    harness.run(3.0)
    led = harness.wait_leader()
    for h in harness.hosts.values():
        assert h.node.commit_index >= 20
        assert h.machine.data == harness.hosts[led.id].machine.data
