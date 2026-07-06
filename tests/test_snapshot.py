"""Log compaction and InstallSnapshot catch-up."""

from __future__ import annotations

from conftest import Harness

CONFIG3 = ["n1", "n2", "n3"]


def test_snapshot_taken_and_log_compacted() -> None:
    harness = Harness(seed=10)
    for nid in CONFIG3:
        harness.add_node(nid, CONFIG3, snapshot_threshold=10)
    harness.wait_leader()
    for i in range(25):
        harness.put(f"k{i}", f"v{i}")
    harness.run(1.0)
    led = harness.wait_leader()
    assert led.log.snapshot_index >= 10
    assert len(led.log.entries) < 25
    # Restarting from disk restores through the snapshot.
    st = harness.hosts[led.id].storage.load()
    assert st.snapshot.last_index == led.log.snapshot_index


def test_lagging_follower_catches_up_via_install_snapshot() -> None:
    harness = Harness(seed=11)
    for nid in CONFIG3:
        harness.add_node(nid, CONFIG3, snapshot_threshold=10)
    led = harness.wait_leader()
    laggard = [n for n in CONFIG3 if n != led.id][0]
    harness.network.partition({laggard}, set(CONFIG3) - {laggard})
    for i in range(30):
        harness.put(f"k{i}", f"v{i}")
    led = harness.wait_leader()
    assert led.log.snapshot_index > 0  # compacted past the laggard's log
    harness.network.heal()
    harness.run(3.0)
    lag_node = harness.hosts[laggard].node
    assert lag_node.log.snapshot_index >= 10  # arrived via InstallSnapshot
    assert lag_node.commit_index == led.commit_index
    assert harness.hosts[laggard].machine.data == harness.hosts[led.id].machine.data
    # Session table rode along inside the snapshot.
    assert harness.hosts[laggard].machine.sessions == \
        harness.hosts[led.id].machine.sessions


def test_restart_from_snapshot_replays_correctly() -> None:
    harness = Harness(seed=12)
    for nid in CONFIG3:
        harness.add_node(nid, CONFIG3, snapshot_threshold=10)
    led = harness.wait_leader()
    for i in range(25):
        harness.put(f"k{i}", f"v{i}")
    victim = [n for n in CONFIG3 if n != led.id][0]
    storage = harness.hosts[victim].storage
    harness.crash(victim)
    harness.run(0.5)
    harness.put("post-crash", "yes")
    harness.add_node(victim, CONFIG3, storage=storage, snapshot_threshold=10)
    harness.run(3.0)
    led = harness.wait_leader()
    assert harness.hosts[victim].machine.data == harness.hosts[led.id].machine.data
    assert harness.hosts[victim].machine.data.get("post-crash") == "yes"
