"""KV state machine semantics and exactly-once client sessions."""

from __future__ import annotations

from conftest import Harness

from raftkv.kv.machine import KVMachine
from raftkv.raft.messages import Entry

CONFIG3 = ["n1", "n2", "n3"]


def cmd(index: int, **data: object) -> Entry:
    return Entry(index, 1, "command", data)


def test_basic_ops() -> None:
    m = KVMachine()
    assert m.apply(cmd(1, op="put", key="a", value="1")) == {"ok": True}
    assert m.get("a") == "1"
    assert m.apply(cmd(2, op="cas", key="a", expected="1", value="2")) == \
        {"ok": True, "swapped": True, "value": "1"}
    assert m.apply(cmd(3, op="cas", key="a", expected="1", value="3"))["swapped"] \
        is False
    assert m.get("a") == "2"
    assert m.apply(cmd(4, op="delete", key="a")) == {"ok": True, "existed": True}
    assert m.get("a") is None
    assert m.apply(cmd(5, op="cas", key="a", expected=None, value="x"))["swapped"]
    assert m.get("a") == "x"


def test_duplicate_seq_returns_cached_result_without_reapplying() -> None:
    m = KVMachine()
    first = m.apply(cmd(1, op="cas", key="a", expected=None, value="1",
                        client="c1", seq=5))
    assert first["swapped"] is True
    # Re-apply the same command (leader failover replays the log, or a
    # client retry lands a duplicate entry): must be a no-op with the
    # original result, even though a fresh cas would now fail.
    dup = m.apply(cmd(2, op="cas", key="a", expected=None, value="1",
                      client="c1", seq=5))
    assert dup["swapped"] is True
    assert m.get("a") == "1"


def test_sessions_survive_snapshot_roundtrip() -> None:
    m = KVMachine()
    m.apply(cmd(1, op="put", key="a", value="1", client="c1", seq=3))
    m2 = KVMachine()
    m2.restore(m.snapshot())
    dup = m2.apply(cmd(2, op="put", key="a", value="1", client="c1", seq=3))
    assert dup == {"ok": True}
    assert m2.sessions["c1"][0] == 3


def test_exactly_once_across_leader_failover() -> None:
    """Commit a cas on the old leader, crash it before the client hears
    back, retry the same (client, seq) on the new leader: the retry must
    return the original result and not double-apply."""
    harness = Harness(seed=20)
    for nid in CONFIG3:
        harness.add_node(nid, CONFIG3)
    led = harness.wait_leader()
    harness.put("x", "0")
    the_cmd = {"op": "cas", "key": "x", "expected": "0", "value": "1",
               "client": "app", "seq": 1}
    index, _ = led.propose(dict(the_cmd))
    harness.wait_commit(led.id, index)
    harness.crash(led.id)  # client never saw the response

    new_led = harness.wait_leader()
    assert new_led.id != led.id
    results: list[tuple[int, dict]] = []
    new_led.apply_listeners.append(
        lambda i, t, e, r: results.append((i, r)))
    retry_index, _ = new_led.propose(dict(the_cmd))
    harness.wait_commit(new_led.id, retry_index)
    retry_result = next(r for i, r in results if i == retry_index)
    assert retry_result["swapped"] is True          # cached original outcome
    assert harness.hosts[new_led.id].machine.get("x") == "1"  # applied once
