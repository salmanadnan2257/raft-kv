"""Shared test harness: hand-built simulated clusters with full control
over storage contents, timing, partitions, and crashes."""

from __future__ import annotations

import random
from typing import Any

import pytest

from raftkv.kv.machine import KVMachine
from raftkv.raft.messages import Entry
from raftkv.raft.node import RaftNode, Role
from raftkv.raft.storage import MemoryStorage
from raftkv.sim.clock import SimClock
from raftkv.sim.network import SimEnv, SimNetwork


class Host:
    def __init__(self, node: RaftNode, machine: KVMachine, env: SimEnv,
                 storage: MemoryStorage) -> None:
        self.node = node
        self.machine = machine
        self.env = env
        self.storage = storage


class Harness:
    def __init__(self, seed: int = 0, **net_kw: Any) -> None:
        self.clock = SimClock()
        self.rng = random.Random(seed)
        self.network = SimNetwork(self.clock,
                                  random.Random(self.rng.getrandbits(64)),
                                  **net_kw)
        self.hosts: dict[str, Host] = {}
        self._seq = 0

    def add_node(self, nid: str, config: list[str],
                 storage: MemoryStorage | None = None,
                 election_timeout: float = 0.15,
                 heartbeat_interval: float = 0.03,
                 snapshot_threshold: int = 10**9) -> RaftNode:
        storage = storage or MemoryStorage()
        env = SimEnv(nid, self.clock, self.network,
                     random.Random(self.rng.getrandbits(64)))
        machine = KVMachine()
        node = RaftNode(nid, config, storage, env, machine.apply,
                        machine.snapshot, machine.restore,
                        election_timeout=election_timeout,
                        heartbeat_interval=heartbeat_interval,
                        snapshot_threshold=snapshot_threshold)
        self.network.register(nid,
                              lambda src, msg, n=node: n.handle_message(src, msg))
        self.hosts[nid] = Host(node, machine, env, storage)
        return node

    def crash(self, nid: str) -> None:
        host = self.hosts.pop(nid)
        host.env.kill()
        host.node.stop()
        self.network.unregister(nid)

    def run(self, seconds: float) -> None:
        self.clock.run_until(self.clock.now + seconds)

    def leader(self) -> RaftNode | None:
        leaders = [h.node for h in self.hosts.values()
                   if h.node.role is Role.LEADER]
        if not leaders:
            return None
        return max(leaders, key=lambda n: n.current_term)

    def wait_leader(self, timeout: float = 10.0,
                    among: set[str] | None = None) -> RaftNode:
        deadline = self.clock.now + timeout
        while self.clock.now < deadline:
            self.run(0.05)
            led = self.leader()
            if led is None or (among is not None and led.id not in among):
                continue
            # Stable: a majority of the leader's config is on its term.
            on_term = sum(
                1 for n in led.config
                if n in self.hosts and
                self.hosts[n].node.current_term == led.current_term)
            if on_term >= len(led.config) // 2 + 1:
                return led
        raise AssertionError("no leader elected within timeout")

    def put(self, key: str, value: str, client: str = "t",
            seq: int | None = None) -> int:
        """Propose a put on the current leader and run until it commits."""
        led = self.wait_leader()
        if seq is None:
            self._seq += 1
            seq = self._seq
        index, _ = led.propose({"op": "put", "key": key, "value": value,
                                "client": client, "seq": seq})
        self.wait_commit(led.id, index)
        return index

    def wait_commit(self, nid: str, index: int, timeout: float = 10.0) -> None:
        deadline = self.clock.now + timeout
        while self.clock.now < deadline:
            self.run(0.05)
            if self.hosts[nid].node.commit_index >= index:
                return
        raise AssertionError(f"index {index} not committed on {nid}")


def entries_from_terms(terms: list[int]) -> list[Entry]:
    """Log with one command entry per term in the list (1-based indexes)."""
    return [Entry(i + 1, t, "command",
                  {"op": "put", "key": f"k{i + 1}", "value": f"t{t}"})
            for i, t in enumerate(terms)]


def storage_with(terms: list[int], current_term: int | None = None) -> MemoryStorage:
    st = MemoryStorage()
    st.append_entries(entries_from_terms(terms))
    st.save_term_vote(current_term if current_term is not None
                      else max(terms, default=0), None)
    return st


@pytest.fixture
def harness() -> Harness:
    return Harness(seed=0)
