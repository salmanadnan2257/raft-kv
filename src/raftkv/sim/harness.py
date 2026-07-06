"""Deterministic simulation harness.

One seeded run wires N Raft nodes to the virtual clock and simulated
network, lets clients issue KV operations, and meanwhile a nemesis injects
partitions, crashes, restarts, message loss/duplication, and election-timer
clock skew. Safety invariants are checked continuously; the recorded client
history is checked for linearizability at the end. Every failure message
carries the seed, and re-running the seed reproduces the exact schedule.

This is the FoundationDB-style approach: make the whole distributed system
a deterministic function of one integer, then search that integer space.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from typing import Any, Callable

from ..checker.history import HistoryRecorder, Op
from ..checker.linearizability import CheckResult, Outcome, check_history
from ..kv.machine import KVMachine
from ..raft.messages import Entry
from ..raft.node import NotLeader, RaftNode, Role
from ..raft.storage import MemoryStorage
from .clock import SimClock
from .network import SimEnv, SimNetwork


class InvariantViolation(AssertionError):
    pass


@dataclass
class SimConfig:
    seed: int
    n_nodes: int = 3
    duration: float = 12.0          # virtual seconds
    n_clients: int = 3
    n_keys: int = 3
    drop_prob: float = 0.05
    dup_prob: float = 0.05
    min_delay: float = 0.001
    max_delay: float = 0.030
    partition_interval: float = 1.5  # mean time between nemesis actions
    crash_prob: float = 0.25         # nemesis action mix
    partition_prob: float = 0.35
    skew_prob: float = 0.15
    restart_delay: float = 1.0
    op_timeout: float = 0.8
    snapshot_threshold: int = 40
    election_timeout: float = 0.15
    heartbeat_interval: float = 0.03


@dataclass
class SimResult:
    seed: int
    ops_completed: int
    ops_incomplete: int
    messages_sent: int
    terms_seen: int
    snapshots_taken: int
    crashes: int
    partitions: int
    linearizability: list[CheckResult]
    history: HistoryRecorder

    @property
    def ok(self) -> bool:
        return all(r.outcome is not Outcome.VIOLATION for r in self.linearizability)


class Monitor:
    """Continuous safety invariant checks across the whole cluster."""

    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.leaders_by_term: dict[int, str] = {}
        self.applied: dict[int, tuple[int, str]] = {}  # index -> (term, payload)
        self.snapshots_taken = 0
        self._steps = 0
        self.cluster: "SimCluster | None" = None

    def fail(self, why: str) -> None:
        raise InvariantViolation(f"[seed={self.seed}] {why}")

    def trace(self, node_id: str) -> Callable[..., None]:
        def cb(event: str, **kw: Any) -> None:
            if event == "leader":
                term = kw["term"]
                prev = self.leaders_by_term.get(term)
                if prev is not None and prev != node_id:
                    self.fail(f"two leaders in term {term}: {prev} and {node_id}")
                self.leaders_by_term[term] = node_id
                self._check_leader_completeness(node_id)
            elif event == "apply":
                entry: Entry = kw["entry"]
                payload = json.dumps(
                    [entry.term, entry.kind, entry.data], sort_keys=True)
                prev_applied = self.applied.get(entry.index)
                if prev_applied is not None and prev_applied != (entry.term, payload):
                    self.fail(
                        f"state machine divergence at index {entry.index}: "
                        f"{prev_applied} vs {(entry.term, payload)} on {node_id}")
                self.applied[entry.index] = (entry.term, payload)
            elif event == "snapshot":
                self.snapshots_taken += 1
        return cb

    def _check_leader_completeness(self, leader_id: str) -> None:
        """A new leader's log must contain every entry ever committed."""
        assert self.cluster is not None
        node = self.cluster.hosts[leader_id].node
        if node is None:
            return
        for index, (term, _) in self.applied.items():
            if index <= node.log.snapshot_index:
                continue  # compacted away; the snapshot covered it
            t = node.log.term_at(index)
            if t != term:
                self.fail(
                    f"leader completeness: new leader {leader_id} missing "
                    f"committed entry {index} (term {term}, leader has {t})")

    def on_step(self) -> None:
        self._steps += 1
        if self._steps % 200 == 0:
            self.check_log_matching()

    def check_log_matching(self) -> None:
        assert self.cluster is not None
        nodes = [h.node for h in self.cluster.hosts.values() if h.node is not None]
        for i in range(len(nodes)):
            for j in range(i + 1, len(nodes)):
                a, b = nodes[i].log, nodes[j].log
                lo = max(a.snapshot_index, b.snapshot_index) + 1
                hi = min(a.last_index, b.last_index)
                for idx in range(hi, lo - 1, -1):
                    ea, eb = a.entry(idx), b.entry(idx)
                    if ea.term == eb.term:
                        if ea != eb:
                            self.fail(f"log matching: index {idx} same term "
                                      f"{ea.term} but different entries")
                        break  # matching here implies matching below (checked lazily)
                    # different terms at same index is fine pre-truncation


class SimHost:
    def __init__(self, node_id: str) -> None:
        self.id = node_id
        self.storage = MemoryStorage()
        self.env: SimEnv | None = None
        self.node: RaftNode | None = None
        self.machine: KVMachine | None = None
        self.pending: dict[tuple[int, int], Callable[[Any], None]] = {}


class SimCluster:
    def __init__(self, cfg: SimConfig, clock: SimClock, network: SimNetwork,
                 rng: random.Random, monitor: Monitor) -> None:
        self.cfg = cfg
        self.clock = clock
        self.network = network
        self.rng = rng
        self.monitor = monitor
        self.node_ids = [f"n{i + 1}" for i in range(cfg.n_nodes)]
        self.hosts = {nid: SimHost(nid) for nid in self.node_ids}
        self.crashes = 0
        monitor.cluster = self
        for nid in self.node_ids:
            self.start_node(nid)

    def start_node(self, nid: str) -> None:
        host = self.hosts[nid]
        env = SimEnv(nid, self.clock, self.network,
                     random.Random(self.rng.getrandbits(64)))
        machine = KVMachine()
        node = RaftNode(
            nid, self.node_ids, host.storage, env,
            machine.apply, machine.snapshot, machine.restore,
            election_timeout=self.cfg.election_timeout,
            heartbeat_interval=self.cfg.heartbeat_interval,
            snapshot_threshold=self.cfg.snapshot_threshold)
        node.trace = self.monitor.trace(nid)
        node.apply_listeners.append(self._make_apply_listener(host))
        node.step_down_listeners.append(lambda h=host: self._fail_pending(h))
        self.network.register(nid, lambda src, msg, n=node: n.handle_message(src, msg))
        host.env, host.node, host.machine = env, node, machine

    def _make_apply_listener(self, host: SimHost) -> Callable[..., None]:
        def on_apply(index: int, term: int, entry: Entry, result: Any) -> None:
            cb = host.pending.pop((index, term), None)
            if cb is not None:
                cb(result)
            # Any pending write at this index with a different term lost its slot.
            for (i, t) in [k for k in host.pending if k[0] == index and k[1] != term]:
                host.pending.pop((i, t))(None)
        return on_apply

    def _fail_pending(self, host: SimHost) -> None:
        pending, host.pending = host.pending, {}
        for cb in pending.values():
            cb(None)

    def crash(self, nid: str) -> None:
        host = self.hosts[nid]
        if host.node is None:
            return
        self.crashes += 1
        host.env.kill()  # type: ignore[union-attr]
        host.node.stop()
        self.network.unregister(nid)
        self._fail_pending(host)
        host.env = host.node = host.machine = None

    def restart(self, nid: str) -> None:
        if self.hosts[nid].node is None:
            self.start_node(nid)

    def alive_ids(self) -> list[str]:
        return [nid for nid, h in self.hosts.items() if h.node is not None]

    # -------------------------------------------------------- client access

    def submit_write(self, nid: str, cmd: dict[str, Any],
                     cb: Callable[[Any], None]) -> bool:
        """Returns False if the node can't take the write right now."""
        host = self.hosts[nid]
        if host.node is None or host.node.role is not Role.LEADER:
            return False
        try:
            index, term = host.node.propose(cmd)
        except NotLeader:
            return False
        host.pending[(index, term)] = cb
        return True

    def submit_read(self, nid: str, key: str, cb: Callable[[Any], None]) -> bool:
        host = self.hosts[nid]
        if host.node is None or host.node.role is not Role.LEADER:
            return False
        machine = host.machine

        def on_read_index(ri: int | None) -> None:
            if ri is None or machine is not self.hosts[nid].machine:
                cb(None)
            else:
                cb({"value": machine.get(key)})  # type: ignore[union-attr]
        host.node.read_index(on_read_index)
        return True


class SimClient:
    """Issues randomized KV ops, retrying across nodes, recording history."""

    def __init__(self, client_id: str, cluster: SimCluster, history: HistoryRecorder,
                 rng: random.Random, cfg: SimConfig, deadline: float) -> None:
        self.id = client_id
        self.cluster = cluster
        self.history = history
        self.rng = rng
        self.cfg = cfg
        self.deadline = deadline
        self.seq = 0
        self.completed = 0
        self.incomplete = 0
        self.cluster.clock.call_later(rng.uniform(0, 0.2), self._next_op)

    def _next_op(self) -> None:
        clock = self.cluster.clock
        if clock.now >= self.deadline:
            return
        key = f"k{self.rng.randrange(self.cfg.n_keys)}"
        roll = self.rng.random()
        if roll < 0.4:
            kind, args = "get", {}
        elif roll < 0.75:
            kind, args = "put", {"value": f"{self.id}-{self.seq}"}
        else:
            # cas against a plausible previous value (or absent)
            expected = None if self.rng.random() < 0.3 else \
                f"{self.id}-{self.rng.randrange(max(1, self.seq + 1))}"
            kind, args = "cas", {"expected": expected,
                                 "value": f"{self.id}-{self.seq}"}
        self.seq += 1
        op = self.history.invoke(self.id, key, kind, args, clock.now)
        self._attempt(op, attempts_left=8)

    def _finish(self, op: Op, result: Any) -> None:
        self.history.complete(op, result, self.cluster.clock.now)
        self.completed += 1
        self.cluster.clock.call_later(self.rng.uniform(0.01, 0.15), self._next_op)

    def _give_up(self) -> None:
        self.incomplete += 1
        self.cluster.clock.call_later(self.rng.uniform(0.05, 0.3), self._next_op)

    def _attempt(self, op: Op, attempts_left: int) -> None:
        clock = self.cluster.clock
        if attempts_left <= 0 or clock.now >= self.deadline + 3.0:
            self._give_up()
            return
        target = self.rng.choice(self.cluster.node_ids)
        responded = False

        def cb(result: Any) -> None:
            nonlocal responded
            if responded:
                return
            responded = True
            if result is None:
                retry()
                return
            if op.kind == "get":
                self._finish(op, result["value"])
            elif op.kind == "put":
                self._finish(op, "ok")
            elif op.kind == "cas":
                self._finish(op, result["swapped"])

        def retry() -> None:
            clock.call_later(self.rng.uniform(0.02, 0.1),
                             lambda: self._attempt(op, attempts_left - 1))

        def on_timeout() -> None:
            nonlocal responded
            if not responded:
                responded = True  # ignore a late response; the retry (same
                # client seq for writes) will be deduplicated by the session
                retry()

        if op.kind == "get":
            accepted = self.cluster.submit_read(target, op.key, cb)
        else:
            cmd = {"op": op.kind, "key": op.key, "client": self.id,
                   "seq": op.op_id, **op.args}
            accepted = self.cluster.submit_write(target, cmd, cb)
        if not accepted:
            responded = True
            retry()
            return
        clock.call_later(self.cfg.op_timeout, on_timeout)


class Nemesis:
    def __init__(self, cluster: SimCluster, rng: random.Random, cfg: SimConfig,
                 chaos_deadline: float) -> None:
        self.cluster = cluster
        self.rng = rng
        self.cfg = cfg
        self.chaos_deadline = chaos_deadline
        self.partitions_made = 0
        cluster.clock.call_later(rng.expovariate(1 / cfg.partition_interval),
                                 self._act)

    def _act(self) -> None:
        clock = self.cluster.clock
        if clock.now >= self.chaos_deadline:
            self._stabilize()
            return
        roll = self.rng.random()
        cfg = self.cfg
        if roll < cfg.partition_prob:
            ids = list(self.cluster.node_ids)
            self.rng.shuffle(ids)
            cut = self.rng.randrange(1, len(ids))
            self.cluster.network.heal()
            self.cluster.network.partition(set(ids[:cut]), set(ids[cut:]))
            self.partitions_made += 1
        elif roll < cfg.partition_prob + cfg.crash_prob:
            alive = self.cluster.alive_ids()
            if len(alive) > 1:  # keep at least one node up
                victim = self.rng.choice(alive)
                self.cluster.crash(victim)
                clock.call_later(self.rng.uniform(0.3, cfg.restart_delay * 2),
                                 lambda: self.cluster.restart(victim))
        elif roll < cfg.partition_prob + cfg.crash_prob + cfg.skew_prob:
            nid = self.rng.choice(self.cluster.node_ids)
            env = self.cluster.hosts[nid].env
            if env is not None:
                env.timer_scale = self.rng.uniform(0.5, 1.8)
        else:
            self.cluster.network.heal()
        clock.call_later(self.rng.expovariate(1 / cfg.partition_interval), self._act)

    def _stabilize(self) -> None:
        """End of chaos window: heal everything so clients can drain."""
        self.cluster.network.heal()
        for nid in self.cluster.node_ids:
            self.cluster.restart(nid)
            env = self.cluster.hosts[nid].env
            if env is not None:
                env.timer_scale = 1.0


def run_simulation(cfg: SimConfig) -> SimResult:
    root = random.Random(cfg.seed)
    clock = SimClock()
    network = SimNetwork(clock, random.Random(root.getrandbits(64)),
                         min_delay=cfg.min_delay, max_delay=cfg.max_delay,
                         drop_prob=cfg.drop_prob, dup_prob=cfg.dup_prob)
    monitor = Monitor(cfg.seed)
    cluster_rng = random.Random(root.getrandbits(64))
    cluster = SimCluster(cfg, clock, network, cluster_rng, monitor)
    chaos_deadline = cfg.duration * 0.7
    nemesis = Nemesis(cluster, random.Random(root.getrandbits(64)), cfg,
                      chaos_deadline)
    history = HistoryRecorder()
    clients = [
        SimClient(f"c{i + 1}", cluster, history,
                  random.Random(root.getrandbits(64)), cfg,
                  deadline=cfg.duration * 0.9)
        for i in range(cfg.n_clients)
    ]

    clock.run_until(cfg.duration, after_each=monitor.on_step)
    # Drain: give in-flight retries a stable window to finish.
    clock.run_until(cfg.duration + 3.0, after_each=monitor.on_step)
    monitor.check_log_matching()

    lin = check_history(history.by_key())
    for r in lin:
        if r.outcome is Outcome.VIOLATION:
            raise InvariantViolation(
                f"[seed={cfg.seed}] linearizability violation: {r.detail}")
    return SimResult(
        seed=cfg.seed,
        ops_completed=sum(c.completed for c in clients),
        ops_incomplete=sum(c.incomplete for c in clients),
        messages_sent=network.messages_sent,
        terms_seen=max(monitor.leaders_by_term, default=0),
        snapshots_taken=monitor.snapshots_taken,
        crashes=cluster.crashes,
        partitions=nemesis.partitions_made,
        linearizability=lin,
        history=history,
    )
