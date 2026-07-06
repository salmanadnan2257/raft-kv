"""The Raft consensus core.

Implements the algorithm from Ongaro & Ousterhout, "In Search of an
Understandable Consensus Algorithm" (the Raft paper), plus log compaction
and single-server membership changes from Ongaro's dissertation.

The node is written sans-IO: it reacts to three kinds of stimuli
(incoming messages, timer callbacks, local API calls) and produces effects
only through the injected Env (send, timers) and Storage (persistence).
That is what lets the exact same class run on a real asyncio TCP cluster
and inside the deterministic simulator.

Persistence discipline: currentTerm/votedFor and log mutations hit Storage
before any message that reveals them is sent, matching the paper's rules.
"""

from __future__ import annotations

import enum
from typing import Any, Callable

from .env import Env, TimerHandle
from .log import RaftLog
from .messages import (
    AppendEntries,
    AppendEntriesReply,
    Entry,
    InstallSnapshot,
    InstallSnapshotReply,
    Message,
    RequestVote,
    RequestVoteReply,
)
from .storage import SnapshotState, Storage


class Role(enum.Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class NotLeader(Exception):
    def __init__(self, leader_hint: str | None) -> None:
        super().__init__("not the leader")
        self.leader_hint = leader_hint


class ConfigChangeInProgress(Exception):
    pass


ApplyFn = Callable[[Entry], Any]
ApplyListener = Callable[[int, int, Entry, Any], None]  # index, term, entry, result
ReadCallback = Callable[[int | None], None]  # read index, or None on failure


class _PendingRead:
    __slots__ = ("seq", "read_index", "cb")

    def __init__(self, seq: int, read_index: int | None, cb: ReadCallback) -> None:
        self.seq = seq
        self.read_index = read_index  # None until an entry of our term commits
        self.cb = cb


class RaftNode:
    def __init__(
        self,
        node_id: str,
        initial_config: list[str],
        storage: Storage,
        env: Env,
        apply_fn: ApplyFn,
        snapshot_fn: Callable[[], str],
        restore_fn: Callable[[str], None],
        *,
        election_timeout: float = 0.15,
        heartbeat_interval: float = 0.03,
        snapshot_threshold: int = 1000,
    ) -> None:
        self.id = node_id
        self.env = env
        self.storage = storage
        self.apply_fn = apply_fn
        self.snapshot_fn = snapshot_fn
        self.restore_fn = restore_fn
        self.election_timeout = election_timeout
        self.heartbeat_interval = heartbeat_interval
        self.snapshot_threshold = snapshot_threshold

        st = storage.load()
        self.current_term = st.term
        self.voted_for = st.voted_for
        self.log = RaftLog(st.snapshot.last_index, st.snapshot.last_term, st.entries)
        self._snapshot_config = tuple(st.snapshot.config)
        self._initial_config = tuple(initial_config)
        if st.snapshot.data:
            self.restore_fn(st.snapshot.data)
        self.commit_index = st.snapshot.last_index
        self.last_applied = st.snapshot.last_index

        self.role = Role.FOLLOWER
        self.leader_hint: str | None = None
        self.config: tuple[str, ...] = self._latest_config()

        # Leader volatile state
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self._votes: set[str] = set()
        self._hb_seq = 0
        self._ack_seq: dict[str, int] = {}
        self._term_commit_index: int | None = None  # first commit of our term
        self._pending_reads: list[_PendingRead] = []

        self.apply_listeners: list[ApplyListener] = []
        self.step_down_listeners: list[Callable[[], None]] = []
        self.trace: Callable[..., None] | None = None

        self._election_timer: TimerHandle | None = None
        self._heartbeat_timer: TimerHandle | None = None
        self._reset_election_timer()

    # ------------------------------------------------------------------ config

    def _config_from(self, entries_last_index: int | None = None) -> tuple[str, ...]:
        for e in reversed(self.log.entries):
            if entries_last_index is not None and e.index > entries_last_index:
                continue
            if e.kind == "config":
                return tuple(e.data["nodes"])
        if self._snapshot_config:
            return self._snapshot_config
        return self._initial_config

    def _latest_config(self) -> tuple[str, ...]:
        return self._config_from(None)

    def _refresh_config(self) -> None:
        new = self._latest_config()
        if new != self.config:
            self.config = new
            if self.role is Role.LEADER:
                for p in self.peers:
                    self.next_index.setdefault(p, self.log.last_index + 1)
                    self.match_index.setdefault(p, 0)
                    self._ack_seq.setdefault(p, 0)

    @property
    def peers(self) -> list[str]:
        return [n for n in self.config if n != self.id]

    def _majority(self) -> int:
        return len(self.config) // 2 + 1

    # ------------------------------------------------------------------ timers

    def _reset_election_timer(self) -> None:
        if self._election_timer is not None:
            self._election_timer.cancel()
        delay = self.env.rng.uniform(self.election_timeout, 2 * self.election_timeout)
        self._election_timer = self.env.call_later(delay, self._on_election_timeout)

    def _on_election_timeout(self) -> None:
        self._reset_election_timer()
        if self.role is Role.LEADER or self.id not in self.config:
            return
        self._start_election()

    def _on_heartbeat_tick(self) -> None:
        if self.role is not Role.LEADER:
            return
        self._heartbeat_timer = self.env.call_later(
            self.heartbeat_interval, self._on_heartbeat_tick)
        self._broadcast_append()

    # --------------------------------------------------------------- elections

    def _persist_term_vote(self) -> None:
        self.storage.save_term_vote(self.current_term, self.voted_for)

    def _start_election(self) -> None:
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self._persist_term_vote()
        self._votes = {self.id}
        self.leader_hint = None
        if self.trace:
            self.trace("candidate", node=self.id, term=self.current_term)
        if len(self._votes) >= self._majority():
            self._become_leader()
            return
        msg = RequestVote(term=self.current_term, candidate_id=self.id,
                          last_log_index=self.log.last_index,
                          last_log_term=self.log.last_term)
        for p in self.peers:
            self.env.send(p, msg)

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_hint = self.id
        self.next_index = {p: self.log.last_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self._hb_seq = 0
        self._ack_seq = {p: 0 for p in self.peers}
        self._term_commit_index = None
        if self.trace:
            self.trace("leader", node=self.id, term=self.current_term)
        # A no-op entry lets the new leader commit entries from earlier terms
        # (the paper's section 5.4.2 restriction on counting replicas).
        self._append_local(Entry(self.log.last_index + 1, self.current_term, "noop"))
        if self._heartbeat_timer is not None:
            self._heartbeat_timer.cancel()
        self._heartbeat_timer = self.env.call_later(
            self.heartbeat_interval, self._on_heartbeat_tick)
        self._broadcast_append()
        self._maybe_advance_commit()

    def _step_down(self, term: int) -> None:
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._persist_term_vote()
        was_leader = self.role is Role.LEADER
        self.role = Role.FOLLOWER
        if self._heartbeat_timer is not None:
            self._heartbeat_timer.cancel()
            self._heartbeat_timer = None
        if was_leader:
            for r in self._pending_reads:
                r.cb(None)
            self._pending_reads.clear()
            for fn in self.step_down_listeners:
                fn()
        self._reset_election_timer()

    # ---------------------------------------------------------------- messages

    def handle_message(self, src: str, msg: Message) -> None:
        if msg.term > self.current_term:
            self._step_down(msg.term)
        if isinstance(msg, RequestVote):
            self._on_request_vote(msg)
        elif isinstance(msg, RequestVoteReply):
            self._on_request_vote_reply(msg)
        elif isinstance(msg, AppendEntries):
            self._on_append_entries(msg)
        elif isinstance(msg, AppendEntriesReply):
            self._on_append_entries_reply(msg)
        elif isinstance(msg, InstallSnapshot):
            self._on_install_snapshot(msg)
        elif isinstance(msg, InstallSnapshotReply):
            self._on_install_snapshot_reply(msg)

    def _on_request_vote(self, msg: RequestVote) -> None:
        granted = False
        if msg.term == self.current_term and self.voted_for in (None, msg.candidate_id):
            up_to_date = (msg.last_log_term, msg.last_log_index) >= (
                self.log.last_term, self.log.last_index)
            if up_to_date:
                granted = True
                self.voted_for = msg.candidate_id
                self._persist_term_vote()
                self._reset_election_timer()
        self.env.send(msg.candidate_id, RequestVoteReply(
            term=self.current_term, voter_id=self.id, vote_granted=granted))

    def _on_request_vote_reply(self, msg: RequestVoteReply) -> None:
        if self.role is not Role.CANDIDATE or msg.term != self.current_term:
            return
        if msg.vote_granted and msg.voter_id in self.config:
            self._votes.add(msg.voter_id)
            if len(self._votes) >= self._majority():
                self._become_leader()

    def _on_append_entries(self, msg: AppendEntries) -> None:
        if msg.term < self.current_term:
            self.env.send(msg.leader_id, AppendEntriesReply(
                term=self.current_term, follower_id=self.id, success=False,
                match_index=0, conflict_index=0, seq=msg.seq))
            return
        # Valid leader for our term.
        if self.role is not Role.FOLLOWER:
            self._step_down(msg.term)
        self.leader_hint = msg.leader_id
        self._reset_election_timer()

        prev_index, prev_term = msg.prev_log_index, msg.prev_log_term
        entries = list(msg.entries)
        if prev_index < self.log.snapshot_index:
            # Everything at or below the snapshot point is committed and
            # therefore matches; skip what the snapshot already covers.
            entries = [e for e in entries if e.index > self.log.snapshot_index]
            prev_index = self.log.snapshot_index
            prev_term = self.log.snapshot_term

        our_prev_term = self.log.term_at(prev_index)
        if our_prev_term is None or our_prev_term != prev_term:
            if our_prev_term is None:
                conflict = self.log.last_index + 1
            else:
                conflict = prev_index
                while conflict > self.log.snapshot_index + 1 and \
                        self.log.term_at(conflict - 1) == our_prev_term:
                    conflict -= 1
            self.env.send(msg.leader_id, AppendEntriesReply(
                term=self.current_term, follower_id=self.id, success=False,
                match_index=0, conflict_index=conflict, seq=msg.seq))
            return

        # Find the first new/conflicting entry; truncate on term conflict.
        to_append: list[Entry] = []
        for e in entries:
            existing_term = self.log.term_at(e.index)
            if existing_term is None:
                to_append.append(e)
            elif existing_term != e.term:
                self.log.truncate_suffix(e.index)
                self.storage.truncate_suffix(e.index, self.log.entries)
                self._refresh_config()
                to_append.append(e)
        if to_append:
            for e in to_append:
                self.log.append(e)
            self.storage.append_entries(to_append)
            self._refresh_config()

        match = prev_index + len(entries)
        if msg.leader_commit > self.commit_index:
            self._set_commit_index(min(msg.leader_commit, match))
        self.env.send(msg.leader_id, AppendEntriesReply(
            term=self.current_term, follower_id=self.id, success=True,
            match_index=match, conflict_index=0, seq=msg.seq))

    def _on_append_entries_reply(self, msg: AppendEntriesReply) -> None:
        if self.role is not Role.LEADER or msg.term != self.current_term:
            return
        p = msg.follower_id
        if p not in self.next_index:
            return
        self._ack_seq[p] = max(self._ack_seq.get(p, 0), msg.seq)
        if msg.success:
            self.match_index[p] = max(self.match_index[p], msg.match_index)
            self.next_index[p] = max(self.next_index[p], msg.match_index + 1)
            self._maybe_advance_commit()
            if self.next_index[p] <= self.log.last_index:
                self._send_append(p)
        else:
            # Fast backoff using the follower's conflict hint.
            self.next_index[p] = max(1, min(self.next_index[p] - 1, msg.conflict_index)
                                     if msg.conflict_index else self.next_index[p] - 1)
            self._send_append(p)
        self._check_pending_reads()

    def _on_install_snapshot(self, msg: InstallSnapshot) -> None:
        if msg.term < self.current_term:
            return
        if self.role is not Role.FOLLOWER:
            self._step_down(msg.term)
        self.leader_hint = msg.leader_id
        self._reset_election_timer()
        if msg.last_included_index > self.log.snapshot_index:
            snap = SnapshotState(msg.last_included_index, msg.last_included_term,
                                 msg.config, msg.data)
            if self.log.term_at(msg.last_included_index) == msg.last_included_term:
                # Our log already extends past the snapshot; keep the suffix.
                self.log.compact_to(msg.last_included_index, msg.last_included_term)
            else:
                self.log = RaftLog(msg.last_included_index, msg.last_included_term)
            self.storage.save_snapshot(snap, self.log.entries)
            self._snapshot_config = msg.config
            self.restore_fn(msg.data)
            self.last_applied = max(self.last_applied, msg.last_included_index)
            self.commit_index = max(self.commit_index, msg.last_included_index)
            self._refresh_config()
        self.env.send(msg.leader_id, InstallSnapshotReply(
            term=self.current_term, follower_id=self.id,
            last_included_index=msg.last_included_index))

    def _on_install_snapshot_reply(self, msg: InstallSnapshotReply) -> None:
        if self.role is not Role.LEADER or msg.term != self.current_term:
            return
        p = msg.follower_id
        if p in self.next_index:
            self.match_index[p] = max(self.match_index[p], msg.last_included_index)
            self.next_index[p] = max(self.next_index[p], msg.last_included_index + 1)
            self._maybe_advance_commit()
            if self.next_index[p] <= self.log.last_index:
                self._send_append(p)

    # ------------------------------------------------------------- replication

    def _append_local(self, entry: Entry) -> None:
        self.log.append(entry)
        self.storage.append_entries([entry])
        if entry.kind == "config":
            self._refresh_config()

    def _send_append(self, p: str) -> None:
        ni = self.next_index[p]
        if ni <= self.log.snapshot_index:
            snap = self._load_snapshot()
            self.env.send(p, InstallSnapshot(
                term=self.current_term, leader_id=self.id,
                last_included_index=snap.last_index,
                last_included_term=snap.last_term,
                config=tuple(snap.config), data=snap.data))
            return
        prev_term = self.log.term_at(ni - 1)
        assert prev_term is not None
        entries = tuple(self.log.slice_from(ni))
        self.env.send(p, AppendEntries(
            term=self.current_term, leader_id=self.id,
            prev_log_index=ni - 1, prev_log_term=prev_term,
            entries=entries, leader_commit=self.commit_index, seq=self._hb_seq))

    def _load_snapshot(self) -> SnapshotState:
        return self.storage.load().snapshot

    def _broadcast_append(self) -> None:
        self._hb_seq += 1
        for p in self.peers:
            self._send_append(p)
        self._check_pending_reads()  # single-node cluster completes immediately

    def _maybe_advance_commit(self) -> None:
        if self.role is not Role.LEADER:
            return
        matches = sorted(
            (self.match_index[n] if n != self.id else self.log.last_index)
            for n in self.config
        )
        if not matches:
            return
        candidate = matches[len(matches) - self._majority()]
        if candidate > self.commit_index and self.log.term_at(candidate) == self.current_term:
            self._set_commit_index(candidate)

    def _set_commit_index(self, index: int) -> None:
        if index <= self.commit_index:
            return
        self.commit_index = index
        if self.role is Role.LEADER and self._term_commit_index is None and \
                self.log.term_at(index) == self.current_term:
            self._term_commit_index = index
            for r in self._pending_reads:
                if r.read_index is None:
                    r.read_index = self.commit_index
        if self.trace:
            self.trace("commit", node=self.id, index=index)
        self._apply_committed()
        self._check_pending_reads()

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.entry(self.last_applied)
            result = self.apply_fn(entry) if entry.kind == "command" else None
            if self.trace:
                self.trace("apply", node=self.id, index=entry.index, entry=entry)
            for listener in self.apply_listeners:
                listener(entry.index, entry.term, entry, result)
            if entry.kind == "config" and self.role is Role.LEADER and \
                    self.id not in tuple(entry.data["nodes"]):
                self._step_down(self.current_term)
        self._maybe_snapshot()

    def _maybe_snapshot(self) -> None:
        if self.last_applied - self.log.snapshot_index < self.snapshot_threshold:
            return
        term = self.log.term_at(self.last_applied)
        assert term is not None
        config = self._config_from(self.last_applied)
        data = self.snapshot_fn()
        self.log.compact_to(self.last_applied, term)
        snap = SnapshotState(self.last_applied, term, config, data)
        self.storage.save_snapshot(snap, self.log.entries)
        self._snapshot_config = config
        if self.trace:
            self.trace("snapshot", node=self.id, index=self.last_applied)

    # ------------------------------------------------------------- client API

    def propose(self, command: dict[str, Any]) -> tuple[int, int]:
        """Append a command entry. Returns (index, term). Raises NotLeader."""
        if self.role is not Role.LEADER:
            raise NotLeader(self.leader_hint)
        entry = Entry(self.log.last_index + 1, self.current_term, "command", command)
        self._append_local(entry)
        self._broadcast_append()
        self._maybe_advance_commit()  # single-node cluster
        return entry.index, entry.term

    def read_index(self, cb: ReadCallback) -> None:
        """ReadIndex protocol: capture commitIndex, confirm leadership with a
        heartbeat round, then hand the read index to cb. cb(None) on failure."""
        if self.role is not Role.LEADER:
            cb(None)
            return
        ri = self.commit_index if self._term_commit_index is not None else None
        self._hb_seq += 1
        pending = _PendingRead(self._hb_seq, ri, cb)
        self._pending_reads.append(pending)
        for p in self.peers:
            self._send_append(p)
        self._check_pending_reads()

    def _check_pending_reads(self) -> None:
        if self.role is not Role.LEADER or not self._pending_reads:
            return
        done: list[_PendingRead] = []
        for r in self._pending_reads:
            if r.read_index is None:
                continue
            acks = sum(1 for n in self.config
                       if n == self.id or self._ack_seq.get(n, 0) >= r.seq)
            if acks >= self._majority() and self.last_applied >= r.read_index:
                done.append(r)
        for r in done:
            self._pending_reads.remove(r)
            r.cb(r.read_index)

    # -------------------------------------------------------------- membership

    def _config_change_pending(self) -> bool:
        return any(e.kind == "config" for e in self.log.slice_from(self.commit_index + 1))

    def _change_config(self, nodes: list[str]) -> tuple[int, int]:
        if self.role is not Role.LEADER:
            raise NotLeader(self.leader_hint)
        if self._config_change_pending():
            raise ConfigChangeInProgress()
        entry = Entry(self.log.last_index + 1, self.current_term, "config",
                      {"nodes": nodes})
        self._append_local(entry)
        for p in self.peers:
            self.next_index.setdefault(p, self.log.last_index)
            self.match_index.setdefault(p, 0)
            self._ack_seq.setdefault(p, 0)
        self._broadcast_append()
        self._maybe_advance_commit()
        return entry.index, entry.term

    def add_server(self, node_id: str) -> tuple[int, int]:
        """Single-server membership change: add one node."""
        if node_id in self.config:
            raise ValueError(f"{node_id} already in config")
        return self._change_config([*self.config, node_id])

    def remove_server(self, node_id: str) -> tuple[int, int]:
        """Single-server membership change: remove one node."""
        if node_id not in self.config:
            raise ValueError(f"{node_id} not in config")
        return self._change_config([n for n in self.config if n != node_id])

    # ------------------------------------------------------------------- misc

    def stop(self) -> None:
        if self._election_timer is not None:
            self._election_timer.cancel()
        if self._heartbeat_timer is not None:
            self._heartbeat_timer.cancel()

    def status(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "role": self.role.value,
            "term": self.current_term,
            "leader": self.leader_hint,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "last_log_index": self.log.last_index,
            "snapshot_index": self.log.snapshot_index,
            "config": list(self.config),
        }
