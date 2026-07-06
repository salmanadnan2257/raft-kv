"""Raft RPC message types.

All messages are plain dataclasses serializable to/from dicts so both the
JSON TCP transport and the in-memory simulated transport can carry them.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, ClassVar


@dataclass(frozen=True)
class Entry:
    """A single log entry.

    kind is one of:
      "noop":    appended by a fresh leader to commit entries from prior terms
      "command": a state machine command (dict payload)
      "config":  cluster membership; data is {"nodes": [node ids]}
    """

    index: int
    term: int
    kind: str
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "Entry":
        return Entry(index=d["index"], term=d["term"], kind=d["kind"], data=d["data"])


@dataclass(frozen=True)
class Message:
    TYPE: ClassVar[str] = "message"
    term: int

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["type"] = self.TYPE
        return d


@dataclass(frozen=True)
class RequestVote(Message):
    TYPE: ClassVar[str] = "request_vote"
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteReply(Message):
    TYPE: ClassVar[str] = "request_vote_reply"
    voter_id: str
    vote_granted: bool


@dataclass(frozen=True)
class AppendEntries(Message):
    TYPE: ClassVar[str] = "append_entries"
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: tuple[Entry, ...]
    leader_commit: int
    seq: int  # heartbeat sequence number, used by ReadIndex

    def to_dict(self) -> dict[str, Any]:
        d = super().to_dict()
        d["entries"] = [e.to_dict() for e in self.entries]
        return d


@dataclass(frozen=True)
class AppendEntriesReply(Message):
    TYPE: ClassVar[str] = "append_entries_reply"
    follower_id: str
    success: bool
    match_index: int
    conflict_index: int
    seq: int


@dataclass(frozen=True)
class InstallSnapshot(Message):
    TYPE: ClassVar[str] = "install_snapshot"
    leader_id: str
    last_included_index: int
    last_included_term: int
    config: tuple[str, ...]
    data: str  # state machine snapshot, JSON text


@dataclass(frozen=True)
class InstallSnapshotReply(Message):
    TYPE: ClassVar[str] = "install_snapshot_reply"
    follower_id: str
    last_included_index: int


_TYPES: dict[str, type[Message]] = {
    cls.TYPE: cls
    for cls in (
        RequestVote,
        RequestVoteReply,
        AppendEntries,
        AppendEntriesReply,
        InstallSnapshot,
        InstallSnapshotReply,
    )
}


def message_from_dict(d: dict[str, Any]) -> Message:
    d = dict(d)
    cls = _TYPES[d.pop("type")]
    if cls is AppendEntries:
        d["entries"] = tuple(Entry.from_dict(e) for e in d["entries"])
    if cls is InstallSnapshot:
        d["config"] = tuple(d["config"])
    return cls(**d)
