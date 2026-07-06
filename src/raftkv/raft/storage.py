"""Persistent state for a Raft node.

Two implementations of one interface:

FileStorage: real on-disk persistence.
  - hardstate.json: currentTerm and votedFor, written atomically via a temp
    file, fsync, and rename (rename is atomic on POSIX).
  - log.bin: append-only record file. Each record is a 4-byte big-endian
    length, a 4-byte CRC32 of the payload, then a JSON payload. On open the
    file is scanned; a short or corrupt record marks a torn tail from a crash
    mid-write and everything from that point is discarded (safe: Raft only
    acknowledges after fsync, so torn records were never acknowledged).
  - snapshot.json: snapshot metadata plus state machine data, atomic
    temp+fsync+rename. After a snapshot lands, the log file is rewritten to
    contain only entries after the snapshot point.

MemoryStorage: same interface for deterministic simulation. Survives a
simulated crash (the harness keeps the storage while destroying the node),
which models an fsync'd disk.
"""

from __future__ import annotations

import json
import os
import struct
import zlib
from dataclasses import dataclass, field
from typing import Protocol

from .messages import Entry

_REC_HEADER = struct.Struct(">II")  # length, crc32


@dataclass
class SnapshotState:
    last_index: int = 0
    last_term: int = 0
    config: tuple[str, ...] = ()
    data: str = ""  # state machine snapshot (JSON text)


@dataclass
class PersistedState:
    term: int = 0
    voted_for: str | None = None
    entries: list[Entry] = field(default_factory=list)
    snapshot: SnapshotState = field(default_factory=SnapshotState)


class Storage(Protocol):
    def load(self) -> PersistedState: ...
    def save_term_vote(self, term: int, voted_for: str | None) -> None: ...
    def append_entries(self, entries: list[Entry]) -> None: ...
    def truncate_suffix(self, from_index: int, kept: list[Entry]) -> None: ...
    def save_snapshot(self, snapshot: SnapshotState, remaining: list[Entry]) -> None: ...


class MemoryStorage:
    def __init__(self) -> None:
        self._state = PersistedState()

    def load(self) -> PersistedState:
        s = self._state
        return PersistedState(s.term, s.voted_for, list(s.entries),
                              SnapshotState(s.snapshot.last_index, s.snapshot.last_term,
                                            s.snapshot.config, s.snapshot.data))

    def save_term_vote(self, term: int, voted_for: str | None) -> None:
        self._state.term = term
        self._state.voted_for = voted_for

    def append_entries(self, entries: list[Entry]) -> None:
        self._state.entries.extend(entries)

    def truncate_suffix(self, from_index: int, kept: list[Entry]) -> None:
        self._state.entries = list(kept)

    def save_snapshot(self, snapshot: SnapshotState, remaining: list[Entry]) -> None:
        self._state.snapshot = snapshot
        self._state.entries = list(remaining)


def _atomic_write(path: str, data: bytes) -> None:
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.rename(tmp, path)
    dirfd = os.open(os.path.dirname(path) or ".", os.O_RDONLY)
    try:
        os.fsync(dirfd)
    finally:
        os.close(dirfd)


def _encode_record(entry: Entry) -> bytes:
    payload = json.dumps(entry.to_dict(), separators=(",", ":")).encode()
    return _REC_HEADER.pack(len(payload), zlib.crc32(payload)) + payload


class FileStorage:
    def __init__(self, data_dir: str) -> None:
        self.dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self._hard = os.path.join(data_dir, "hardstate.json")
        self._logf = os.path.join(data_dir, "log.bin")
        self._snap = os.path.join(data_dir, "snapshot.json")
        self._log_fh = open(self._logf, "ab")

    def close(self) -> None:
        self._log_fh.close()

    def load(self) -> PersistedState:
        st = PersistedState()
        if os.path.exists(self._hard):
            with open(self._hard, "rb") as f:
                d = json.load(f)
            st.term, st.voted_for = d["term"], d["voted_for"]
        if os.path.exists(self._snap):
            with open(self._snap, "rb") as f:
                d = json.load(f)
            st.snapshot = SnapshotState(d["last_index"], d["last_term"],
                                        tuple(d["config"]), d["data"])
        st.entries = self._read_log(st.snapshot.last_index)
        return st

    def _read_log(self, snapshot_index: int) -> list[Entry]:
        entries: list[Entry] = []
        good_end = 0
        with open(self._logf, "rb") as f:
            raw = f.read()
        pos = 0
        while pos + _REC_HEADER.size <= len(raw):
            length, crc = _REC_HEADER.unpack_from(raw, pos)
            start = pos + _REC_HEADER.size
            payload = raw[start:start + length]
            if len(payload) < length or zlib.crc32(payload) != crc:
                break  # torn tail from a crash mid-write
            entries.append(Entry.from_dict(json.loads(payload)))
            pos = start + length
            good_end = pos
        if good_end < len(raw):
            # Discard the torn tail so future appends start from a clean point.
            self._log_fh.close()
            with open(self._logf, "r+b") as f:
                f.truncate(good_end)
                f.flush()
                os.fsync(f.fileno())
            self._log_fh = open(self._logf, "ab")
        # Entries at or below the snapshot point may linger if we crashed
        # between snapshot rename and log rewrite; drop them.
        return [e for e in entries if e.index > snapshot_index]

    def save_term_vote(self, term: int, voted_for: str | None) -> None:
        _atomic_write(self._hard, json.dumps(
            {"term": term, "voted_for": voted_for}).encode())

    def append_entries(self, entries: list[Entry]) -> None:
        if not entries:
            return
        self._log_fh.write(b"".join(_encode_record(e) for e in entries))
        self._log_fh.flush()
        os.fsync(self._log_fh.fileno())

    def _rewrite_log(self, entries: list[Entry]) -> None:
        self._log_fh.close()
        _atomic_write(self._logf, b"".join(_encode_record(e) for e in entries))
        self._log_fh = open(self._logf, "ab")

    def truncate_suffix(self, from_index: int, kept: list[Entry]) -> None:
        self._rewrite_log(kept)

    def save_snapshot(self, snapshot: SnapshotState, remaining: list[Entry]) -> None:
        _atomic_write(self._snap, json.dumps({
            "last_index": snapshot.last_index,
            "last_term": snapshot.last_term,
            "config": list(snapshot.config),
            "data": snapshot.data,
        }).encode())
        self._rewrite_log(remaining)
