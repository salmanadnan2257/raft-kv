"""In-memory Raft log with snapshot-aware index arithmetic.

Entries before (and including) the snapshot's last included index are
compacted away; `offset` tracks how many indices the snapshot swallowed.
Log indices are 1-based, as in the paper. Index 0 is the empty-log sentinel
with term 0.
"""

from __future__ import annotations

from .messages import Entry


class RaftLog:
    def __init__(self, snapshot_index: int = 0, snapshot_term: int = 0,
                 entries: list[Entry] | None = None) -> None:
        self.snapshot_index = snapshot_index
        self.snapshot_term = snapshot_term
        self.entries: list[Entry] = list(entries or [])

    @property
    def last_index(self) -> int:
        return self.snapshot_index + len(self.entries)

    @property
    def last_term(self) -> int:
        return self.entries[-1].term if self.entries else self.snapshot_term

    def contains(self, index: int) -> bool:
        return self.snapshot_index < index <= self.last_index

    def entry(self, index: int) -> Entry:
        return self.entries[index - self.snapshot_index - 1]

    def term_at(self, index: int) -> int | None:
        """Term of the entry at index, or None if unknown (compacted or beyond end)."""
        if index == self.snapshot_index:
            return self.snapshot_term
        if index == 0:
            return 0
        if self.contains(index):
            return self.entry(index).term
        return None

    def slice_from(self, index: int) -> list[Entry]:
        return self.entries[max(0, index - self.snapshot_index - 1):]

    def append(self, entry: Entry) -> None:
        assert entry.index == self.last_index + 1
        self.entries.append(entry)

    def truncate_suffix(self, from_index: int) -> None:
        """Drop entries at from_index and beyond."""
        self.entries = self.entries[: max(0, from_index - self.snapshot_index - 1)]

    def compact_to(self, index: int, term: int) -> None:
        """Discard entries up to and including index (which becomes the snapshot point)."""
        if index <= self.snapshot_index:
            return
        kept = self.entries[index - self.snapshot_index:] if self.contains(index) else []
        self.entries = kept
        self.snapshot_index = index
        self.snapshot_term = term
