"""FileStorage: crash-safe persistence of term/vote, log, and snapshots."""

from __future__ import annotations

import json
import os
import struct

from conftest import entries_from_terms

from raftkv.raft.storage import FileStorage, SnapshotState


def test_term_vote_roundtrip(tmp_path) -> None:
    st = FileStorage(str(tmp_path))
    st.save_term_vote(7, "n2")
    st.close()
    st2 = FileStorage(str(tmp_path))
    loaded = st2.load()
    assert (loaded.term, loaded.voted_for) == (7, "n2")
    st2.close()


def test_log_roundtrip_and_truncate(tmp_path) -> None:
    st = FileStorage(str(tmp_path))
    entries = entries_from_terms([1, 1, 2, 3, 3])
    st.append_entries(entries)
    st.truncate_suffix(4, entries[:3])
    st.append_entries(entries_from_terms([1, 1, 2, 2])[3:])  # new index 4, term 2
    st.close()
    st2 = FileStorage(str(tmp_path))
    assert [e.term for e in st2.load().entries] == [1, 1, 2, 2]
    st2.close()


def test_crash_mid_write_discards_torn_tail(tmp_path) -> None:
    st = FileStorage(str(tmp_path))
    st.append_entries(entries_from_terms([1, 2, 3]))
    st.close()
    log_path = os.path.join(str(tmp_path), "log.bin")
    size = os.path.getsize(log_path)
    # Simulate a crash midway through appending a fourth record: a valid
    # header promising more bytes than were flushed.
    with open(log_path, "ab") as f:
        f.write(struct.pack(">II", 500, 12345) + b'{"index": 4')
    st2 = FileStorage(str(tmp_path))
    loaded = st2.load()
    assert [e.index for e in loaded.entries] == [1, 2, 3]
    assert os.path.getsize(log_path) == size  # torn tail truncated away
    # Appends after recovery land cleanly.
    st2.append_entries(entries_from_terms([1, 2, 3, 4])[3:])
    st2.close()
    st3 = FileStorage(str(tmp_path))
    assert [e.index for e in st3.load().entries] == [1, 2, 3, 4]
    st3.close()


def test_corrupt_record_stops_scan(tmp_path) -> None:
    st = FileStorage(str(tmp_path))
    st.append_entries(entries_from_terms([1, 2, 3]))
    st.close()
    log_path = os.path.join(str(tmp_path), "log.bin")
    raw = open(log_path, "rb").read()
    # Flip a byte inside the last record's payload: CRC must reject it.
    with open(log_path, "wb") as f:
        f.write(raw[:-2] + bytes([raw[-2] ^ 0xFF]) + raw[-1:])
    st2 = FileStorage(str(tmp_path))
    assert [e.index for e in st2.load().entries] == [1, 2]
    st2.close()


def test_snapshot_roundtrip_and_log_compaction(tmp_path) -> None:
    st = FileStorage(str(tmp_path))
    entries = entries_from_terms([1, 1, 2, 2, 3])
    st.append_entries(entries)
    snap = SnapshotState(last_index=3, last_term=2, config=("n1", "n2"),
                         data=json.dumps({"data": {"k": "v"}, "sessions": {}}))
    st.save_snapshot(snap, entries[3:])
    st.close()
    st2 = FileStorage(str(tmp_path))
    loaded = st2.load()
    assert loaded.snapshot.last_index == 3
    assert loaded.snapshot.config == ("n1", "n2")
    assert [e.index for e in loaded.entries] == [4, 5]
    st2.close()


def test_crash_between_snapshot_and_log_rewrite(tmp_path) -> None:
    """If we crash after snapshot.json lands but before the log rewrite,
    stale entries at or below the snapshot point must be dropped on load."""
    st = FileStorage(str(tmp_path))
    entries = entries_from_terms([1, 1, 2, 2, 3])
    st.append_entries(entries)
    st.close()
    snap = {"last_index": 4, "last_term": 2, "config": ["n1"], "data": ""}
    with open(os.path.join(str(tmp_path), "snapshot.json"), "w") as f:
        json.dump(snap, f)
    st2 = FileStorage(str(tmp_path))
    loaded = st2.load()
    assert loaded.snapshot.last_index == 4
    assert [e.index for e in loaded.entries] == [5]
    st2.close()


def test_atomic_hardstate_leaves_no_temp(tmp_path) -> None:
    st = FileStorage(str(tmp_path))
    for term in range(1, 20):
        st.save_term_vote(term, None)
    st.close()
    assert not [p for p in os.listdir(str(tmp_path)) if p.endswith(".tmp")]
