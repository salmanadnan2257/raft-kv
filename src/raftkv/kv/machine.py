"""Replicated key-value state machine with client sessions.

Operations: get / put / delete / cas.

Sessions and exactly-once semantics: every write carries (client_id, seq).
The machine remembers, per client, the highest seq applied and its result.
Re-applying a duplicate (same client_id, seq <= last seq) returns the cached
result without mutating state. Because this table is part of the replicated
state (it rides in snapshots too), a client that retries a write against a
new leader after failover gets the original result instead of a double
apply. Sessions are implicit: the first write from a client_id creates one.

Reads (get) never mutate state and are served through ReadIndex, not the log.
"""

from __future__ import annotations

import json
from typing import Any

from ..raft.messages import Entry

Result = dict[str, Any]


class KVMachine:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}
        self.sessions: dict[str, tuple[int, Result]] = {}

    def apply(self, entry: Entry) -> Result:
        cmd = entry.data
        client, seq = cmd.get("client"), cmd.get("seq")
        if client is not None and seq is not None:
            cached = self.sessions.get(client)
            if cached is not None and seq <= cached[0]:
                return cached[1]
        result = self._execute(cmd)
        if client is not None and seq is not None:
            self.sessions[client] = (seq, result)
        return result

    def _execute(self, cmd: dict[str, Any]) -> Result:
        op, key = cmd["op"], cmd["key"]
        if op == "put":
            self.data[key] = cmd["value"]
            return {"ok": True}
        if op == "delete":
            existed = key in self.data
            self.data.pop(key, None)
            return {"ok": True, "existed": existed}
        if op == "cas":
            current = self.data.get(key)
            if current == cmd["expected"]:
                if cmd["value"] is None:
                    self.data.pop(key, None)
                else:
                    self.data[key] = cmd["value"]
                return {"ok": True, "swapped": True, "value": current}
            return {"ok": True, "swapped": False, "value": current}
        raise ValueError(f"unknown op: {op}")

    def get(self, key: str) -> str | None:
        return self.data.get(key)

    def snapshot(self) -> str:
        return json.dumps({"data": self.data, "sessions": self.sessions})

    def restore(self, blob: str) -> None:
        d = json.loads(blob)
        self.data = d["data"]
        self.sessions = {c: (s[0], s[1]) for c, s in d["sessions"].items()}
