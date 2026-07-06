"""RaftServer: one node of the real TCP cluster.

Glues together FileStorage, the KV state machine, the Raft core, and the
TCP transport, and serves client requests:

  put/delete/cas -> proposed through the Raft log, replied after apply
  get            -> served via ReadIndex (linearizable, no log entry)
  status         -> local node introspection (never redirected)
  add_server / remove_server -> single-server membership changes
"""

from __future__ import annotations

import asyncio
from typing import Any

from .kv.machine import KVMachine
from .raft.messages import Entry
from .raft.node import ConfigChangeInProgress, NotLeader, RaftNode
from .raft.storage import FileStorage
from .transport.tcp import AsyncioEnv, TcpTransport, write_frame


class RaftServer:
    def __init__(self, node_id: str, addresses: dict[str, tuple[str, int]],
                 data_dir: str, *, election_timeout: float = 0.3,
                 heartbeat_interval: float = 0.06,
                 snapshot_threshold: int = 1000,
                 initial_config: list[str] | None = None) -> None:
        self.node_id = node_id
        self.machine = KVMachine()
        self.storage = FileStorage(data_dir)
        self.transport = TcpTransport(node_id, addresses,
                                      self._on_raft_message, self._on_client_frame)
        self.env = AsyncioEnv(self.transport)
        self.node: RaftNode | None = None
        self._initial_config = initial_config if initial_config is not None \
            else sorted(addresses)
        self._election_timeout = election_timeout
        self._heartbeat_interval = heartbeat_interval
        self._snapshot_threshold = snapshot_threshold
        self._pending: dict[tuple[int, int], asyncio.Future[Any]] = {}

    async def start(self) -> None:
        await self.transport.start()
        self.node = RaftNode(
            self.node_id, self._initial_config, self.storage, self.env,
            self.machine.apply, self.machine.snapshot, self.machine.restore,
            election_timeout=self._election_timeout,
            heartbeat_interval=self._heartbeat_interval,
            snapshot_threshold=self._snapshot_threshold)
        self.node.apply_listeners.append(self._on_apply)
        self.node.step_down_listeners.append(self._on_step_down)

    async def stop(self) -> None:
        if self.node is not None:
            self.node.stop()
        await self.transport.stop()
        self.storage.close()

    # ------------------------------------------------------------ raft wiring

    def _on_raft_message(self, src: str, msg: Any) -> None:
        if self.node is not None:
            self.node.handle_message(src, msg)

    def _on_apply(self, index: int, term: int, entry: Entry, result: Any) -> None:
        fut = self._pending.pop((index, term), None)
        if fut is not None and not fut.done():
            fut.set_result(result)
        for key in [k for k in self._pending if k[0] == index and k[1] != term]:
            stale = self._pending.pop(key)
            if not stale.done():
                stale.set_result(None)

    def _on_step_down(self) -> None:
        pending, self._pending = self._pending, {}
        for fut in pending.values():
            if not fut.done():
                fut.set_result(None)

    # ---------------------------------------------------------- client frames

    async def _on_client_frame(self, frame: dict[str, Any],
                               writer: asyncio.StreamWriter) -> None:
        req = frame.get("req", {})
        reply = await self.handle_request(req)
        reply["kind"] = "reply"
        reply["id"] = frame.get("id")
        write_frame(writer, reply)
        try:
            await writer.drain()
        except (ConnectionError, OSError):
            pass

    async def handle_request(self, req: dict[str, Any]) -> dict[str, Any]:
        node = self.node
        assert node is not None
        op = req.get("op")
        if op == "status":
            return {"ok": True, "status": node.status()}
        try:
            if op == "get":
                return await self._handle_read(req["key"])
            if op in ("put", "delete", "cas"):
                return await self._handle_write(req)
            if op == "add_server":
                self.transport.add_peer(req["node_id"],
                                        (req["host"], req["port"]))
                index, term = node.add_server(req["node_id"])
                return await self._await_commit(index, term)
            if op == "remove_server":
                index, term = node.remove_server(req["node_id"])
                return await self._await_commit(index, term)
        except NotLeader as exc:
            return {"ok": False, "error": "not_leader", "leader": exc.leader_hint}
        except ConfigChangeInProgress:
            return {"ok": False, "error": "config_change_in_progress"}
        return {"ok": False, "error": f"unknown op {op!r}"}

    async def _handle_write(self, req: dict[str, Any]) -> dict[str, Any]:
        node = self.node
        assert node is not None
        cmd = {k: req[k] for k in
               ("op", "key", "value", "expected", "client", "seq") if k in req}
        index, term = node.propose(cmd)
        result = await self._register(index, term)
        if result is None:
            return {"ok": False, "error": "leadership_lost"}
        return {"ok": True, "result": result}

    async def _await_commit(self, index: int, term: int) -> dict[str, Any]:
        result = await self._register(index, term)
        del result  # config entries carry no result; commit is the answer
        node = self.node
        assert node is not None
        if node.commit_index >= index:
            return {"ok": True, "config": list(node.config)}
        return {"ok": False, "error": "leadership_lost"}

    async def _register(self, index: int, term: int) -> Any:
        fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[(index, term)] = fut
        try:
            return await asyncio.wait_for(fut, timeout=5.0)
        except asyncio.TimeoutError:
            self._pending.pop((index, term), None)
            return None

    async def _handle_read(self, key: str) -> dict[str, Any]:
        node = self.node
        assert node is not None
        if node.role.value != "leader":
            return {"ok": False, "error": "not_leader", "leader": node.leader_hint}
        fut: asyncio.Future[int | None] = asyncio.get_running_loop().create_future()
        node.read_index(lambda ri: fut.done() or fut.set_result(ri))
        try:
            ri = await asyncio.wait_for(fut, timeout=2.0)
        except asyncio.TimeoutError:
            return {"ok": False, "error": "read_timeout"}
        if ri is None:
            return {"ok": False, "error": "not_leader", "leader": node.leader_hint}
        return {"ok": True, "result": {"value": self.machine.get(key)}}
