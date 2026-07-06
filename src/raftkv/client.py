"""TCP client for a raftkv cluster.

Follows leader hints, retries with backoff on unreachable nodes and
elections in progress. Writes carry a (client_id, seq) pair so a retry
after a lost response is deduplicated by the replicated session table:
the operation applies exactly once even across leader failover.
"""

from __future__ import annotations

import asyncio
import itertools
import uuid
from typing import Any

from .transport.tcp import read_frame, write_frame


class ClusterError(Exception):
    pass


class RaftClient:
    def __init__(self, addresses: dict[str, tuple[str, int]],
                 client_id: str | None = None, *,
                 request_timeout: float = 3.0, total_timeout: float = 15.0) -> None:
        self.addresses = dict(addresses)
        self.client_id = client_id or f"cli-{uuid.uuid4().hex[:12]}"
        self.request_timeout = request_timeout
        self.total_timeout = total_timeout
        self._seq = itertools.count(1)
        self._leader_hint: str | None = None
        self._req_id = itertools.count(1)

    async def _call_node(self, node_id: str, req: dict[str, Any]) -> dict[str, Any]:
        host, port = self.addresses[node_id]
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=self.request_timeout)
        try:
            rid = next(self._req_id)
            write_frame(writer, {"kind": "client", "id": rid, "req": req})
            await writer.drain()
            reply = await asyncio.wait_for(read_frame(reader),
                                           timeout=self.request_timeout)
            if reply is None or reply.get("id") != rid:
                raise ClusterError("connection closed mid-request")
            return reply
        finally:
            writer.close()

    async def request(self, req: dict[str, Any]) -> dict[str, Any]:
        """Send req to the cluster, following leader hints, until it sticks."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.total_timeout
        candidates = list(self.addresses)
        i = 0
        last_error = "no nodes reachable"
        while loop.time() < deadline:
            if self._leader_hint in self.addresses:
                target = self._leader_hint
            else:
                target = candidates[i % len(candidates)]
                i += 1
            try:
                reply = await self._call_node(target, req)
            except (OSError, asyncio.TimeoutError, ClusterError) as exc:
                last_error = f"{target}: {type(exc).__name__}"
                self._leader_hint = None
                await asyncio.sleep(0.15)
                continue
            if reply.get("ok"):
                return reply
            error = reply.get("error")
            if error == "not_leader":
                hint = reply.get("leader")
                self._leader_hint = hint if hint != target else None
                if self._leader_hint is None:
                    await asyncio.sleep(0.15)
                continue
            if error in ("leadership_lost", "read_timeout"):
                self._leader_hint = None
                last_error = str(error)
                await asyncio.sleep(0.15)
                continue
            raise ClusterError(str(error))
        raise ClusterError(f"cluster unavailable ({last_error})")

    # ------------------------------------------------------------ operations

    async def put(self, key: str, value: str) -> None:
        await self.request({"op": "put", "key": key, "value": value,
                            "client": self.client_id, "seq": next(self._seq)})

    async def get(self, key: str) -> str | None:
        reply = await self.request({"op": "get", "key": key})
        return reply["result"]["value"]

    async def delete(self, key: str) -> bool:
        reply = await self.request({"op": "delete", "key": key,
                                    "client": self.client_id,
                                    "seq": next(self._seq)})
        return bool(reply["result"]["existed"])

    async def cas(self, key: str, expected: str | None,
                  value: str | None) -> tuple[bool, str | None]:
        reply = await self.request({"op": "cas", "key": key, "expected": expected,
                                    "value": value, "client": self.client_id,
                                    "seq": next(self._seq)})
        return bool(reply["result"]["swapped"]), reply["result"]["value"]

    async def status(self, node_id: str) -> dict[str, Any] | None:
        try:
            reply = await self._call_node(node_id, {"op": "status"})
        except (OSError, asyncio.TimeoutError, ClusterError):
            return None
        return reply.get("status")

    async def add_server(self, node_id: str, host: str, port: int) -> list[str]:
        reply = await self.request({"op": "add_server", "node_id": node_id,
                                    "host": host, "port": port})
        return list(reply["config"])

    async def remove_server(self, node_id: str) -> list[str]:
        reply = await self.request({"op": "remove_server", "node_id": node_id})
        return list(reply["config"])
