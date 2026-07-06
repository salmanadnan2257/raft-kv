"""Real TCP transport over asyncio streams.

Frame format: 4-byte big-endian payload length, then a JSON object.
Peer traffic and client traffic share one listening port; the first field
of each frame distinguishes them ("raft" vs client request types).

Peer sends are fire-and-forget through a persistent per-peer connection
with reconnect on failure, matching Raft's assumption of a lossy network:
if a connection is down the message is simply dropped and a later
heartbeat retries.
"""

from __future__ import annotations

import asyncio
import json
import random
from typing import Any, Awaitable, Callable

from ..raft.messages import Message, message_from_dict

MAX_FRAME = 64 * 1024 * 1024


async def read_frame(reader: asyncio.StreamReader) -> dict[str, Any] | None:
    try:
        header = await reader.readexactly(4)
    except (asyncio.IncompleteReadError, ConnectionError):
        return None
    length = int.from_bytes(header, "big")
    if length > MAX_FRAME:
        return None
    try:
        payload = await reader.readexactly(length)
    except (asyncio.IncompleteReadError, ConnectionError):
        return None
    return json.loads(payload)


def write_frame(writer: asyncio.StreamWriter, obj: dict[str, Any]) -> None:
    payload = json.dumps(obj, separators=(",", ":")).encode()
    writer.write(len(payload).to_bytes(4, "big") + payload)


class AsyncioEnv:
    """Env implementation over the running asyncio loop."""

    def __init__(self, transport: "TcpTransport") -> None:
        self.transport = transport
        self.rng = random.Random()

    def now(self) -> float:
        return asyncio.get_running_loop().time()

    def call_later(self, delay: float, fn: Callable[[], None]) -> asyncio.TimerHandle:
        return asyncio.get_running_loop().call_later(delay, fn)

    def send(self, dest: str, msg: Message) -> None:
        self.transport.send(dest, msg)


class TcpTransport:
    def __init__(self, node_id: str, addresses: dict[str, tuple[str, int]],
                 on_raft_message: Callable[[str, Message], None],
                 on_client_frame: Callable[
                     [dict[str, Any], asyncio.StreamWriter], Awaitable[None]]) -> None:
        self.node_id = node_id
        self.addresses = dict(addresses)
        self.on_raft_message = on_raft_message
        self.on_client_frame = on_client_frame
        self._server: asyncio.Server | None = None
        self._peer_queues: dict[str, asyncio.Queue[Message]] = {}
        self._peer_tasks: dict[str, asyncio.Task[None]] = {}
        self._conns: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        host, port = self.addresses[self.node_id]
        self._server = await asyncio.start_server(self._handle_conn, host, port)

    async def stop(self) -> None:
        for task in self._peer_tasks.values():
            task.cancel()
        if self._peer_tasks:
            await asyncio.gather(*self._peer_tasks.values(), return_exceptions=True)
        for writer in list(self._conns):
            writer.close()
        if self._server is not None:
            self._server.close()
            # wait_closed also waits for in-flight connection handlers,
            # which exit once their writers are closed above.
            await self._server.wait_closed()

    def add_peer(self, node_id: str, addr: tuple[str, int]) -> None:
        self.addresses[node_id] = addr

    def send(self, dest: str, msg: Message) -> None:
        if dest not in self.addresses:
            return
        q = self._peer_queues.get(dest)
        if q is None:
            q = asyncio.Queue(maxsize=256)
            self._peer_queues[dest] = q
            self._peer_tasks[dest] = asyncio.get_running_loop().create_task(
                self._peer_writer(dest, q))
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            pass  # lossy by design; heartbeats retransmit

    async def _peer_writer(self, dest: str, q: asyncio.Queue[Message]) -> None:
        writer: asyncio.StreamWriter | None = None
        try:
            while True:
                msg = await q.get()
                if writer is None:
                    host, port = self.addresses[dest]
                    try:
                        _, writer = await asyncio.wait_for(
                            asyncio.open_connection(host, port), timeout=1.0)
                        write_frame(writer, {"kind": "hello", "from": self.node_id})
                    except (OSError, asyncio.TimeoutError):
                        writer = None
                        continue  # drop msg; peer unreachable
                try:
                    write_frame(writer, {"kind": "raft", "from": self.node_id,
                                         "msg": msg.to_dict()})
                    await writer.drain()
                except (ConnectionError, OSError):
                    writer.close()
                    writer = None
        except asyncio.CancelledError:
            pass
        finally:
            if writer is not None:
                writer.close()

    async def _handle_conn(self, reader: asyncio.StreamReader,
                           writer: asyncio.StreamWriter) -> None:
        self._conns.add(writer)
        try:
            while True:
                frame = await read_frame(reader)
                if frame is None:
                    break
                kind = frame.get("kind")
                if kind == "hello":
                    continue  # peer connection preamble, nothing to do
                if kind == "raft":
                    self.on_raft_message(frame["from"],
                                         message_from_dict(frame["msg"]))
                else:
                    await self.on_client_frame(frame, writer)
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            self._conns.discard(writer)
            writer.close()
