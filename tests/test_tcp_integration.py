"""Integration: a real 3-node TCP cluster on localhost, with FileStorage,
leader failure, failover, and restart catch-up."""

from __future__ import annotations

import asyncio
import socket

from raftkv.client import RaftClient
from raftkv.server import RaftServer


def free_ports(n: int) -> list[int]:
    socks = [socket.socket() for _ in range(n)]
    for s in socks:
        s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in socks]
    for s in socks:
        s.close()
    return ports


async def wait_leader(client: RaftClient, node_ids: list[str],
                      timeout: float = 15.0) -> str:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        for nid in node_ids:
            st = await client.status(nid)
            if st and st["role"] == "leader":
                return nid
        await asyncio.sleep(0.2)
    raise AssertionError("no leader in TCP cluster")


def test_three_node_tcp_cluster(tmp_path) -> None:
    async def scenario() -> None:
        ports = free_ports(3)
        node_ids = ["n1", "n2", "n3"]
        addrs = {nid: ("127.0.0.1", p) for nid, p in zip(node_ids, ports)}
        servers = {
            nid: RaftServer(nid, addrs, str(tmp_path / nid),
                            election_timeout=0.25, heartbeat_interval=0.05,
                            snapshot_threshold=50)
            for nid in node_ids
        }
        for s in servers.values():
            await s.start()
        client = RaftClient(addrs, client_id="itest")
        try:
            leader = await wait_leader(client, node_ids)

            await client.put("alpha", "1")
            await client.put("beta", "2")
            assert await client.get("alpha") == "1"
            swapped, old = await client.cas("alpha", "1", "10")
            assert swapped and old == "1"
            swapped, _ = await client.cas("alpha", "nope", "20")
            assert not swapped
            assert await client.get("alpha") == "10"

            # Kill the leader: the cluster must fail over with no data loss.
            await servers[leader].stop()
            await client.put("gamma", "3")
            assert await client.get("alpha") == "10"
            assert await client.get("gamma") == "3"
            second_leader = await wait_leader(
                client, [n for n in node_ids if n != leader])
            assert second_leader != leader

            # Restart the killed node from its disk state; it catches up.
            servers[leader] = RaftServer(
                leader, addrs, str(tmp_path / leader),
                election_timeout=0.25, heartbeat_interval=0.05,
                snapshot_threshold=50)
            await servers[leader].start()
            deadline = asyncio.get_running_loop().time() + 10
            while True:
                st = await client.status(leader)
                if st and st["commit_index"] >= 1 and st["leader"]:
                    ref = await client.status(second_leader)
                    if ref and st["commit_index"] >= ref["commit_index"] - 1:
                        break
                assert asyncio.get_running_loop().time() < deadline, \
                    "restarted node never caught up"
                await asyncio.sleep(0.2)

            assert await client.get("beta") == "2"
            await client.delete("beta")
            assert await client.get("beta") is None
        finally:
            for s in servers.values():
                try:
                    await s.stop()
                except Exception:
                    pass

    asyncio.run(scenario())


def test_add_and_remove_server_over_tcp(tmp_path) -> None:
    async def scenario() -> None:
        ports = free_ports(4)
        node_ids = ["n1", "n2", "n3"]
        addrs = {nid: ("127.0.0.1", p) for nid, p in zip(node_ids, ports[:3])}
        servers = {
            nid: RaftServer(nid, addrs, str(tmp_path / nid),
                            election_timeout=0.25, heartbeat_interval=0.05)
            for nid in node_ids
        }
        for s in servers.values():
            await s.start()
        client = RaftClient(addrs)
        try:
            await wait_leader(client, node_ids)
            await client.put("k", "v")

            # A joining node starts with an empty config and learns the
            # cluster from the leader once added.
            n4_addrs = {**addrs, "n4": ("127.0.0.1", ports[3])}
            servers["n4"] = RaftServer("n4", n4_addrs, str(tmp_path / "n4"),
                                       election_timeout=0.25,
                                       heartbeat_interval=0.05,
                                       initial_config=[])
            await servers["n4"].start()
            config = await client.add_server("n4", "127.0.0.1", ports[3])
            assert set(config) == {"n1", "n2", "n3", "n4"}

            deadline = asyncio.get_running_loop().time() + 10
            while servers["n4"].machine.get("k") != "v":
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.1)
            assert set(servers["n4"].node.config) == {"n1", "n2", "n3", "n4"}

            config = await client.remove_server("n4")
            assert set(config) == {"n1", "n2", "n3"}
            await client.put("k2", "v2")
            assert await client.get("k2") == "v2"
        finally:
            for s in servers.values():
                try:
                    await s.stop()
                except Exception:
                    pass

    asyncio.run(scenario())


def test_writes_survive_full_restart(tmp_path) -> None:
    async def phase1(addrs) -> None:
        servers = [RaftServer(nid, addrs, str(tmp_path / nid),
                              election_timeout=0.25, heartbeat_interval=0.05)
                   for nid in addrs]
        for s in servers:
            await s.start()
        client = RaftClient(addrs)
        await wait_leader(client, list(addrs))
        for i in range(10):
            await client.put(f"k{i}", f"v{i}")
        for s in servers:
            await s.stop()

    async def phase2(addrs) -> None:
        servers = [RaftServer(nid, addrs, str(tmp_path / nid),
                              election_timeout=0.25, heartbeat_interval=0.05)
                   for nid in addrs]
        for s in servers:
            await s.start()
        client = RaftClient(addrs)
        await wait_leader(client, list(addrs))
        for i in range(10):
            assert await client.get(f"k{i}") == f"v{i}"
        for s in servers:
            await s.stop()

    ports = free_ports(3)
    addrs = {f"n{i + 1}": ("127.0.0.1", p) for i, p in enumerate(ports)}
    asyncio.run(phase1(addrs))
    asyncio.run(phase2(addrs))
