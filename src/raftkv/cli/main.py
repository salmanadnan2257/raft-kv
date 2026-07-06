"""raftkv command line interface.

Cluster management spawns one OS process per node, so `kill -9` on a node
is a genuine crash test. Client commands read the cluster layout from
$RAFTKV_HOME/cluster.json (default ./.raftkv).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from typing import Any

from ..client import ClusterError, RaftClient
from ..server import RaftServer
from ..sim.batch import run_batch


def _home(args: argparse.Namespace) -> str:
    return os.path.abspath(args.home or os.environ.get("RAFTKV_HOME", ".raftkv"))


def _load_cluster(home: str) -> dict[str, Any]:
    path = os.path.join(home, "cluster.json")
    if not os.path.exists(path):
        raise SystemExit(f"no cluster at {path}; run `raftkv cluster start` first")
    with open(path) as f:
        return json.load(f)


def _addresses(cluster: dict[str, Any]) -> dict[str, tuple[str, int]]:
    return {nid: (n["host"], n["port"]) for nid, n in cluster["nodes"].items()}


def _client(args: argparse.Namespace) -> RaftClient:
    return RaftClient(_addresses(_load_cluster(_home(args))))


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


# ------------------------------------------------------------------ commands

def cmd_server(args: argparse.Namespace) -> int:
    addresses: dict[str, tuple[str, int]] = {}
    for part in args.cluster.split(","):
        nid, addr = part.split("=", 1)
        host, port = addr.rsplit(":", 1)
        addresses[nid.strip()] = (host, int(port))
    initial = sorted(addresses) if not args.join else []

    async def run() -> None:
        server = RaftServer(args.id, addresses, args.data_dir,
                            snapshot_threshold=args.snapshot_threshold,
                            initial_config=initial)
        await server.start()
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        print(f"[{args.id}] listening on "
              f"{addresses[args.id][0]}:{addresses[args.id][1]}", flush=True)
        await stop.wait()
        await server.stop()

    asyncio.run(run())
    return 0


def cmd_cluster_start(args: argparse.Namespace) -> int:
    home = _home(args)
    os.makedirs(home, exist_ok=True)
    path = os.path.join(home, "cluster.json")
    if os.path.exists(path):
        cluster = json.load(open(path))
        if any(_alive(n["pid"]) for n in cluster["nodes"].values()):
            raise SystemExit(f"cluster already running (see {path})")
    nodes = {f"n{i + 1}": {"host": "127.0.0.1", "port": args.base_port + i,
                           "data_dir": os.path.join(home, f"n{i + 1}")}
             for i in range(args.nodes)}
    spec = ",".join(f"{nid}={n['host']}:{n['port']}" for nid, n in nodes.items())
    for nid, n in nodes.items():
        log_path = os.path.join(home, f"{nid}.log")
        with open(log_path, "ab") as log:
            proc = subprocess.Popen(
                [sys.executable, "-m", "raftkv.cli.main", "server",
                 "--id", nid, "--data-dir", n["data_dir"], "--cluster", spec],
                stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True)
        n["pid"] = proc.pid
    with open(path, "w") as f:
        json.dump({"nodes": nodes}, f, indent=2)
    print(f"started {args.nodes} nodes (base port {args.base_port}), "
          f"state in {home}")
    # Wait for a leader so the cluster is usable when we return.
    client = RaftClient(_addresses({"nodes": nodes}))
    for _ in range(50):
        for nid in nodes:
            st = asyncio.run(client.status(nid))
            if st and st["leader"]:
                print(f"leader elected: {st['leader']} (term {st['term']})")
                return 0
        time.sleep(0.3)
    print("warning: no leader observed yet; check node logs", file=sys.stderr)
    return 1


def cmd_cluster_stop(args: argparse.Namespace) -> int:
    home = _home(args)
    cluster = _load_cluster(home)
    for nid, n in cluster["nodes"].items():
        if _alive(n["pid"]):
            os.kill(n["pid"], signal.SIGTERM)
            print(f"stopped {nid} (pid {n['pid']})")
    os.remove(os.path.join(home, "cluster.json"))
    return 0


def cmd_cluster_status(args: argparse.Namespace) -> int:
    cluster = _load_cluster(_home(args))
    client = RaftClient(_addresses(cluster))

    async def run() -> None:
        for nid, n in cluster["nodes"].items():
            st = await client.status(nid)
            alive = _alive(n["pid"])
            if st is None:
                print(f"{nid} pid={n['pid']} port={n['port']} "
                      f"{'alive' if alive else 'dead'} (no response)")
            else:
                print(f"{nid} pid={n['pid']} port={n['port']} role={st['role']} "
                      f"term={st['term']} commit={st['commit_index']} "
                      f"leader={st['leader']}")

    asyncio.run(run())
    return 0


def _run_client(args: argparse.Namespace, coro_factory: Any) -> int:
    client = _client(args)
    try:
        out = asyncio.run(coro_factory(client))
    except ClusterError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if out is not None:
        print(out)
    return 0


def cmd_put(args: argparse.Namespace) -> int:
    async def go(c: RaftClient) -> str:
        await c.put(args.key, args.value)
        return "OK"
    return _run_client(args, go)


def cmd_get(args: argparse.Namespace) -> int:
    async def go(c: RaftClient) -> str:
        value = await c.get(args.key)
        return "(nil)" if value is None else value
    return _run_client(args, go)


def cmd_del(args: argparse.Namespace) -> int:
    async def go(c: RaftClient) -> str:
        existed = await c.delete(args.key)
        return "deleted" if existed else "(nil)"
    return _run_client(args, go)


def cmd_cas(args: argparse.Namespace) -> int:
    async def go(c: RaftClient) -> str:
        expected = None if args.expected == "-" else args.expected
        swapped, old = await c.cas(args.key, expected, args.value)
        return f"swapped (was {old!r})" if swapped else f"failed (current {old!r})"
    return _run_client(args, go)


def cmd_sim(args: argparse.Namespace) -> int:
    return run_batch(args.start, args.seeds, args.workers)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="raftkv",
                                 description="Raft-replicated key-value store")
    sub = ap.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("server", help="run one node (foreground)")
    sp.add_argument("--id", required=True)
    sp.add_argument("--data-dir", required=True)
    sp.add_argument("--cluster", required=True,
                    help="comma list of id=host:port for all nodes")
    sp.add_argument("--snapshot-threshold", type=int, default=1000)
    sp.add_argument("--join", action="store_true",
                    help="start with empty config and wait to be added")
    sp.set_defaults(fn=cmd_server)

    cp = sub.add_parser("cluster", help="manage a local cluster")
    csub = cp.add_subparsers(dest="cluster_command", required=True)
    c_start = csub.add_parser("start", help="start a local N-node cluster")
    c_start.add_argument("-n", "--nodes", type=int, default=3)
    c_start.add_argument("--base-port", type=int, default=7101)
    c_start.add_argument("--home")
    c_start.set_defaults(fn=cmd_cluster_start)
    c_stop = csub.add_parser("stop", help="stop the local cluster")
    c_stop.add_argument("--home")
    c_stop.set_defaults(fn=cmd_cluster_stop)
    c_status = csub.add_parser("status", help="show each node's view")
    c_status.add_argument("--home")
    c_status.set_defaults(fn=cmd_cluster_status)

    for name, fn, extra in (
        ("put", cmd_put, ["key", "value"]),
        ("get", cmd_get, ["key"]),
        ("del", cmd_del, ["key"]),
        ("cas", cmd_cas, ["key", "expected", "value"]),
    ):
        p = sub.add_parser(name, help=f"{name} a key" if name != "cas"
                           else "compare-and-swap (use - for expected-absent)")
        for a in extra:
            p.add_argument(a)
        p.add_argument("--home")
        p.set_defaults(fn=fn)

    sim = sub.add_parser("sim", help="run the deterministic simulation sweep")
    sim.add_argument("--seeds", type=int, default=200)
    sim.add_argument("--start", type=int, default=0)
    sim.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    sim.set_defaults(fn=cmd_sim)

    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
