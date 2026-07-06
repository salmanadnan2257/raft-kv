# Architecture

## Module map

```
src/raftkv/
  raft/        consensus core (no IO, no clocks, no sockets)
    node.py      RaftNode: elections, replication, commit, snapshots, membership
    log.py       snapshot-aware log index arithmetic
    storage.py   Storage interface + FileStorage (fsync) + MemoryStorage (sim)
    messages.py  RPC dataclasses, dict serialization
    env.py       Env protocol: now / call_later / send / rng
  kv/          replicated state machine (get/put/delete/cas + sessions)
  transport/   TcpTransport (asyncio, length-prefixed JSON) + AsyncioEnv
  sim/         SimClock, SimNetwork/SimEnv, chaos harness, seed batch runner
  checker/     history recording + linearizability checker
  server.py    one TCP node: storage + machine + node + transport
  client.py    retrying TCP client with leader hints and write dedup
  cli/         raftkv command (cluster mgmt, KV ops, sim sweeps)
```

## The sans-IO core

`RaftNode` never calls `asyncio`, `time`, `socket`, or the global `random`.
It reacts to three stimuli: `handle_message`, timer callbacks it scheduled
through `Env.call_later`, and local API calls (`propose`, `read_index`,
`add_server`, `remove_server`). All effects go out through `Env.send` and
the `Storage` interface.

Two Env implementations exist:

- `AsyncioEnv`: `loop.call_later`, real TCP sends, wall clock.
- `SimEnv`: virtual clock events, simulated network with seeded loss,
  duplication, delay, and partitions, plus a per-node timer scale that
  models clock skew on election timeouts.

Because the core is identical in both, every bug the simulator finds is a
bug in the production code path, not in a test double.

## Raft specifics

- Elections: randomized timeouts in [T, 2T], vote persistence before the
  reply leaves the node, the up-to-date log check from section 5.4.1.
- Replication: nextIndex/matchIndex per follower, fast backoff via a
  conflict hint (first index of the conflicting term) instead of
  one-entry-at-a-time decrement.
- Commit: median-of-matchIndex over the current config, restricted to
  entries of the leader's own term (section 5.4.2); a fresh leader appends
  a no-op so older entries can commit through it.
- Persistence: `hardstate.json` (term/vote) is written atomically via
  temp file + fsync + rename. `log.bin` is an append-only record file
  (length, CRC32, JSON payload) fsync'd per append; a torn tail from a
  crash mid-write fails its CRC and is truncated on recovery. This is safe
  because nothing is acknowledged before fsync returns.
- Snapshots: when the log grows past a threshold, the applied prefix is
  replaced by a state machine snapshot (KV data + session table + config).
  Followers too far behind receive `InstallSnapshot`. The snapshot is a
  single JSON blob sent in one message, fine at this scale, not chunked.

## Membership: single-server changes

Membership uses the dissertation's single-server approach (one add or
remove per config entry) rather than joint consensus. Rationale: any two
configs that differ by one server share an overlapping majority, so two
leaders cannot be elected in disjoint quorums during the transition; the
mechanism reuses plain log replication with one extra rule (a new change
is rejected until the previous config entry commits). Joint consensus
buys arbitrary config jumps at the cost of a two-phase protocol and a
"both quorums" special state; for this project's scope the single-change
approach is simpler to implement, test, and reason about. A config entry
takes effect on append, not commit, per the dissertation. A leader that
removes itself keeps leading until the entry commits, then steps down.
Joining nodes start with an empty config, never campaign, and learn the
cluster from the leader's log or snapshot.

Not implemented from the dissertation: pre-vote and leader stickiness,
and automatic catch-up rounds before adding a server (an added server that
is far behind briefly raises the commit quorum until it catches up).

## Linearizable reads: ReadIndex

`get` does not go through the log. The leader:

1. refuses unless an entry of its own term has committed (the no-op),
2. records `readIndex = commitIndex`,
3. broadcasts a heartbeat round carrying a sequence number and waits for
   acks from a majority for that round (proof it is still the leader),
4. waits until `lastApplied >= readIndex`, then reads local state.

A deposed leader in a minority partition can never collect the round of
acks, so it can never serve a stale read; the tests exercise exactly that.

The alternative not taken: leader leases (serve reads for
`election_timeout - clock_drift` after a heartbeat quorum without a
per-read round). Leases save a round trip per read but their safety
depends on a bounded clock drift assumption across machines. ReadIndex
costs one heartbeat round and needs no clock assumptions, which fits a
project whose whole point is testable correctness. The heartbeat round is
shared: concurrent reads piggyback on the same sequence number window.

## Client sessions and exactly-once writes

Every write carries `(client_id, seq)`. The state machine keeps, per
client, the last applied seq and its result; duplicates return the cached
result without re-executing. The table is part of the replicated state
and rides in snapshots, so a retry that lands on a new leader after
failover still deduplicates. That upgrades at-least-once retries into
exactly-once apply semantics (tested with a cas retried across a leader
crash: the retry returns the original "swapped" result instead of
re-executing and failing). Sessions are implicit and never expire, which
is an accepted memory leak at this scale; production systems expire them
with a session TTL agreed through the log.
