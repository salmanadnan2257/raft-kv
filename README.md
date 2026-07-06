# raft-kv

A distributed key-value store with Raft consensus implemented from scratch
in Python 3.12 (asyncio, zero runtime dependencies), plus the part I care
most about: a deterministic simulation harness that runs thousands of
seeded chaos schedules against the real consensus code, and a
linearizability checker, also written from scratch, that verifies the
client histories those runs produce.

The Raft implementation follows Diego Ongaro and John Ousterhout,
"In Search of an Understandable Consensus Algorithm (Extended Version)"
(USENIX ATC 2014), with log compaction and single-server membership
changes from Ongaro's dissertation, "Consensus: Bridging Theory and
Practice" (Stanford, 2014). No Raft or consensus libraries are used, and
the linearizability checker is not a porcupine port.

## Why it exists

Consensus code is easy to write and nearly impossible to trust from
example-based tests, because the bugs live in message interleavings you
will never hit on a laptop loopback. This project takes the FoundationDB
position seriously: the system should be testable as a deterministic
function of a seed. The same `RaftNode` class runs unmodified on a real
TCP cluster and inside a simulator where time, delivery order, loss,
partitions, crashes, and clock skew are all derived from one seeded RNG.
A failing seed replays its exact schedule, every time.

## Features

- Raft core: randomized-timeout leader election, log replication with
  nextIndex/matchIndex and fast conflict backoff, commit advancement
  restricted to current-term entries (with the new-leader no-op),
  fsync'd crash-safe persistence of term/vote/log (CRC-framed append-only
  log that truncates torn tails on recovery), snapshot-based log
  compaction with InstallSnapshot, and single-server membership changes
  (add/remove, including leader self-removal).
- KV state machine: get / put / delete / cas, with client sessions that
  deduplicate retried writes for exactly-once apply semantics across
  leader failover (the session table is replicated and snapshotted).
- Linearizable reads through ReadIndex (heartbeat-quorum confirmation,
  no clock assumptions); the leader-lease alternative and why it was not
  taken are discussed in docs/ARCHITECTURE.md.
- Two transports behind one Env interface: asyncio TCP with
  length-prefixed JSON frames, and the deterministic simulated network
  (virtual clock, seeded delay/loss/duplication/partitions, per-node
  election-timer skew).
- Deterministic chaos harness: concurrent clients, nemesis-injected
  partitions/crashes/restarts/skew, continuous invariant checking
  (election safety, log matching, leader completeness, state machine
  safety), every violation reported with its seed.
- Linearizability checker: Wing & Gong search with Lowe-style
  memoization, per-key decomposition, both-ways branching for
  incomplete operations, and an explicit UNKNOWN outcome when the state
  budget is exceeded. Known-bad synthetic histories in the test suite
  prove it actually rejects violations.
- CLI: `raftkv cluster start -n 3` runs a real multi-process cluster on
  localhost; `raftkv put/get/del/cas` client commands; `raftkv sim`
  drives seed sweeps.

## Architecture (short)

```
raft/       sans-IO consensus core: no sockets, no clocks, no asyncio
kv/         replicated state machine + session dedup table
transport/  TCP transport + AsyncioEnv (production Env)
sim/        SimClock + SimNetwork/SimEnv (test Env), chaos harness, batch runner
checker/    history recording + linearizability search
server.py   one TCP node; client.py: retrying client; cli/: raftkv command
```

`RaftNode` only ever touches the world through `Env` (now, call_later,
send, rng) and `Storage` (load, append, truncate, snapshot). Swap those
two and the identical consensus code runs under the simulator or over
TCP. Details in docs/ARCHITECTURE.md; the testing philosophy is in
docs/TESTING.md.

## Setup

Python 3.12+. No runtime dependencies; pytest for development.

```
python3.12 -m venv .venv && . .venv/bin/activate   # or your preferred venv path
pip install -e ".[dev]"
```

The only environment variable is `RAFTKV_HOME` (where `raftkv cluster`
keeps cluster.json, node data, and logs; defaults to ./.raftkv). See
.env.example.

## Usage

```
raftkv cluster start -n 3        # real processes, one per node
raftkv cluster status
raftkv put city lahore
raftkv get city
raftkv cas city lahore karachi   # use - for expected-absent
raftkv del city
raftkv cluster stop

raftkv sim --seeds 200           # deterministic chaos sweep
raftkv sim --seeds 5000 --workers 8      # the long run
raftkv sim --start 1337 --seeds 1 --workers 1   # replay one seed exactly

pytest -m "not slow"             # ~15s
pytest                           # includes the 200-seed sweep
```

## Demo transcript

`demo.sh` output, run for real on this machine (`kill -9` on the actual
leader process):

```
=== raftkv demo (cluster state in /tmp/raftkv-demo.48w3Ui) ===

$ raftkv cluster start -n 3
started 3 nodes (base port 7101), state in /tmp/raftkv-demo.48w3Ui
leader elected: n2 (term 1)

$ raftkv cluster status
n1 pid=89806 port=7101 role=follower term=1 commit=1 leader=n2
n2 pid=89807 port=7102 role=leader term=1 commit=1 leader=n2
n3 pid=89808 port=7103 role=follower term=1 commit=1 leader=n2

$ raftkv put city lahore
OK

$ raftkv put team raptors
OK

$ raftkv cas team raptors dinos
swapped (was 'raptors')

$ raftkv get city
lahore

$ raftkv get team
dinos

=== killing leader n2 (pid 89807) with SIGKILL ===

$ raftkv cluster status
n1 pid=89806 port=7101 role=leader term=2 commit=5 leader=n1
n2 pid=89807 port=7102 dead (no response)
n3 pid=89808 port=7103 role=follower term=2 commit=5 leader=n1

=== data survives, writes keep working ===

$ raftkv get city
lahore

$ raftkv get team
dinos

$ raftkv put after-failover yes
OK

$ raftkv get after-failover
yes

$ raftkv cluster stop
stopped n1 (pid 89806)
stopped n3 (pid 89808)

=== demo complete: leader killed, failover elected, no data lost ===
```

## Verification results

On this machine (Linux, Python 3.12.3):

- `pytest`: 57 tests, all passing, including the paper's Figure 7
  replication scenarios, torn-write recovery, membership changes over
  both transports, exactly-once failover retries, ReadIndex staleness
  tests, checker known-bad histories, the 200-seed slow sweep, and real
  3-node TCP cluster integration tests (failover, restart catch-up,
  add/remove server).
- Long randomized run, actual output:

  ```
  $ raftkv sim --seeds 5000 --workers 10
  5000 seeds in 84.7s (59.0 seeds/s), 564170 client ops total, 0 failure(s)
  ```

  Each seed is 12 virtual seconds of a 3-node cluster under partitions,
  crashes, restarts, 5% message loss, 5% duplication, and election-timer
  skew, with every completed client history checked for linearizability.
- The harness's teeth are themselves tested: weakening the quorum rule
  makes the sweep detect split brain within a few dozen seeds.

## Honest limits

- Scale: built and tested for single-digit node counts and localhost.
  Throughput is bounded by Python, JSON serialization, and one fsync per
  append batch; this is a correctness project, not a fast store.
- Checker window: linearizability checking is NP-complete; the checker
  handles at most 63 operations per key per history and gives up with
  UNKNOWN (never a false PASS or FAIL) past its state budget. Chaos-run
  histories here fit comfortably; adversarial wide histories will not.
- Snapshots ship as one message, not chunks; fine at this scale.
- No pre-vote: a healed, previously partitioned node forces one
  re-election through term inflation before rejoining quietly.
- Client sessions never expire, so the dedup table grows with distinct
  client ids.
- Membership changes replicate to a new server only after the config
  entry is appended (no pre-join catch-up rounds), so adding a far-behind
  server briefly raises the effective commit quorum.

## Challenges

- Python 3.12 changed `asyncio.Server.wait_closed()` to also wait for
  in-flight connection handlers. Server shutdown deadlocked because
  client connections sat blocked in `readexactly` forever; the TCP
  integration test hung for minutes with ~0 CPU. Fix: track every
  accepted `StreamWriter` and close them before awaiting `wait_closed()`.
- Exactly-once dedup bit its own test harness. The test helper issued
  writes under one client id with random seq numbers, and the session
  table (correctly) discarded any write whose seq was lower than one it
  had seen, so later writes silently vanished and two unrelated tests
  failed. The fix was a monotonic per-client counter, and the lesson is
  baked into the client library, which allocates seqs from a counter.
- A fresh leader cannot serve ReadIndex reads before committing an entry
  in its own term (its commitIndex may still reflect a stale view of
  what is durable). Pending reads register with no read index and get
  one assigned the moment the leader's no-op commits; the failover read
  test drove this design.
- The Figure 7 tests could not be set up through real elections: cases
  (c) and (d) hold logs more up-to-date than the intended leader's, so
  they deny it votes in a small cluster. The premise of the figure is
  that the leader already won term 8 with other voters, so the test
  installs leadership directly and freezes the follower's election
  timer, keeping the scenario exact without weakening the vote rule.
- The first "checker budget" test passed for the wrong reason: a history
  of 40 incomplete concurrent puts is trivially linearizable by declaring
  none of them ever happened, so the search finished instantly instead of
  exhausting its budget. The test now buries a real violation under 20
  completed concurrent puts, which forces the exhaustive search the
  budget is meant to bound.
- Getting bit-for-bit determinism required treating randomness as
  plumbing: one root seed fans out to per-node, network, nemesis, and
  client `Random` instances, every event goes through a single
  (time, seq) heap, and partitions filter at delivery time. A test runs
  the same seed twice and compares full history fingerprints, which
  caught two accidental uses of ambient ordering during development.

## What I learned

- Sans-IO is the whole ballgame for testable distributed systems. The
  moment the consensus core stopped owning sockets and clocks, "run 5000
  chaos schedules in 90 seconds" became an ordinary function call, and
  every simulator bug found was by construction a production bug.
- Exactly-once is a state machine feature, not a networking feature. The
  dedup table has to live inside the replicated state (and inside
  snapshots), or leader failover silently downgrades you to
  at-least-once.
- Crash safety falls out of one discipline: never acknowledge before
  fsync, and make every on-disk structure either atomic-rename or
  scan-and-truncate recoverable. The torn-tail CRC scan is under 20
  lines.
- Safety nets need their own tests. A checker or invariant monitor that
  never fires is indistinguishable from one that cannot fire; the
  weakened-quorum canary test and the known-bad histories exist for
  exactly that reason.
- Raft's paper is honest about where the subtlety lives: section 5.4.2
  (no counting replicas from older terms) and the ReadIndex term-commit
  precondition are the two spots where a plausible shortcut is a real
  linearizability bug, and the chaos harness found the second one for me.

## What I'd do differently

- Replication is naive: the leader resends the whole remaining suffix to
  a follower on every heartbeat instead of tracking in-flight ranges.
  Correct (AppendEntries is idempotent) but wasteful; with pipelining
  and a per-follower window this would be far less chatty, and the
  batch-size cap would also bound message sizes.
- `InstallSnapshot` re-reads the snapshot from disk on every send and
  ships it whole. A cached handle plus chunked transfer with an offset,
  as in the dissertation, is the obvious upgrade.
- Add pre-vote. The term-inflation disruption after partitions is the
  one place the cluster does avoidable work, and the simulator would
  verify the fix in minutes.
- The simulator's MemoryStorage models a perfect disk. Injecting torn
  writes and fsync reordering into simulated storage would let the chaos
  runs cover the recovery paths that are currently only unit-tested.
- The CLI client opens a TCP connection per request. Fine for a demo,
  but a persistent connection with request multiplexing is what the
  frame protocol was designed for, and I did not finish that.
- Session expiry (a TTL agreed through the log, as in the dissertation)
  should exist before anyone calls the dedup table production-ready.

## License

MIT, see LICENSE.
