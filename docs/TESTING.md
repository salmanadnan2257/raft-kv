# Testing

## Why deterministic simulation

Distributed systems bugs live in interleavings: a partition forming while
a snapshot is in flight, a leader crashing between commit and reply, two
elections racing under clock skew. Integration tests on real networks hit
these interleavings rarely and, worse, unreproducibly.

The approach here follows the FoundationDB lineage: make the entire
cluster a deterministic function of one integer. Every source of
nondeterminism (time, timers, message delay, loss, duplication, partition
timing, crash timing, client behavior, election jitter) is driven by a
single seeded RNG through a virtual clock (`sim/clock.py`). A run is then
a pure function of its seed:

- thousands of schedules can be searched cheaply (no real sleeping: 12
  virtual seconds of cluster time run in ~0.1 real seconds),
- a failing seed replays the exact schedule, every time, under a
  debugger,
- CI failure reports are one integer, not a flaky-test shrug.

The critical design constraint is that the simulated system must be the
real system. `RaftNode` is sans-IO and runs unmodified under both the
asyncio TCP transport and the simulator; only the Env/Storage
implementations differ (see ARCHITECTURE.md).

## What a simulation run does

`sim/harness.py::run_simulation(SimConfig(seed=N))`:

- 3 nodes (configurable), 3 concurrent clients issuing randomized
  get/put/cas with retries and timeouts, history recorded with virtual
  timestamps.
- Nemesis actions at random times during the first 70% of the run:
  random bisecting partitions, crash + delayed restart (volatile state
  destroyed, "disk" kept), election-timer clock skew (0.5x-1.8x), heals.
  Message loss (5%), duplication (5%), and randomized delivery delay run
  the whole time.
- The final stretch stabilizes the cluster so in-flight operations drain.

Invariants checked during the run, each failure naming the seed:

- Election safety: at most one leader per term, tracked over the entire
  run, not per sample.
- Leader completeness: at the moment a node wins an election, its log
  must contain every entry any node ever applied (committed entries are
  never lost by leader changes).
- State machine safety: every applied (index, entry) pair is recorded
  globally; two nodes applying different entries at one index is an
  immediate failure.
- Log matching: periodic pairwise check that entries with equal index and
  term are identical.

After the run, the recorded client history is checked for
linearizability. `test_sim_invariants.py::test_broken_raft_is_caught`
deliberately weakens the quorum rule and asserts the harness detects the
resulting split brain, so the safety net is itself tested.

## The linearizability checker

`checker/linearizability.py` implements the Wing & Gong search with
Lowe's memoization, written from scratch:

- Keys are independent registers, so each key's sub-history is checked
  separately (the largest practical win).
- At each step any "minimal" operation (one whose invocation precedes the
  completion of every other pending candidate) may be linearized next if
  the model register reproduces the observed result.
- Incomplete operations (client never got a response) branch both ways:
  applied at some point, or never happened.
- Visited (linearized-set, register-value) states are memoized.

Limits, honestly: the problem is NP-complete in general; the checker uses
a bitmask over at most 63 ops per key and a configurable state budget,
returning UNKNOWN rather than hanging if a history is too wide. Chaos-run
histories here (roughly 30-60 ops per key, bounded concurrency) verify in
milliseconds. `test_linearizability.py` includes known-bad synthetic
histories (stale read, lost update, phantom value) proving the checker
rejects violations, which guards against the classic trap of a checker
that silently accepts everything.

## Test suites and commands

Everything runs with the venv's pytest from the project root.

```
pytest -m "not slow"          # full suite minus the long sweep (seconds)
pytest                        # includes the 200-seed slow sweep (~15s)
raftkv sim --seeds 5000       # the long randomized sweep (documented run
                              # for this repo reported in the README)
raftkv sim --start N --seeds 1 --workers 1   # replay one failing seed
```

Suites:

- `test_election.py`, `test_replication.py`: unit-level scenarios on the
  simulator with faults disabled or targeted, including all six follower
  states from the paper's Figure 7 and the section 5.4.2 commit rule.
- `test_persistence.py`: FileStorage torn-write recovery (crash mid-append
  leaves a partial record; CRC scan truncates it), atomic hardstate,
  snapshot/log-rewrite crash windows.
- `test_snapshot.py`: compaction thresholds, InstallSnapshot catch-up of a
  partitioned follower, restart from snapshot.
- `test_membership.py`: add/remove server, leader self-removal, the
  one-change-at-a-time rule.
- `test_kv_sessions.py`: dedup semantics, exactly-once across failover.
- `test_readindex.py`: stale leaders in minority partitions cannot serve
  reads.
- `test_sim_invariants.py`: determinism (identical seed, identical run),
  quick sweep, harness self-test, 200-seed slow sweep.
- `test_tcp_integration.py`: a real 3-node asyncio TCP cluster on
  localhost ports: put/get/cas, leader kill, failover, restart catch-up,
  full-cluster restart from disk.
