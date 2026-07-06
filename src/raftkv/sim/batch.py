"""Batch runner: sweep many simulation seeds, in parallel processes.

Usage (also wired into the CLI as `raftkv sim`):

    python -m raftkv.sim.batch --seeds 200
    python -m raftkv.sim.batch --seeds 5000 --workers 8

Any failing seed is printed; re-run just that seed with --start SEED
--seeds 1 to replay the exact schedule under a debugger.
"""

from __future__ import annotations

import argparse
import multiprocessing
import sys
import time
from dataclasses import dataclass

from .harness import SimConfig, run_simulation


@dataclass
class SeedOutcome:
    seed: int
    ok: bool
    ops: int
    error: str = ""


def _run_one(seed: int) -> SeedOutcome:
    try:
        result = run_simulation(SimConfig(seed=seed))
        return SeedOutcome(seed, True, result.ops_completed)
    except Exception as exc:  # noqa: BLE001 - report any failure with its seed
        return SeedOutcome(seed, False, 0, f"{type(exc).__name__}: {exc}")


def run_batch(start: int, count: int, workers: int) -> int:
    t0 = time.monotonic()
    seeds = range(start, start + count)
    failures: list[SeedOutcome] = []
    total_ops = 0
    done = 0
    with multiprocessing.Pool(workers) as pool:
        for outcome in pool.imap_unordered(_run_one, seeds, chunksize=4):
            done += 1
            total_ops += outcome.ops
            if not outcome.ok:
                failures.append(outcome)
                print(f"FAIL seed={outcome.seed}: {outcome.error}", flush=True)
            if done % 250 == 0:
                print(f"... {done}/{count} seeds "
                      f"({time.monotonic() - t0:.0f}s)", flush=True)
    dt = time.monotonic() - t0
    print(f"{count} seeds in {dt:.1f}s "
          f"({count / dt:.1f} seeds/s), {total_ops} client ops total, "
          f"{len(failures)} failure(s)")
    for f in failures:
        print(f"  seed {f.seed}: {f.error}")
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Raft simulation seed sweep")
    ap.add_argument("--seeds", type=int, default=200, help="number of seeds")
    ap.add_argument("--start", type=int, default=0, help="first seed")
    ap.add_argument("--workers", type=int,
                    default=max(1, multiprocessing.cpu_count() - 1))
    args = ap.parse_args(argv)
    return run_batch(args.start, args.seeds, args.workers)


if __name__ == "__main__":
    sys.exit(main())
