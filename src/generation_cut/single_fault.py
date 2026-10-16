"""Repeat the immediate vs. backoff single-fault comparison on loopback."""

import argparse
import asyncio
import json
from pathlib import Path

from .experiment import RATES_30
from .harness import ServerProc, pct, run_closed
from .policies import standard


def measure(outcomes):
    return {
        "success": sum(outcome.ok for outcome in outcomes) / len(outcomes),
        "attempts": sum(outcome.attempts for outcome in outcomes) / len(outcomes),
        "p99_s": pct([outcome.latency for outcome in outcomes], .99),
    }


def experiment(seeds, requests):
    rate, concurrency = .30, 8
    runs = []
    for seed in range(seeds):
        for fault in RATES_30:
            _, immediate, backoff, _ = standard(f"single-fault:retry:{seed}")
            pair = {"fault": fault, "seed": seed}
            # Alternate which policy runs first without overlapping measurements.
            arms = [("immediate", immediate), ("backoff", backoff)]
            for name, policy in arms if seed % 2 == 0 else reversed(arms):
                with ServerProc() as srv:
                    srv.config(rates={fault: rate}, seed=seed, hang_s=3.0, retry_after_s=.05)
                    pair[name] = measure(asyncio.run(run_closed(srv, policy, requests, concurrency)))
            runs.append(pair)
            print(f"seed {seed}: {fault}", flush=True)
    return {"version": 1, "config": {"seeds": seeds, "requests": requests, "fault_rate": rate,
                                     "concurrency": concurrency}, "runs": runs}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", type=int, default=5)
    parser.add_argument("--requests", type=int, default=200, help="Logical requests per fault, policy, and seed.")
    parser.add_argument("--out", type=Path, default=Path("results/raw/single-fault.json"))
    args = parser.parse_args(argv)
    if not (2 <= args.seeds <= 20 and 1 <= args.requests <= 1000):
        parser.error("Use 2-20 seeds and 1-1000 requests per cell.")
    report = experiment(args.seeds, args.requests)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    temp = args.out.with_suffix(".tmp")
    temp.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temp.replace(args.out)
    print(f"Saved {args.out}")


if __name__ == "__main__":
    main()
