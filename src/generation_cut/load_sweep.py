"""Compare backoff and a fixed client cap across local offered loads and worker counts."""

import argparse
import asyncio
import json
from pathlib import Path

from .harness import ServerProc
from .recovery import cases, run_case, summarize


POLICIES = ("backoff", "client-cap")
OUTAGE_SHAPES = {"hard": (1., 1., 1.), "early-heavy": (.9, .6, .3), "late-heavy": (.3, .6, .9)}


def workloads(workers, utilizations, service_s, fixed_rate=None, outage_shapes=()):
    cells = []
    for slots in sorted(set(workers)):
        rates = {round(load * slots / service_s, 6): round(load, 6) for load in sorted(set(utilizations))}
        if fixed_rate is not None:
            rates.setdefault(round(fixed_rate, 6), round(fixed_rate * service_s / slots, 6))
        for rate, load in sorted(rates.items()):
            for shape in dict.fromkeys(outage_shapes or (None,)):
                cell = {"workers": slots, "rate": rate, "utilization": load, "runs": []}
                if shape is not None:
                    cell["outage_shape"] = shape
                cells.append(cell)
    return cells


def experiment(cells, *, seeds, seed_start, duration, outage, deadline, service_s):
    for index, seed in enumerate(range(seed_start, seed_start + seeds)):
        offset = index % len(cells)
        for cell_index, cell in enumerate(cells[offset:] + cells[:offset]):
            workers, rate = cell["workers"], cell["rate"]
            setup = dict(slots=workers, service_s=service_s, seed=seed, outage_start=outage[0], outage_end=outage[1])
            shape = cell.get("outage_shape")
            if shape is not None:
                setup["outage_profile"] = list(OUTAGE_SHAPES[shape])
            policies = {name: policy for name, _, policy, _ in cases(seed) if name in POLICIES}
            order = POLICIES if (index + cell_index) % 2 == 0 else POLICIES[::-1]
            for name in order:
                with ServerProc() as srv:
                    samples = asyncio.run(run_case(srv, policies[name], rate, duration, seed, setup))
                    stats = srv.stats()
                    row = {"case": name, "seed": seed, **summarize(samples, stats, duration, outage[1], deadline)}
                    if shape is not None:
                        row["outage"] = stats["outage"]
                    cell["runs"].append(row)
            label = f", {shape}" if shape else ""
            print(f"Seed {seed}: {workers} workers, {rate:g} req/s, {cell['utilization']:.0%} nominal load{label}", flush=True)
    config = {"seeds": seeds, "seed_start": seed_start, "duration": duration,
              "outage": outage, "deadline": deadline, "service_s": service_s}
    shapes = {cell["outage_shape"]: OUTAGE_SHAPES[cell["outage_shape"]] for cell in cells if "outage_shape" in cell}
    if shapes:
        config["outage_shapes"] = shapes
    return {"version": 1, "config": config, "cells": cells}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--utilizations", nargs="+", type=float, default=[.32, .64, .88],
                        help="Nominal load fractions, excluding retries.")
    parser.add_argument("--workers", nargs="+", type=int, default=[1, 2])
    parser.add_argument("--fixed-rate", type=float, help="Also test this offered rate for every worker count.")
    parser.add_argument("--service-time", type=float, default=.04)
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument("--outage", type=int, nargs=2, default=[1, 3])
    parser.add_argument("--outage-shapes", nargs="+", choices=OUTAGE_SHAPES,
                        help="Try these failure profiles across equal thirds of the outage.")
    parser.add_argument("--deadline", type=float, default=1.0)
    parser.add_argument("--seeds", type=int, default=3)
    parser.add_argument("--seed-start", type=int, default=0, help="Use a different block for a replication run.")
    parser.add_argument("--out", type=Path, help="Defaults to load-sweep.json or load-shapes.json under results/raw.")
    args = parser.parse_args(argv)
    if not (2 <= len(set(args.workers)) <= 4 and all(1 <= n <= 8 for n in args.workers)
            and 1 <= len(set(args.utilizations)) <= 6 and all(0 < n <= 2 for n in args.utilizations)
            and (args.fixed_rate is None or 0 < args.fixed_rate <= 100)
            and 2 <= args.seeds <= 20 and args.seed_start >= 0
            and 0 < args.service_time <= 1 and 0 < args.deadline <= 60
            and 0 <= args.outage[0] < args.outage[1] <= args.duration - 3 and args.duration <= 120):
        parser.error("Use 2-4 distinct worker counts (1-8), 1-6 load levels (0-2), 2-20 seeds, and 3 s after the outage.")
    cells = workloads(args.workers, args.utilizations, args.service_time, args.fixed_rate, args.outage_shapes)
    if any(not 0 < cell["rate"] <= 100 for cell in cells):
        parser.error("The resulting rates must be greater than 0 and at most 100 requests/s.")
    trials = len(cells) * args.seeds * len(POLICIES)
    print(f"{trials} sequential trials ({args.duration} s of arrivals each, plus drain)", flush=True)
    report = experiment(cells, seeds=args.seeds, seed_start=args.seed_start, duration=args.duration,
                        outage=args.outage, deadline=args.deadline, service_s=args.service_time)
    out = args.out or Path("results/raw") / ("load-shapes.json" if args.outage_shapes else "load-sweep.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    temp = out.with_suffix(".tmp")
    temp.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temp.replace(out)
    print(f"Saved {out}")


if __name__ == "__main__":
    main()
