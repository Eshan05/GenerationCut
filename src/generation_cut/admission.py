"""Compare two queue policies for one streaming worker, with no retries."""

import argparse
import asyncio
import json
import math
import os
import time
from collections import deque
from contextlib import aclosing
from dataclasses import dataclass, field
from pathlib import Path
from statistics import median

from .streaming import DEFAULT_MODEL, SourceError, ollama_chunks, scripted_chunks


QUEUE_LIMIT = 1


@dataclass(frozen=True)
class Estimate:
    first_s: float
    completion_s: float


@dataclass(frozen=True)
class Arrival:
    offset_s: float
    first_s: float
    completion_s: float
    kind: str


@dataclass
class Sample:
    status: str = "pending"
    first_s: float | None = None
    completion_s: float | None = None
    gaps_s: list[float] = field(default_factory=list)
    error: str | None = None


def fits(estimate, wait_s, first_left_s, completion_left_s):
    return wait_s + estimate.first_s <= first_left_s and wait_s + estimate.completion_s <= completion_left_s


async def observe(source, arrived, deadline):
    clock = time.perf_counter
    sample = Sample()
    previous = None

    async def read():
        nonlocal previous
        async with aclosing(source()) as stream:
            async for text in stream:
                if not text.strip():
                    continue
                now = clock()
                if now > deadline:
                    raise asyncio.TimeoutError
                if previous is None:
                    sample.first_s = now - arrived
                else:
                    sample.gaps_s.append(now - previous)
                previous = now

    try:
        await asyncio.wait_for(read(), timeout=max(0, deadline - clock()))
        finished = clock()
        if finished > deadline:
            sample.status = "completion_timeout"
        elif sample.first_s is None:
            sample.status = "empty_stream"
        else:
            sample.status = "complete"
            sample.completion_s = finished - arrived
    except asyncio.TimeoutError:
        sample.status = "completion_timeout"
    except SourceError as exc:
        sample.status = "source_error"
        sample.error = str(exc)
    return sample


async def run_case(source, arrivals, estimate, *, predict, queue_limit=QUEUE_LIMIT):
    clock = time.perf_counter
    started = clock()
    pending = deque()
    results = [Sample() for _ in arrivals]
    worker = None
    active_since = None

    def prune(now):
        for _ in range(len(pending)):
            index, job = pending.popleft()
            if now >= started + job.offset_s + min(job.first_s, job.completion_s):
                results[index].status = "expired_in_queue"
            else:
                pending.append((index, job))

    async def drain(index, job):
        nonlocal active_since, worker
        while True:
            arrived = started + job.offset_s
            results[index] = await observe(source, arrived, arrived + job.completion_s)
            now = clock()
            prune(now)
            if not pending:
                active_since = worker = None
                return
            index, job = pending.popleft()
            active_since = now

    try:
        for index, job in enumerate(arrivals):
            arrived = started + job.offset_s
            await asyncio.sleep(max(0, arrived - clock()))
            now = clock()
            prune(now)
            elapsed = now - arrived
            if elapsed >= min(job.first_s, job.completion_s):
                results[index].status = "expired_in_queue"
                continue
            if worker is not None and len(pending) >= queue_limit:
                results[index].status = "queue_full"
                continue
            remaining = max(0, estimate.completion_s - (now - active_since)) if active_since is not None else 0
            wait = remaining + len(pending) * estimate.completion_s
            if predict and not fits(estimate, wait, job.first_s - elapsed, job.completion_s - elapsed):
                results[index].status = "deadline_rejected"
            elif worker is None:
                active_since = now
                worker = asyncio.create_task(drain(index, job))
            else:
                pending.append((index, job))
        if worker is not None:
            await worker
    finally:
        if worker is not None and not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
    return results


def workload(estimate, bursts):
    service = max(.2, estimate.completion_s)
    jobs = []
    for burst in range(bursts):
        start = burst * 4 * service
        jobs.extend([
            Arrival(start, estimate.first_s + .5 * service, 2.5 * service, "Standard"),
            Arrival(start + .1 * service, estimate.first_s + .25 * service, 2.5 * service, "Tight first text"),
            Arrival(start + .2 * service, estimate.first_s + 1.2 * service, 2.5 * service, "Standard"),
        ])
    return jobs


def summarize(samples, arrivals):
    gaps = sorted(gap for sample in samples for gap in sample.gaps_s)
    return {
        "first_on_time": sum(s.first_s is not None and s.first_s <= a.first_s for s, a in zip(samples, arrivals)),
        "completion_on_time": sum(s.completion_s is not None and s.completion_s <= a.completion_s
                                  for s, a in zip(samples, arrivals)),
        "p95_gap_ms": round(gaps[math.ceil(.95 * len(gaps)) - 1] * 1000, 1) if gaps else None,
        "gap_samples": len(gaps),
        "requests": [{"status": s.status, "first_s": s.first_s, "completion_s": s.completion_s} for s in samples],
    }


async def experiment(source, bursts):
    factory = ollama_chunks if source == "ollama" else scripted_chunks
    clock = time.perf_counter
    calibration = []
    print(f"Warming and calibrating {source} (four streams).", flush=True)
    for _ in range(4):
        start = clock()
        sample = await observe(factory, start, start + 90)
        if sample.status != "complete":
            raise RuntimeError(f"Warm-up/calibration did not complete: {sample.error or sample.status}")
        calibration.append(sample)
    # Ignore the first request, which can include cold model loading.
    estimate = Estimate(median(s.first_s for s in calibration[1:]), median(s.completion_s for s in calibration[1:]))
    arrivals = workload(estimate, bursts)
    runs = {}
    for name, predict in [("bounded", False), ("deadline", True)]:
        runs[name] = summarize(await run_case(factory, arrivals, estimate, predict=predict), arrivals)
        print(f"{name}: {runs[name]['first_on_time']}/{len(arrivals)} first text on time, "
              f"{runs[name]['completion_on_time']}/{len(arrivals)} finished on time", flush=True)
    return {
        "source": source,
        "model": os.environ.get("GENERATION_CUT_OLLAMA_MODEL", DEFAULT_MODEL) if source == "ollama" else None,
        "queue_limit": QUEUE_LIMIT,
        "estimate": vars(estimate),
        "arrivals": [vars(job) for job in arrivals],
        "runs": runs,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["scripted", "ollama"], default="scripted")
    parser.add_argument("--bursts", type=int, default=3, choices=range(1, 9), metavar="1-8")
    parser.add_argument("--out", type=Path, default=Path("results/raw/admission.json"))
    args = parser.parse_args(argv)
    try:
        report = asyncio.run(experiment(args.source, args.bursts))
    except RuntimeError as exc:
        parser.exit(1, f"Admission experiment: {exc}\n")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    temp = args.out.with_suffix(".tmp")
    temp.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temp.replace(args.out)
    print(f"Saved {args.out}")


if __name__ == "__main__":
    main()
