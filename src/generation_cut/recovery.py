"""Run opt-in recovery comparisons against fresh local Generation Interrupted servers."""

import argparse
import asyncio
import json
import math
import random
import time
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from .harness import ServerProc, pct, run_open
from .policies import Outcome, Policy, standard
from .recovery_budget import WholeCreditBudget
from .sdk_retries import RETRYABLE, SdkRetryStack


SENSITIVITY_CASES = {
    "short-timeout": ("1 worker / 400 ms timeout", 1, .4),
    "two-workers": ("2 workers / 1 s timeout", 2, 1.0),
    "two-workers-short": ("2 workers / 400 ms timeout", 2, .4),
}
BACKOFF_SWEEP_CASES = {f"backoff-{ms}ms": ms for ms in (25, 400, 800)}


class RetryStack:
    """Two attempts per layer; a shared cap, when enabled, is charged at the wire."""

    def __init__(self, depth, seed, shared=False):
        self.layers = [Policy("layer", backoff="expo_jitter", validate=True, rng=random.Random(seed + i))
                       for i in range(depth)]
        self.budget = WholeCreditBudget() if shared else None
        self.denied = 0

    async def call(self, client):
        start = time.monotonic()
        actual = 0
        if self.budget:
            self.budget.request()

        async def send(level):
            nonlocal actual
            if level < 0:
                if actual and self.budget and not self.budget.try_retry():
                    self.denied += 1
                    return "budget", None
                actual += 1
                return await self.layers[0].attempt(client)

            kind, retry_after = await send(level - 1)
            if kind in RETRYABLE:
                await asyncio.sleep(self.layers[level].delay(0, retry_after))
                return await send(level - 1)
            return kind, retry_after

        kind, _ = await send(len(self.layers) - 1)
        return Outcome(kind == "ok", actual, time.monotonic() - start, "" if kind == "ok" else kind)


@dataclass
class CircuitPolicy(Policy):
    """Open after three retryable failures; one physical probe after cooldown."""

    threshold: int = 3
    cooldown: float = 0.5
    failures: int = 0
    reopen_at: float = 0.0
    probing: bool = False
    epoch: int = 0
    rejected: int = 0

    async def attempt(self, client):
        probe = bool(self.reopen_at)
        if probe and (self.probing or time.monotonic() < self.reopen_at):
            self.rejected += 1
            return "circuit_open", None
        if probe:
            self.probing = True
        epoch = self.epoch
        try:
            result = await super().attempt(client)
        finally:
            if probe:
                self.probing = False

        failed = result[0] in RETRYABLE
        if probe or epoch == self.epoch:
            self.failures = self.failures + 1 if failed else 0
            if probe or self.failures >= self.threshold:
                self.epoch += 1
                self.reopen_at = time.monotonic() + self.cooldown if failed else 0.0
        return result


class WholeCreditPolicy(Policy):
    def __post_init__(self):
        if self.budget_ratio is not None:
            self._budget = WholeCreditBudget(self.budget_ratio)


@dataclass
class MeasuredBackoff(Policy):
    """Count active HTTP attempts; a full client rejects rather than adding a local queue."""

    max_in_flight: int | None = None
    in_flight: int = 0
    peak_in_flight: int = 0
    client_rejected: int = 0

    async def attempt(self, client):
        if self.max_in_flight is not None and self.in_flight >= self.max_in_flight:
            self.client_rejected += 1
            return "client_busy", None
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            return await super().attempt(client)
        finally:
            self.in_flight -= 1


def cases(seed, sdk=False):
    seed = random.Random(f"recovery:retry:{seed}").getrandbits(128)
    budget = standard(seed)[3]
    backoff = dict(max_attempts=4, backoff="expo_jitter", validate=True)
    rows = [
        ("backoff", "Backoff only", MeasuredBackoff("backoff", rng=random.Random(seed), **backoff), {}),
        ("client-cap", "Client cap: four active attempts", MeasuredBackoff(
            "client-cap", rng=random.Random(seed), max_in_flight=4, **backoff), {}),
        ("retry-budget", "Backoff + fractional-credit budget", budget, {}),
        ("budget-whole", "Backoff + whole-credit budget", WholeCreditPolicy(
            "whole-credit", max_attempts=4, backoff="expo_jitter", validate=True,
            rng=random.Random(seed), budget_ratio=.1), {}),
        ("bounded-queue", "Bounded queue", standard(seed)[2], {"queue_limit": 4}),
        ("early-shed", "Shed without queueing", standard(seed)[2], {"queue_limit": 0}),
        ("cancel", "Cancel abandoned work", standard(seed)[2], {"cancel_abandoned": True}),
        ("queue-cancel", "Bounded queue + cancellation", standard(seed)[2],
         {"queue_limit": 4, "cancel_abandoned": True}),
        ("breaker", "Client circuit breaker", CircuitPolicy(
            "breaker", max_attempts=4, backoff="expo_jitter", validate=True, rng=random.Random(seed)), {}),
        ("one-layer", "One retry layer", RetryStack(1, seed), {}),
        ("stacked", "Three retry layers", RetryStack(3, seed), {}),
        ("stack-budget", "Three layers + whole-credit cap", RetryStack(3, seed, shared=True), {}),
    ]
    rows += [
        (key, label, MeasuredBackoff(key, timeout=timeout, rng=random.Random(seed), **backoff), {"slots": slots})
        for key, (label, slots, timeout) in SENSITIVITY_CASES.items()
    ]
    rows += [
        (key, f"{ms} ms base", MeasuredBackoff(key, base=ms / 1000, rng=random.Random(seed), **backoff), {})
        for key, ms in BACKOFF_SWEEP_CASES.items()
    ]
    if sdk:
        rows += [
            ("sdk-only", "SDK retries only", SdkRetryStack(), {}),
            ("sdk-stacked", "SDK + app + workflow retries", SdkRetryStack(stacked=True), {}),
            ("sdk-budget", "SDK stack + wire budget", SdkRetryStack(stacked=True, shared=True), {}),
        ]
    return rows


STABILITY_WINDOW_S = 5


def timing_buckets(samples, duration, deadline):
    offered = [0] * duration
    successes = [[] for _ in range(duration)]
    for arrival, outcome, completed_at in samples:
        if arrival >= duration:
            continue
        bucket = int(arrival)
        offered[bucket] += 1
        if outcome.ok and completed_at <= min(duration, arrival + deadline):
            successes[bucket].append(completed_at)
    return offered, successes


def score_timing(samples, duration, outage_end, deadline):
    offered, successes = timing_buckets(samples, duration, deadline)
    rates = [len(times) / count if count else None for times, count in zip(successes, offered)]
    confirmed = []
    for times, count in zip(successes, offered):
        required = math.ceil(.9 * count)
        confirmed.append(sorted(times)[required - 1] if required and len(times) >= required else None)
    windows = (max(i + 3, *confirmed[i:i + 3]) for i in range(outage_end, duration - 2)
               if all(value is not None for value in confirmed[i:i + 3]))
    confirmation = min(windows, default=None)
    count = sum(offered)
    return {
        "logical": count,
        "on_time": sum(map(len, successes)) / count if count else None,
        "on_time_by_arrival_second": rates,
        "recovery_s": round(confirmation - outage_end, 3) if confirmation is not None else None,
    }


def recovery_windows(samples, outage_end, windows, deadline):
    return [
        {"post_outage_s": window,
         "recovery_s": score_timing(samples, outage_end + window, outage_end, deadline)["recovery_s"]}
        for window in windows
    ]


def recovery_stability(samples, duration, outage_end, deadline):
    offered, successes = timing_buckets(samples, duration, deadline)
    width = STABILITY_WINDOW_S
    on_time = [len(times) for times in successes]
    windows = {}
    # Wait for every deadline in a cohort before declaring a later drop.
    for start in range(outage_end, math.floor(duration - deadline) - width + 1):
        count = sum(offered[start:start + width])
        if count:
            windows[start] = 10 * sum(on_time[start:start + width]) >= 9 * count
    pairs = [start for start in windows if start + width in windows]
    start = next((i for i in pairs if windows[i] and windows[i + width]), None)
    follow_up = [i for i in windows if start is not None and i >= start + 2 * width]
    drop = next((i for i in follow_up if not windows[i]), None)

    def elapsed(end):
        return round(end + deadline - outage_end, 3)

    return {
        "evaluable": bool(pairs),
        "sustained_s": elapsed(start + 2 * width) if start is not None else None,
        "follow_up_evaluable": bool(follow_up),
        "relapse_s": elapsed(drop + width) if drop is not None else None,
    }


def summarize(samples, stats, duration, outage_end, deadline=1.0):
    timing = score_timing(samples, duration, outage_end, deadline)
    count = timing["logical"]
    return {
        **timing,
        "amplification": stats["received"] / count if count else None,
        "p99_s": pct([outcome.latency for _, outcome, _ in samples], .99) if count else None,
        "peak_queued": stats["peak_queued"],
        "shed": stats["shed"],
        "cancelled": stats["cancelled"],
    }


async def run_case(srv, policy, rate, duration, seed, config):
    if isinstance(policy, SdkRetryStack):
        async with policy.connect(srv.url):
            return await run_open(srv, policy, rate, duration, seed, strict_window=True, record_completion=True,
                                  config=config)
    return await run_open(srv, policy, rate, duration, seed, strict_window=True, record_completion=True,
                          config=config)


def budget_denials(policy, samples):
    if isinstance(policy, (RetryStack, SdkRetryStack)):
        return policy.denied
    return sum(outcome.reason.endswith("(budget)") for _, outcome, _ in samples)


def experiment(rate, duration, outage, seeds, deadline, *, sdk=False, selected=None, service_time=.04,
               windows=(), stability=False):
    rows = []
    selected = set(selected or ())
    sensitivity = bool(selected & SENSITIVITY_CASES.keys())
    sweep = bool(selected & BACKOFF_SWEEP_CASES.keys())
    opt_in = SENSITIVITY_CASES.keys() | BACKOFF_SWEEP_CASES.keys()
    for seed in range(seeds):
        arms = [row for row in cases(seed, sdk) if row[0] == "backoff" or
                (row[0] in selected if selected else row[0] not in opt_in)]
        if sweep or stability:
            # Rotate the first arm across seeds so the baseline does not always run first.
            offset = seed % len(arms)
            arms = arms[offset:] + arms[:offset]
        for case, label, policy, safeguards in arms:
            if sensitivity and case == "backoff":
                label = "1 worker / 1 s timeout"
            elif sweep and case == "backoff":
                label = f"{policy.base * 1000:g} ms base"
            with ServerProc() as srv:
                setup = dict(seed=seed, service_s=service_time, slots=1, outage_start=outage[0], outage_end=outage[1])
                setup.update(safeguards)
                samples = asyncio.run(run_case(srv, policy, rate, duration, seed, setup))
                stats = srv.stats()
                row = {"case": case, "label": label, "seed": seed,
                       **summarize(samples, stats, duration, outage[1], deadline),
                       "breaker_rejected": getattr(policy, "rejected", 0),
                       "budget_denied": budget_denials(policy, samples)}
                if isinstance(policy, SdkRetryStack):
                    row["sdk_layers"] = policy.layers
                    row["server_received"] = stats["received"]
                if isinstance(policy, MeasuredBackoff):
                    row["workers"] = setup["slots"]
                    row["attempt_timeout_s"] = policy.timeout
                    row["backoff_base_s"] = policy.base
                    row["client_attempts"] = {
                        "limit": policy.max_in_flight, "peak": policy.peak_in_flight,
                        "rejected": policy.client_rejected,
                    }
                if windows:
                    row["recovery_windows"] = recovery_windows(samples, outage[1], windows, deadline)
                if stability:
                    row["stability"] = recovery_stability(samples, duration, outage[1], deadline)
                rows.append(row)
                print(f"seed {seed}: {label}", flush=True)
    config = {"rate": rate, "duration": duration, "outage": outage, "seeds": seeds, "deadline": deadline,
              "service_s": service_time, "policy_retry_seed_scheme": "recovery:retry:<seed>"}
    if sdk:
        config["openai_sdk"] = version("openai")
    if windows:
        config["post_outage_windows"] = list(windows)
    if stability:
        config["stability_window_s"] = STABILITY_WINDOW_S
    return {"version": 2, "config": config, "runs": rows}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=float, default=16)
    parser.add_argument("--duration", type=int, default=12)
    parser.add_argument("--outage", type=int, nargs=2, default=[2, 5])
    parser.add_argument("--seeds", type=int, default=3)
    parser.add_argument("--deadline", type=float, default=1.0, help="Per-request on-time target in seconds.")
    parser.add_argument("--service-time", type=float, default=.04, help="Simulated service time per attempt in seconds.")
    parser.add_argument("--sdk", action="store_true", help="Include the real SDK retry cases (requires the dev extra).")
    parser.add_argument("--case", action="append", help="Run a named case plus backoff; repeat to compare several.")
    parser.add_argument("--post-outage-windows", type=int, nargs="+",
                        help="Score recovery at these post-outage cutoffs using the same extended run.")
    parser.add_argument("--sustained-recovery", action="store_true", help="Score sustained recovery and later drops.")
    parser.add_argument("--out", type=Path, help="Defaults to recovery.json or recovery-stability.json under results/raw.")
    args = parser.parse_args(argv)
    if not (0 < args.rate <= 100 and 1 <= args.seeds <= 20 and 0 < args.deadline <= 60
            and 0 < args.service_time <= 1
            and 0 <= args.outage[0] < args.outage[1] <= args.duration - 3 and args.duration <= 120):
        parser.error("Use 1-20 seeds, up to 100 req/s, 1 s service time, and 120 s duration; leave 3 seconds after the outage.")
    if args.sdk:
        try:
            version("openai")
        except PackageNotFoundError:
            parser.error("--sdk requires the dev extra.")
    available = {case for case, *_ in cases(0, args.sdk)}
    if args.case and (set(args.case) - available or set(args.case) == {"backoff"}):
        parser.error("Select at least one comparison case: " + ", ".join(sorted(available - {"backoff"})))
    windows = sorted(set(args.post_outage_windows or []))
    if windows and (windows[0] < 3 or windows[-1] > args.duration - args.outage[1]):
        parser.error("Post-outage windows must be at least 3 s and fit inside the run.")
    report = experiment(args.rate, args.duration, args.outage, args.seeds, args.deadline,
                        sdk=args.sdk, selected=args.case, service_time=args.service_time, windows=windows,
                        stability=args.sustained_recovery)
    out = args.out or Path("results/raw") / ("recovery-stability.json" if args.sustained_recovery else "recovery.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    temp = out.with_suffix(".tmp")
    temp.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temp.replace(out)
    print(f"Saved {out}")


if __name__ == "__main__":
    main()
