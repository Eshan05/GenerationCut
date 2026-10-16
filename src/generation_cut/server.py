"""Fault-injecting mock of an OpenAI-style chat endpoint.

Every request is assigned a fault by a seeded RNG keyed on (seed, arrival number), so a run's fault
sequence is reproducible. Capacity is modelled explicitly: `slots` workers each need `service_s`
seconds per request. By default the queue is unbounded and abandoned work continues. The recovery
experiment can cap waiting work and cancel work on an ASGI disconnect.
Configure with POST /_config, read counters with GET /_stats, restart the clock with POST /_reset.
"""
import asyncio, json, random, time
from math import fsum
from sys import float_info
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from .streaming import router as streaming_router
from .shared_backend import router as shared_backend_router
from .reconnect import router as reconnect_router

FAULT_ORDER = ["http429", "http500", "hang", "truncate", "garbage"]
DEFAULT = dict(rates={}, service_s=0.04, slots=1, hang_s=10.0, retry_after_s=0.2,
               outage_start=None, outage_end=None, outage_profile=None, seed=0, queue_limit=None, cancel_abandoned=False)

app = FastAPI()
app.include_router(streaming_router)
app.include_router(shared_backend_router)
app.include_router(reconnect_router)
S = {}


def choose_fault(seed, n, rates):
    """Deterministic fault for arrival number n. Rates are probabilities, applied in FAULT_ORDER."""
    r = random.Random(f"{seed}:{n}").random()
    acc = 0.0
    for f in FAULT_ORDER:
        acc += rates.get(f, 0.0)
        if r < acc:
            return f
    return "ok"


def outage_failure(state, arrival, attempt):
    start, end = state["outage_start"], state["outage_end"]
    if start is None or not start <= arrival < end:
        return False
    profile = state["outage_profile"]
    if profile is None:
        return True
    phase = min(2, int(3 * (arrival - start) / (end - start)))
    probability = profile[phase]
    failed = probability == 1
    if 0 < probability < 1:
        failed = random.Random(f"outage:{state['seed']}:{attempt}").random() < probability
    state["outage_attempts"][phase] += 1
    state["outage_failures"][phase] += int(failed)
    return failed


def valid_seconds(value):
    return type(value) in (float, int) and 0 <= value <= float_info.max


def reset(cfg=None):
    global S
    cfg = cfg or {}
    if unknown := cfg.keys() - DEFAULT.keys():
        raise HTTPException(422, f"Unknown mock settings: {', '.join(sorted(unknown))}")
    state = {**DEFAULT, **cfg}
    if type(state["slots"]) is not int or state["slots"] < 1:
        raise HTTPException(422, "slots must be a positive integer")
    for key in ("service_s", "hang_s", "retry_after_s"):
        if not valid_seconds(state[key]):
            raise HTTPException(422, f"{key} must be a finite nonnegative number")
    rates = state["rates"]
    if (not isinstance(rates, dict) or any(fault not in FAULT_ORDER for fault in rates)
            or any(type(p) not in (float, int) or not 0 <= p <= 1 for p in rates.values())
            or fsum(rates.values()) > 1):
        raise HTTPException(422, "rates must map known faults to probabilities summing to at most 1")
    limit = state["queue_limit"]
    if limit is not None and (type(limit) is not int or limit < 0):
        raise HTTPException(422, "queue_limit must be a nonnegative integer or null")
    if type(state["cancel_abandoned"]) is not bool:
        raise HTTPException(422, "cancel_abandoned must be a boolean")
    start, end = state["outage_start"], state["outage_end"]
    has_outage = start is not None or end is not None
    if has_outage and not (valid_seconds(start) and valid_seconds(end) and start < end):
        raise HTTPException(422, "outage_start and outage_end must form an increasing nonnegative window")
    profile = state["outage_profile"]
    if profile is not None:
        if (not has_outage or not isinstance(profile, list) or len(profile) != 3
                or not all(type(p) in (float, int) and 0 <= p <= 1 for p in profile)):
            raise HTTPException(422, "outage_profile needs three probabilities and a valid outage window")
        state.update(outage_attempts=[0, 0, 0], outage_failures=[0, 0, 0])
    state.update(n=0, t0=time.monotonic(), sem=asyncio.Semaphore(state["slots"]), arrivals={}, served=0,
                 faults={}, in_system=0, peak_queued=0, shed=0, cancelled=0)
    S = state


@app.on_event("startup")
async def _startup():
    reset()


@app.post("/_config")
async def config(cfg: dict):
    reset(cfg)
    return {"ok": True, "monotonic_start": S["t0"]}


@app.post("/_reset")
async def _reset():
    reset({k: S[k] for k in DEFAULT})
    return {"ok": True}


@app.get("/_stats")
async def stats():
    counters = {"arrivals_per_s": S["arrivals"], "received": S["n"], "served": S["served"], "faults": S["faults"],
                "in_system": S["in_system"], "peak_queued": S["peak_queued"], "shed": S["shed"], "cancelled": S["cancelled"]}
    if S["outage_profile"] is not None:
        counters["outage"] = {"attempts": S["outage_attempts"], "assigned_failures": S["outage_failures"]}
    return counters


def body():
    return {"choices": [{"message": {"role": "assistant", "content": json.dumps({"answer": 42})}}]}


async def perform_work(state, fault):
    if fault == "hang":
        await asyncio.sleep(state["hang_s"])
    async with state["sem"]:
        await asyncio.sleep(state["service_s"])
    state["served"] += 1


async def wait_for_disconnect(request):
    while (await request.receive())["type"] != "http.disconnect":
        pass


async def work_until_disconnect(request, state, fault):
    work = asyncio.create_task(perform_work(state, fault))
    monitor = asyncio.create_task(wait_for_disconnect(request))
    try:
        done, _ = await asyncio.wait((work, monitor), return_when=asyncio.FIRST_COMPLETED)
        if work in done:
            await work
            return True
        await monitor
        state["cancelled"] += 1
        return False
    finally:
        for task in (work, monitor):
            if not task.done():
                task.cancel()
        await asyncio.gather(work, monitor, return_exceptions=True)


@app.post("/v1/chat/completions")
async def chat(request: Request):
    state = S
    now = time.monotonic() - state["t0"]
    n = state["n"]; state["n"] += 1
    sec = int(now)
    state["arrivals"][sec] = state["arrivals"].get(sec, 0) + 1
    fault = choose_fault(state["seed"], n, state["rates"])
    state["faults"][fault] = state["faults"].get(fault, 0) + 1
    if fault == "http429":     # front-door rate limiter: cheap, no capacity used
        return JSONResponse({"error": "rate limited"}, 429, headers={"Retry-After": str(state["retry_after_s"])})
    limit = state["queue_limit"]
    if limit is not None and state["in_system"] >= state["slots"] + limit:
        state["shed"] += 1
        return JSONResponse({"error": "admission limit"}, 429, headers={"Retry-After": str(state["retry_after_s"])})
    state["in_system"] += 1
    state["peak_queued"] = max(state["peak_queued"], state["in_system"] - state["slots"])
    try:
        failed_in_outage = outage_failure(state, now, n)
        if state["cancel_abandoned"]:
            if not await work_until_disconnect(request, state, fault):
                return Response(status_code=499)
        else:
            await perform_work(state, fault)
    finally:
        state["in_system"] -= 1
    if failed_in_outage:
        return JSONResponse({"error": "outage"}, 500)   # decided at arrival time, after consuming capacity
    if fault == "http500":
        return JSONResponse({"error": "internal"}, 500)
    if fault == "garbage":
        return JSONResponse({"choices": [{"message": {"role": "assistant", "content": "Sure! {answer: forty-two"}}]})
    if fault == "truncate":
        full = json.dumps(body())

        async def gen():
            yield full[: len(full) // 2]
            raise RuntimeError("connection dropped mid-body")
        return StreamingResponse(gen(), media_type="application/json")
    return JSONResponse(body())
