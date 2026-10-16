"""Compare dispatch, cancellation, and saturation on one backend."""

import asyncio
from contextlib import aclosing
from time import perf_counter
from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .streaming import NO_CACHE, SourceError, frames, ollama_chunks, require_local

router = APIRouter()
live_comparison = asyncio.Lock()
ARRIVAL_CHUNK = 3
SCRIPT_STEP_S = 0.22
STOP_DELAY_S = 0.25
LONG_PROMPT = "Walk through how an API cache handles hits, misses, expiration, and traffic spikes."
SHORT_PROMPT = "What is a cache hit? Answer in one short sentence."
PEER_PROMPT = "Explain how a database handles indexing, reads, writes, and transaction rollbacks."
PROMPTS = {"long": LONG_PROMPT, "peer": PEER_PROMPT, "short": SHORT_PROMPT}
SCRIPTS = {
    "long": (
        "First, the API looks for the requested data ", "in its cache. ",
        "On a hit, it returns the saved result ", "without contacting the database. ",
        "On a miss, it fetches the data ", "and saves a copy for later. ",
        "Each copy has an expiration time ", "so old data does not stick around forever. ",
        "When a popular entry expires, ", "many clients may ask for it together. ",
        "The API can coalesce those requests ", "into a single fetch ",
        "while the other clients wait. ", "That keeps a burst of traffic ",
        "from becoming a burst of database work.",
    ),
    "peer": (
        "An index lets the database find rows ", "without a full scan. ",
        "Reads can use that index ", "to locate the requested records. ",
        "Writes update the table ", "and any affected indexes. ",
        "A transaction groups changes ", "into one unit of work. ",
        "Rolling back discards those changes ", "instead of committing them.",
    ),
    "short": ("A cache hit ", "means the requested data ", "was already ", "in the cache."),
}
TARGETS = {
    "scripted": {"long": {"completion": 5.5}, "short": {"first": 1.1, "completion": 2.2}},
    "ollama": {"long": {"completion": 20}, "short": {"first": 3, "completion": 8}},
}
POLICY_PAIRS = {
    "dispatch": ("queued", "immediate"),
    "cancellation": ("keep", "cancel"),
    "saturation": ("keep-both", "stop-a"),
}


def scripted_source(slots=None):
    decoder = asyncio.Semaphore(slots or 1)

    async def serialized(lane):
        async with decoder:
            for text in SCRIPTS[lane]:
                await asyncio.sleep(SCRIPT_STEP_S)
                yield text

    async def chunks(lane):
        # Dispatch trials share decoder turns rather than holding a slot for a whole request.
        for text in SCRIPTS[lane]:
            async with decoder:
                await asyncio.sleep(SCRIPT_STEP_S)
            yield text

    return serialized if slots is not None else chunks


def local_source(lane):
    return ollama_chunks(
        prompt=PROMPTS[lane],
        instructions="Explain in about 100 words across four short plain-text paragraphs."
        if lane != "short" else "Keep the answer to one short sentence.",
        max_tokens=48 if lane == "short" else 256,
        temperature=0,
        accept_length_limit=False,
    )


async def trial_events(policy, timeout, play):
    events = asyncio.Queue()
    started = perf_counter()

    def emit(kind, lane=None, **fields):
        event = {"type": kind, "policy": policy, "ms": (perf_counter() - started) * 1000, **fields}
        if lane is not None:
            event["lane"] = lane
        events.put_nowait(event)

    async def run():
        try:
            await asyncio.wait_for(play(emit), timeout)
        except asyncio.TimeoutError:
            emit("timeout")
        finally:
            events.put_nowait(None)

    task = asyncio.create_task(run())
    try:
        while (event := await events.get()) is not None:
            yield event
        await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def trial(policy, chunks, timeout):
    short_arrived = asyncio.get_running_loop().create_future()
    long_finished = asyncio.Event()

    async def generate(emit, lane):
        emit("dispatch", lane)
        visible = 0
        cancelled = False
        try:
            async with aclosing(chunks(lane)) as stream:
                async for text in stream:
                    emit("delta", lane, text=text)
                    if text.strip():
                        visible += 1
                        if lane == "long" and visible == ARRIVAL_CHUNK:
                            emit("arrival", "short")
                            short_arrived.set_result(True)
                            if policy == "cancel":
                                cancelled = True
                                break
        except SourceError as exc:
            emit("error", lane, message=str(exc))
        else:
            emit("cancelled" if cancelled else "done", lane)

    async def long_user(emit):
        try:
            await generate(emit, "long")
        finally:
            long_finished.set()
            if not short_arrived.done():
                short_arrived.set_result(False)

    async def short_user(emit):
        if not await short_arrived:
            emit("skipped", "short", message="Long stream ended before the short request could run.")
            return
        if policy == "queued":
            await long_finished.wait()
        await generate(emit, "short")

    async def play(emit):
        emit("arrival", "long")
        tasks = [asyncio.create_task(long_user(emit)), asyncio.create_task(short_user(emit))]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async with aclosing(trial_events(policy, timeout, play)) as timeline:
        async for event in timeline:
            yield event


async def saturation_trial(policy, chunks, timeout):
    async def play(emit):
        visible = dict.fromkeys(PROMPTS, 0)
        ready = {lane: asyncio.Event() for lane in ("long", "peer")}
        tasks = {}
        stopped_a = False

        async def generate(lane):
            emit("dispatch", lane)
            try:
                async with aclosing(chunks(lane)) as stream:
                    async for text in stream:
                        emit("delta", lane, text=text)
                        if text.strip():
                            visible[lane] += 1
                            if lane in ready and visible[lane] == ARRIVAL_CHUNK:
                                ready[lane].set()
            except SourceError as exc:
                emit("error", lane, message=str(exc))
            except asyncio.CancelledError:
                if lane == "long" and stopped_a:
                    emit("cancelled", lane)
                    return
                raise
            else:
                emit("done", lane)
            finally:
                if lane in ready:
                    ready[lane].set()

        def start(lane):
            emit("arrival", lane)
            tasks[lane] = asyncio.create_task(generate(lane))

        try:
            start("long")
            await ready["long"].wait()
            if tasks["long"].done():
                emit("trial-invalid", message="A stopped before both streams were active.")
                return
            start("peer")
            await ready["peer"].wait()
            if tasks["long"].done() or tasks["peer"].done():
                emit("trial-invalid", message="A and B did not stay active together.")
                return
            start("short")
            # Give C the same head start before the simulated user stop in both trials.
            await asyncio.sleep(STOP_DELAY_S)
            if tasks["long"].done() or tasks["peer"].done():
                emit("trial-invalid", message="A or B finished before the stop point.")
                return
            if visible["short"] or tasks["short"].done():
                emit("trial-invalid", message="C did not wait while both streams were active.")
                return
            if policy == "stop-a":
                stopped_a = True
                tasks["long"].cancel()
            await asyncio.gather(*tasks.values())
        finally:
            for task in tasks.values():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)

    async with aclosing(trial_events(policy, timeout, play)) as timeline:
        async for event in timeline:
            yield event


async def warm_up():
    async with aclosing(ollama_chunks(prompt="Say ready.", max_tokens=4, temperature=0)) as stream:
        async for _ in stream:
            pass


async def compare(source, comparison="dispatch", order="baseline"):
    saturation = comparison == "saturation"
    yield frames({
        "type": "setup", "targets": {} if saturation else TARGETS[source],
        "prompts": PROMPTS if saturation else {"long": LONG_PROMPT, "short": SHORT_PROMPT},
    })
    if source == "ollama":
        yield frames({"type": "warming"})
        try:
            await asyncio.wait_for(warm_up(), 75)
        except asyncio.TimeoutError:
            yield frames({"type": "failed", "message": "Ollama warm-up timed out."})
            return
        except SourceError as exc:
            yield frames({"type": "failed", "message": str(exc)})
            return

    policies = POLICY_PAIRS[comparison]
    for policy in reversed(policies) if order == "reverse" else policies:
        slots = {"cancellation": 1, "saturation": 2}.get(comparison)
        chunks = local_source if source == "ollama" else scripted_source(slots)
        run_trial = saturation_trial if saturation else trial
        async with aclosing(run_trial(policy, chunks, timeout=90 if source == "ollama" else 12)) as events:
            async for event in events:
                yield frames(event)
        yield frames({"type": "trial-end", "policy": policy})
    yield frames({"type": "comparison-end"})


async def live_run(comparison, order):
    if live_comparison.locked():
        yield frames({"type": "failed", "message": "A live comparison is already running."})
        return
    async with live_comparison, aclosing(compare("ollama", comparison, order)) as events:
        async for event in events:
            yield event


@router.post("/v1/chat/completions/shared-backend")
async def shared_backend(
    request: Request,
    source: Literal["scripted", "ollama"] = "scripted",
    comparison: Literal["dispatch", "cancellation", "saturation"] = "dispatch",
    order: Literal["baseline", "reverse"] = "baseline",
):
    if source == "ollama":
        require_local(request)
    return StreamingResponse(
        live_run(comparison, order) if source == "ollama" else compare(source, comparison, order),
        media_type="application/x-ndjson", headers=NO_CACHE,
    )
