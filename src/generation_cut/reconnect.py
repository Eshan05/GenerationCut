"""One local inference independent of its browser delivery connection."""

import asyncio
import logging
from contextlib import aclosing
from typing import Literal
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from .streaming import NO_CACHE, PROMPT, SourceError, frames, ollama_chunks, require_local

router = APIRouter(prefix="/v1/stream-runs")
GRACE_S = 5
DEADLINE_S = 90
CUT_AFTER = 3
MAX_RUNS = 8
MAX_CHUNKS = 512
runs = {}


class DeliveryRun:
    def __init__(self, *, cut_upstream=False, prefix=None):
        self.events = []
        self.changed = asyncio.Event()
        self.listeners = 0
        self.terminal = None
        self.stop_reason = None
        self.task = asyncio.create_task(self._produce(cut_upstream, prefix))
        self.task.add_done_callback(self._settled)
        self.expiry = self._grace_timer()

    def _grace_timer(self):
        return asyncio.get_running_loop().call_later(GRACE_S, self.stop, "expired")

    def _emit(self, kind, **fields):
        self.events.append({"seq": len(self.events) + 1, "type": kind, **fields})
        self.changed.set()
        self.changed = asyncio.Event()

    def stop(self, reason="stopped"):
        if not self.task.done() and self.stop_reason is None:
            self.stop_reason = reason
            self.task.cancel()

    async def _produce(self, cut_upstream, prefix):
        async def collect():
            options = {"accept_length_limit": False}
            if prefix is not None:
                options["prompt"] = (
                    f"Original question: {PROMPT}\n\nPartial answer:\n{prefix}\n\n"
                    "Continue the partial answer with only the missing text. Do not repeat the prefix."
                )
                options["instructions"] = "Complete the answer concisely without Markdown."
            visible = 0
            async with aclosing(ollama_chunks(on_request=lambda: self._emit("started"), **options)) as stream:
                async for chunk in stream:
                    if len(self.events) > MAX_CHUNKS:
                        raise SourceError("This run exceeded the output buffer.")
                    self._emit("delta", text=chunk)
                    if chunk.strip():
                        visible += 1
                        if cut_upstream and visible == CUT_AFTER:
                            return "upstream-cut", "Generation Interrupted closed the Ollama stream."
            if not visible:
                raise SourceError("Ollama returned no visible text.")
            return "done", None

        try:
            return await asyncio.wait_for(collect(), DEADLINE_S)
        except asyncio.TimeoutError:
            return "deadline", "The run reached its deadline."
        except SourceError as exc:
            return "error", str(exc)

    def _settled(self, task):
        self.expiry.cancel()
        if task.cancelled():
            self.terminal, message = self.stop_reason or "stopped", None
        else:
            try:
                self.terminal, message = task.result()
            except Exception:
                logging.getLogger(__name__).exception("Unexpected delivery experiment error")
                self.terminal, message = "error", "The run ended unexpectedly."
        self._emit(self.terminal, **({"message": message} if message else {}))

    async def subscribe(self, after):
        replay_end = len(self.events) if after else 0
        self.listeners += 1
        self.expiry.cancel()
        try:
            while True:
                while after < len(self.events):
                    event = self.events[after]
                    after += 1
                    yield frames({**event, "replayed": True} if after <= replay_end else event)
                if self.terminal is not None:
                    return
                await self.changed.wait()
        finally:
            self.listeners -= 1
            if not self.listeners and self.terminal is None and self.stop_reason is None:
                self.expiry = self._grace_timer()


def get_run(request, run_id):
    require_local(request)
    if run_id not in runs:
        raise HTTPException(404, "Run is no longer available.")
    return runs[run_id]


def new_run(**options):
    if any(run.terminal is None for run in runs.values()):
        raise HTTPException(409, "An inference is already running in this experiment.")
    if len(runs) >= MAX_RUNS:
        del runs[next(iter(runs))]
    run_id = uuid4().hex
    runs[run_id] = DeliveryRun(**options)
    return {"id": run_id, "cut_after": CUT_AFTER}


@router.post("")
async def start_run(request: Request, fault: Literal["browser", "upstream"]):
    require_local(request)
    return new_run(cut_upstream=fault == "upstream")


@router.get("/{run_id}/events")
async def run_events(request: Request, run_id: str, after: int = 0):
    run = get_run(request, run_id)
    if not 0 <= after <= len(run.events):
        raise HTTPException(422, "The cursor is outside this run's event history.")
    return StreamingResponse(run.subscribe(after), media_type="application/x-ndjson", headers=NO_CACHE)


@router.delete("/{run_id}")
async def stop_run(request: Request, run_id: str):
    get_run(request, run_id).stop()
    return {"ok": True}


@router.post("/{run_id}/retry")
async def retry_run(request: Request, run_id: str, action: Literal["continue", "regenerate"]):
    run = get_run(request, run_id)
    if run.terminal != "upstream-cut":
        raise HTTPException(409, "A new attempt is only available after the Ollama cut.")
    prefix = "".join(event["text"] for event in run.events if event["type"] == "delta")
    return new_run(prefix=prefix if action == "continue" else None)
