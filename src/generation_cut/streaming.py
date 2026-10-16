"""Scripted and local-model streams, separate from the seeded load experiment."""

import asyncio
import json
import math
import os
from collections.abc import AsyncGenerator, Callable
from contextlib import aclosing
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse


router = APIRouter()
DEMO = Path(__file__).resolve().parents[2] / "demo"
OLLAMA_CHAT = "http://127.0.0.1:11434/api/chat"
DEFAULT_MODEL = "openbmb/minicpm5:q8_0"
STREAM_OUTPUT_LIMIT = 96
OLLAMA_EVENT_FIELDS = {
    "done", "message", "model", "created_at", "done_reason", "total_duration", "load_duration",
    "prompt_eval_count", "prompt_eval_duration", "eval_count", "eval_duration",
}
PROMPT = "How does a cache reduce load on a busy API?"
CHUNKS = (
    "A cache keeps copies ",
    "of recent API responses. ",
    "When the same request arrives again, ",
    "the cache returns a copy ",
    "without calling the backend. ",
    "That reduces repeated work ",
    "and keeps response times steady ",
    "during traffic spikes.",
)
SCRIPT_WORK = (4, 5, 10, 6, 5, 5, 6, 4)
CUT_AT = {"before": 0, "after": 3, "late": 6}
INTERVAL_S = 0.43
STALL_STEP_S = 0.22
STALL_GAP_S = {"temporary": 2.4, "persistent": 16.0}
STALL_AFTER = 3
ASSETS = {
    "style.css": "web/style.css",
    "app.js": "dist/app.js",
    "experiment.js": "dist/experiment.js",
    "recovery.js": "dist/recovery.js",
    "single-fault.js": "dist/single-fault.js",
    "load-sweep.js": "dist/load-sweep.js",
    "admission.js": "dist/admission.js",
    "reconnect.js": "dist/reconnect.js",
    "stall.js": "dist/stall.js",
    "stall-policy.js": "dist/stall-policy.js",
    "shared-backend.js": "dist/shared-backend.js",
    **{
        f"icons/{name}.svg": f"node_modules/lucide-static/icons/{name}.svg"
        for name in (
            "activity", "audio-lines", "chart-no-axes-combined", "list-filter",
            "network", "play", "rotate-ccw", "square",
        )
    },
}
NO_CACHE = {"Cache-Control": "no-store"}


def demo_file(name):
    file = DEMO / name
    if not file.is_file():
        raise HTTPException(404, "Demo file not found. See the README for build steps.")
    return FileResponse(file, headers=NO_CACHE)


@router.get("/demo/", include_in_schema=False)
async def demo():
    return demo_file("web/index.html")


@router.get("/demo/recovery/", include_in_schema=False)
async def recovery_demo():
    return demo_file("web/recovery.html")


@router.get("/demo/recovery/single-fault/", include_in_schema=False)
async def single_fault_demo():
    return demo_file("web/single-fault.html")


@router.get("/demo/single-fault-results.json", include_in_schema=False)
async def single_fault_results():
    return local_report("single-fault.json")


@router.get("/demo/recovery/load/", include_in_schema=False)
async def load_sweep_demo():
    return demo_file("web/load-sweep.html")


@router.get("/demo/load-sweep-results.json", include_in_schema=False)
async def load_sweep_results(study: Literal["load", "shapes"] = "load"):
    return local_report("load-shapes.json" if study == "shapes" else "load-sweep.json")


@router.get("/demo/recovery-results.json", include_in_schema=False)
async def recovery_results(
    workload: Literal["standard", "timeout", "client-cap", "sensitivity", "observation", "backoff", "stability"] = "standard",
):
    files = {
        "standard": "recovery.json", "timeout": "recovery-timeout.json",
        "client-cap": "recovery-client-cap.json", "sensitivity": "recovery-sensitivity.json",
        "observation": "recovery-observation.json", "backoff": "recovery-backoff.json", "stability": "recovery-stability.json",
    }
    return local_report(files[workload])


@router.get("/demo/admission/", include_in_schema=False)
async def admission_demo():
    return demo_file("web/admission.html")


@router.get("/demo/admission-results.json", include_in_schema=False)
async def admission_results():
    return local_report("admission.json")


@router.get("/demo/shared-backend/", include_in_schema=False)
async def shared_backend_demo():
    return demo_file("web/shared-backend.html")


def local_report(filename):
    report = DEMO.parent / "results" / "raw" / filename
    if not report.is_file():
        raise HTTPException(404, "No experiment result is available.")
    return FileResponse(report, media_type="application/json", headers=NO_CACHE)


@router.get("/demo/reconnect/", include_in_schema=False)
async def reconnect_demo():
    return demo_file("web/reconnect.html")


@router.get("/demo/stall/", include_in_schema=False)
async def stall_demo():
    return demo_file("web/stall.html")


@router.get("/demo/{name:path}", include_in_schema=False)
async def demo_asset(name: str):
    if name not in ASSETS:
        raise HTTPException(404)
    return demo_file(ASSETS[name])


def frames(*events):
    return "".join(f"{json.dumps(event)}\n" for event in events)


class SourceError(Exception):
    def __init__(self, message: str, *, retryable: bool = False, retry_after: float | None = None):
        super().__init__(message)
        self.retryable = retryable
        self.retry_after = retry_after


@dataclass
class WorkMeter:
    limit: int
    fixture_units: tuple[int, ...] | None = None
    observed: int | None = None

    def record_output(self, count: int):
        self.observed = count


async def scripted_chunks(cursor: int = 0) -> AsyncGenerator[str, None]:
    for text in CHUNKS[cursor:]:
        await asyncio.sleep(INTERVAL_S)
        yield text


async def stall_chunks(scenario: str, attempt: str) -> AsyncGenerator[str, None]:
    for index, text in enumerate(CHUNKS):
        gap = STALL_GAP_S[scenario] if index == STALL_AFTER and attempt == "1" else STALL_STEP_S
        await asyncio.sleep(gap)
        yield text


@router.post("/v1/chat/completions/stall")
async def stalled_stream(scenario: Literal["temporary", "persistent"], attempt: Literal["1", "2"]):
    return StreamingResponse(
        generate(stall_chunks(scenario, attempt), None),
        media_type="application/x-ndjson",
        headers=NO_CACHE,
    )


async def ollama_chunks(
    *,
    prompt: str = PROMPT,
    instructions: str = "Answer in two short sentences without Markdown.",
    max_tokens: int = STREAM_OUTPUT_LIMIT,
    temperature: float | None = None,
    accept_length_limit: bool = True,
    output_usage: Callable[[int], None] | None = None,
    on_request: Callable[[], None] | None = None,
) -> AsyncGenerator[str, None]:
    model = os.environ.get("GENERATION_CUT_OLLAMA_MODEL", DEFAULT_MODEL)
    port = os.environ.get("GENERATION_CUT_OLLAMA_PORT")
    chat_url = OLLAMA_CHAT
    if port is not None:
        try:
            port_number = int(port)
        except ValueError as exc:
            raise SourceError("GENERATION_CUT_OLLAMA_PORT must be a valid TCP port.") from exc
        if not 1 <= port_number <= 65535:
            raise SourceError("GENERATION_CUT_OLLAMA_PORT must be a valid TCP port.")
        chat_url = f"http://127.0.0.1:{port_number}/api/chat"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": instructions},
            {"role": "user", "content": prompt},
        ],
        "stream": True,
        "think": False,
        "options": {"num_ctx": 2048, "num_predict": max_tokens},
    }
    if temperature is not None:
        payload["options"]["temperature"] = temperature
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(65, connect=2), trust_env=False) as client:
            if on_request is not None:
                on_request()
            async with client.stream("POST", chat_url, json=payload) as response:
                if response.status_code == 404:
                    raise SourceError("The configured Ollama model is not installed.")
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        raise SourceError("Ollama returned an invalid stream event.")
                    if "error" in record:
                        if isinstance(record["error"], str) and record["error"].strip():
                            raise SourceError("Ollama reported a generation error.")
                        raise SourceError("Ollama returned an invalid stream event.")
                    if OLLAMA_EVENT_FIELDS.isdisjoint(record):
                        raise SourceError("Ollama returned an invalid stream event.")
                    done = record.get("done", False)
                    message = record.get("message", {})
                    if type(done) is not bool or not isinstance(message, dict):
                        raise SourceError("Ollama returned an invalid stream event.")
                    content = message.get("content", "")
                    if not isinstance(content, str):
                        raise SourceError("Ollama returned an invalid stream event.")
                    if done:
                        output = record.get("eval_count")
                        if output_usage and type(output) is int and output >= 0:
                            output_usage(output)
                    if content:
                        yield content
                    if done:
                        if not accept_length_limit and record.get("done_reason") == "length":
                            raise SourceError("Output limit reached before the answer finished.")
                        return
    except httpx.ConnectError as exc:
        raise SourceError(f"Cannot reach Ollama at {chat_url}.", retryable=True) from exc
    except httpx.TimeoutException as exc:
        raise SourceError("Ollama did not respond in time.", retryable=True) from exc
    except (httpx.NetworkError, httpx.RemoteProtocolError) as exc:
        raise SourceError("Ollama's connection broke.", retryable=True) from exc
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status in {408, 429} or 500 <= status < 600:
            try:
                delay = float(exc.response.headers.get("Retry-After", ""))
            except ValueError:
                delay = None
            if delay is not None and (not math.isfinite(delay) or delay < 0):
                delay = None
            raise SourceError(f"Ollama returned HTTP {status}.", retryable=True, retry_after=delay) from exc
        raise SourceError(f"Ollama rejected the request (HTTP {status}).") from exc
    except (httpx.HTTPError, ValueError) as exc:
        raise SourceError("Ollama did not return a usable stream.") from exc
    raise SourceError("Ollama ended the stream without a completion event.", retryable=True)


async def generate(chunks: AsyncGenerator[str, None], cut_at: int | None, meter: WorkMeter | None = None):
    visible_chunks = 0
    fixture = iter(meter.fixture_units) if meter and meter.fixture_units is not None else None

    def usage_frame():
        return frames({"type": "usage", "output": meter.observed}) if meter and meter.observed is not None else ""

    try:
        async with aclosing(chunks):
            if meter:
                yield frames({"type": "work-limit", "limit": meter.limit})
            async for text in chunks:
                generated = {"type": "generated"}
                if fixture is not None:
                    generated["units"] = next(fixture)
                if visible_chunks == cut_at:
                    yield frames(generated) + usage_frame()
                    break
                yield frames(generated, {"type": "delta", "text": text})
                if text.strip():
                    visible_chunks += 1
            else:
                if meter and meter.fixture_units is None and not visible_chunks:
                    raise SourceError("Ollama completed without visible text.")
                yield usage_frame() + frames({"type": "done"})
                return
    except SourceError as exc:
        error = {"type": "error", "message": str(exc)}
        if exc.retryable:
            error["retryable"] = True
        if exc.retry_after is not None:
            error["retryAfterSeconds"] = exc.retry_after
        yield usage_frame() + frames(error)
        return

    await asyncio.sleep(INTERVAL_S)
    # Abort the response mid-transfer to simulate a dropped connection.
    raise RuntimeError("Generation Interrupted injected a streaming disconnect")


def require_local(request: Request):
    origin = request.headers.get("origin")
    if request.url.hostname not in {"127.0.0.1", "localhost", "::1"} or (
        origin is not None and origin != str(request.base_url).rstrip("/")
    ):
        raise HTTPException(403, "Live generation is only available on the local demo.")


@router.post("/v1/chat/completions/stream")
async def stream(
    request: Request,
    scenario: Literal["before", "after", "late", "healthy"],
    attempt: Literal["1", "2"],
    source: Literal["scripted", "ollama"] = "scripted",
    cursor: int | None = None,
):
    if cursor is not None and (source != "scripted" or attempt != "2" or not 0 <= cursor <= len(CHUNKS)):
        raise HTTPException(422, "The cursor is only valid for a scripted retry.")
    if source == "ollama":
        require_local(request)
    cut_at = CUT_AT.get(scenario) if attempt == "1" else None
    if source == "ollama":
        meter = WorkMeter(STREAM_OUTPUT_LIMIT)
        chunks = ollama_chunks(output_usage=meter.record_output, accept_length_limit=False)
    else:
        units = SCRIPT_WORK[cursor or 0:]
        meter = WorkMeter(sum(units), units)
        chunks = scripted_chunks(cursor or 0)
    return StreamingResponse(generate(chunks, cut_at, meter), media_type="application/x-ndjson", headers=NO_CACHE)
