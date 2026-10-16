"""Measure actual SDK retries against the loopback fault server."""

import asyncio
import json
import time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass

import httpx

from .harness import OPEN_LOAD_LIMITS
from .policies import Outcome
from .recovery_budget import WholeCreditBudget


RETRYABLE = {"timeout", "conn", "http429", "http5xx", "bad_output"}
_DENIED = "x-generation-cut-budget-denied"


@dataclass
class Trace:
    dispatched: int = 0


class MeteredTransport(httpx.AsyncBaseTransport):
    def __init__(self, budget):
        self.inner = httpx.AsyncHTTPTransport(trust_env=False, limits=OPEN_LOAD_LIMITS)
        self.budget = budget
        self.current = ContextVar("generation_cut_sdk_trace")
        self.dispatched = 0
        self.denied = 0

    async def handle_async_request(self, request):
        trace = self.current.get()
        if trace.dispatched and self.budget and not self.budget.try_retry():
            self.denied += 1
            return httpx.Response(400, headers={_DENIED: "1"},
                                  json={"error": {"message": "Generation Interrupted retry budget exhausted"}})
        trace.dispatched += 1
        self.dispatched += 1
        return await self.inner.handle_async_request(request)

    async def aclose(self):
        await self.inner.aclose()


class SdkRetryStack:
    def __init__(self, *, stacked=False, shared=False):
        self.stacked = stacked
        self.budget = WholeCreditBudget() if shared else None
        self.transport = MeteredTransport(self.budget)
        self.app_calls = 0
        self.sdk_calls = 0

    @property
    def denied(self):
        return self.transport.denied

    @property
    def layers(self):
        return {"app_calls": self.app_calls, "sdk_calls": self.sdk_calls,
                "transport_attempts": self.transport.dispatched}

    @asynccontextmanager
    async def connect(self, url):
        import openai

        target = httpx.URL(url)
        if target.scheme != "http" or target.host != "127.0.0.1":
            raise ValueError("The SDK experiment only supports the loopback fault server.")
        client = httpx.AsyncClient(transport=self.transport, trust_env=False, follow_redirects=False)
        async with openai.AsyncOpenAI(
            api_key="generation-cut-loopback", organization="generation-cut", project="generation-cut",
            base_url=f"{url}/v1", max_retries=1, timeout=1.0, http_client=client,
        ) as sdk:
            self._sdk = sdk
            yield self

    async def call(self, _client):
        import openai

        started = time.monotonic()
        trace = Trace()
        marker = self.transport.current.set(trace)
        if self.budget:
            self.budget.request()

        async def sdk_call():
            self.sdk_calls += 1
            try:
                completion = await self._sdk.chat.completions.create(model="generation-cut-mock", messages=[])
            except openai.APIStatusError as exc:
                if exc.response.headers.get(_DENIED) == "1":
                    return "budget"
                if exc.status_code == 429:
                    return "http429"
                if exc.status_code >= 500:
                    return "http5xx"
                return "timeout" if exc.status_code == 408 else "http4xx"
            except openai.APITimeoutError:
                return "timeout"
            except openai.APIConnectionError:
                return "conn"
            try:
                json.loads(completion.choices[0].message.content)
            except (AttributeError, IndexError, TypeError, ValueError):
                return "bad_output"
            return "ok"

        async def retry_once(call):
            kind = await call()
            if kind in RETRYABLE:
                await asyncio.sleep(.05)
                return await call()
            return kind

        async def app():
            self.app_calls += 1
            return await retry_once(sdk_call)

        try:
            kind = await retry_once(app) if self.stacked else await sdk_call()
            return Outcome(kind == "ok", trace.dispatched, time.monotonic() - started,
                           "" if kind == "ok" else kind)
        finally:
            self.transport.current.reset(marker)
