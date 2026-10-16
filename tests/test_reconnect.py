import asyncio
import json
from contextlib import aclosing

import httpx
import pytest

from generation_cut import reconnect, streaming
from generation_cut.server import app


def test_browser_disconnect_replays_same_generation(monkeypatch):
    async def verify():
        resume = asyncio.Event()
        calls = 0

        async def model(*, on_request, **_):
            nonlocal calls
            on_request()
            calls += 1
            yield "first "
            await resume.wait()
            yield "second"

        monkeypatch.setattr(reconnect, "ollama_chunks", model)
        run = reconnect.DeliveryRun()
        async with aclosing(run.subscribe(0)) as delivery:
            received = [json.loads(await anext(delivery)) for _ in range(2)]
        assert run.listeners == 0
        assert not run.task.done()
        resume.set()
        await run.task
        replayed = [json.loads(line) async for line in run.subscribe(received[-1]["seq"])]
        assert calls == 1
        assert [event["seq"] for event in received + replayed] == [1, 2, 3, 4]
        assert [event["type"] for event in received + replayed] == ["started", "delta", "delta", "done"]
        assert replayed[0]["text"] == "second"
        assert replayed[0]["replayed"]

    asyncio.run(verify())


@pytest.mark.parametrize("action", ["continue", "regenerate"])
def test_upstream_cut_closes_source_and_recovery_starts_a_new_request(monkeypatch, action):
    async def verify():
        calls = []
        closed = []

        async def model(*, on_request, **options):
            on_request()
            calls.append(options)
            try:
                for chunk in ("An ", " ", "answer ", "begins ", "here."):
                    yield chunk
            finally:
                closed.append(True)

        monkeypatch.setattr(reconnect, "ollama_chunks", model)
        monkeypatch.setattr(reconnect, "runs", {})
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            first = (await client.post("/v1/stream-runs?fault=upstream")).json()["id"]
            before = await client.get(f"/v1/stream-runs/{first}/events")
            events = [json.loads(line) for line in before.text.splitlines()]
            assert "".join(e["text"] for e in events if e["type"] == "delta") == "An  answer begins "
            assert events[-1]["type"] == "upstream-cut"
            assert len(closed) == len(calls) == 1
            assert (await client.get(f"/v1/stream-runs/{first}/events?after=999")).status_code == 422

            second = (await client.post(f"/v1/stream-runs/{first}/retry?action={action}")).json()["id"]
            assert second != first
            after = await client.get(f"/v1/stream-runs/{second}/events")
            assert json.loads(after.text.splitlines()[-1])["type"] == "done"
            assert len(closed) == len(calls) == 2
            if action == "continue":
                assert "An  answer begins " in calls[1]["prompt"]
            else:
                assert "prompt" not in calls[1]

    asyncio.run(verify())


@pytest.mark.parametrize("reason", ["expired", "stopped"])
def test_abandoned_and_explicitly_stopped_runs_close_upstream(monkeypatch, reason):
    async def verify():
        closed = asyncio.Event()

        async def model(*, on_request, **_):
            on_request()
            try:
                yield "partial"
                await asyncio.Future()
            finally:
                closed.set()

        monkeypatch.setattr(reconnect, "ollama_chunks", model)
        monkeypatch.setattr(reconnect, "GRACE_S", 0.02)
        run = reconnect.DeliveryRun()
        async with aclosing(run.subscribe(0)) as delivery:
            assert json.loads(await anext(delivery))["type"] == "started"
            assert json.loads(await anext(delivery))["type"] == "delta"
            if reason == "stopped":
                run.stop()
                assert json.loads(await anext(delivery))["type"] == "stopped"
        await asyncio.wait_for(asyncio.gather(run.task, return_exceptions=True), timeout=2)
        assert closed.is_set()
        assert run.events[-1]["type"] == reason

    asyncio.run(verify())


def test_live_reconnect_rejects_cross_origin_requests():
    async def verify():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            assert (await client.get("/demo/reconnect/")).status_code == 200
            response = await client.post("/v1/stream-runs?fault=browser", headers={"Origin": "https://example.test"})
            assert response.status_code == 403

    asyncio.run(verify())


@pytest.mark.parametrize("port, expected_events", [
    ("invalid", ["error"]),
    ("11434", ["started", "delta", "done"]),
])
def test_reconnect_counts_only_http_attempts(monkeypatch, port, expected_events):
    requests = []

    def reply(request):
        requests.append(request)
        return httpx.Response(200, text='{"message":{"content":"answer"},"done":true}\n')

    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(reply)
    monkeypatch.setenv("GENERATION_CUT_OLLAMA_PORT", port)
    monkeypatch.setattr(streaming.httpx, "AsyncClient", lambda **kwargs: real_client(transport=transport, **kwargs))

    async def verify():
        run = reconnect.DeliveryRun()
        events = [json.loads(line)["type"] async for line in run.subscribe(0)]
        assert events == expected_events

    asyncio.run(verify())
    assert len(requests) == expected_events.count("started")
