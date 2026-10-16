import asyncio
import json
from contextlib import aclosing

import httpx
import pytest

from generation_cut import shared_backend
from generation_cut.server import app
from generation_cut.streaming import SourceError


async def run_trial(policy, chunks):
    return [event async for event in shared_backend.trial(policy, chunks, timeout=2)]


@pytest.mark.parametrize("policy", ["queued", "immediate"])
def test_two_users_share_scripted_decoder(policy, monkeypatch):
    monkeypatch.setattr(shared_backend, "SCRIPT_STEP_S", 0)
    events = asyncio.run(run_trial(policy, shared_backend.scripted_source()))

    def positions(lane, kind):
        return [i for i, event in enumerate(events) if event["lane"] == lane and event["type"] == kind]

    long_text = positions("long", "delta")
    short_arrival, = positions("short", "arrival")
    short_dispatch, = positions("short", "dispatch")
    long_done, = positions("long", "done")
    assert len(long_text) == len(shared_backend.SCRIPTS["long"])
    assert len(positions("short", "delta")) == len(shared_backend.SCRIPTS["short"])
    assert short_arrival == long_text[2] + 1
    assert len(positions("short", "done")) == 1
    if policy == "queued":
        assert short_arrival < long_done < short_dispatch
    else:
        assert short_arrival < short_dispatch < positions("short", "delta")[0] < long_text[-1]
    assert [event["ms"] for event in events] == sorted(event["ms"] for event in events)


def test_short_request_is_not_created_when_long_fails_before_arrival():
    async def chunks(lane):
        assert lane == "long"
        yield "First"
        yield " "
        raise SourceError("Upstream stopped.")

    events = asyncio.run(run_trial("immediate", chunks))
    assert [(event["type"], event["lane"]) for event in events if event["lane"] == "short"] == [
        ("skipped", "short")
    ]
    assert any(event["type"] == "error" and event["lane"] == "long" for event in events)


def test_short_request_is_still_dispatched_if_long_finishes_at_arrival():
    async def chunks(lane):
        for text in (["one", "two", "three"] if lane == "long" else ["answer"]):
            yield text

    events = asyncio.run(run_trial("queued", chunks))
    assert any(event["type"] == "done" and event["lane"] == "short" for event in events)


def test_cancel_closes_long_stream_at_trigger_without_holding_short_dispatch():
    async def verify():
        short_started = asyncio.Event()
        long_closed = asyncio.Event()

        async def chunks(lane):
            if lane == "short":
                short_started.set()
                yield "A short answer."
                return
            try:
                for text in ("one", " ", "two", "three", "must not be read"):
                    yield text
            finally:
                # Simulate a connection close that takes longer than dispatching B.
                await short_started.wait()
                long_closed.set()

        events = await run_trial("cancel", chunks)
        assert long_closed.is_set()
        assert [e["text"] for e in events if e["lane"] == "long" and e["type"] == "delta"] == [
            "one", " ", "two", "three"
        ]
        marks = [(e["lane"], e["type"]) for e in events]
        assert ("long", "done") not in marks
        assert marks.index(("short", "arrival")) < marks.index(("short", "dispatch"))
        assert marks.index(("short", "dispatch")) < marks.index(("long", "cancelled"))
        assert ("short", "done") in marks

    asyncio.run(verify())


@pytest.mark.parametrize("order", ["baseline", "reverse"])
def test_cancellation_pair_can_run_in_either_order(monkeypatch, order):
    monkeypatch.setattr(shared_backend, "SCRIPT_STEP_S", 0)

    async def verify():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            response = await client.post(f"/v1/chat/completions/shared-backend?comparison=cancellation&order={order}")
            assert response.status_code == 200
            events = [json.loads(line) for line in response.text.splitlines()]
        expected = ["cancel", "keep"] if order == "reverse" else ["keep", "cancel"]
        assert [e["policy"] for e in events if e["type"] == "trial-end"] == expected

        for policy in expected:
            marks = [(e["lane"], e["type"]) for e in events if e.get("policy") == policy and "lane" in e]
            assert marks.index(("short", "arrival")) < marks.index(("short", "dispatch"))
            assert ("short", "done") in marks
        assert [e["policy"] for e in events if e["type"] == "cancelled"] == ["cancel"]
        assert [e["policy"] for e in events if e["type"] == "done" and e["lane"] == "long"] == ["keep"]

    asyncio.run(verify())


def test_disconnect_closes_both_inflight_sources():
    async def verify():
        closed = set()

        async def chunks(lane):
            try:
                for text in ("one", "two", "three"):
                    yield text
                await asyncio.Future()
            finally:
                closed.add(lane)

        async with aclosing(shared_backend.trial("immediate", chunks, timeout=2)) as stream:
            async for event in stream:
                if event["type"] == "delta" and event["lane"] == "short":
                    break
        assert closed == {"long", "short"}

    asyncio.run(verify())


@pytest.mark.parametrize("order", ["baseline", "reverse"])
def test_saturation_only_releases_one_slot_when_a_stops(monkeypatch, order):
    monkeypatch.setattr(shared_backend, "SCRIPT_STEP_S", 0)
    monkeypatch.setattr(shared_backend, "STOP_DELAY_S", 0)

    async def verify():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            response = await client.post(f"/v1/chat/completions/shared-backend?comparison=saturation&order={order}")
        assert response.status_code == 200
        events = [json.loads(line) for line in response.text.splitlines()]
        assert set(events[0]["prompts"]) == {"long", "peer", "short"}
        assert events[-1]["type"] == "comparison-end"
        assert not any(e["type"] in {"trial-invalid", "error", "timeout"} for e in events)
        expected = ["stop-a", "keep-both"] if order == "reverse" else ["keep-both", "stop-a"]
        assert [e["policy"] for e in events if e["type"] == "trial-end"] == expected

        for policy in expected:
            marks = [(e["lane"], e["type"]) for e in events if e.get("policy") == policy and "lane" in e]
            c_arrival = marks.index(("short", "arrival"))
            assert all(marks[:c_arrival].count((lane, "delta")) >= 3 for lane in ("long", "peer"))
            c_first = marks.index(("short", "delta"))
            assert c_arrival < marks.index(("short", "dispatch")) < c_first < marks.index(("short", "done"))
            assert marks.count(("peer", "delta")) == len(shared_backend.SCRIPTS["peer"])
            a_count = marks.count(("long", "delta"))
            if policy == "stop-a":
                assert marks.index(("short", "dispatch")) < marks.index(("long", "cancelled")) < c_first
                assert c_first < marks.index(("peer", "done"))
                assert 3 <= a_count < len(shared_backend.SCRIPTS["long"])
                assert ("long", "done") not in marks
            else:
                assert min(marks.index(("long", "done")), marks.index(("peer", "done"))) < c_first
                assert a_count == len(shared_backend.SCRIPTS["long"])

    asyncio.run(verify())


def test_saturation_reports_when_two_streams_never_overlap(monkeypatch):
    monkeypatch.setattr(shared_backend, "SCRIPT_STEP_S", 0)

    async def verify():
        events = [e async for e in shared_backend.saturation_trial("stop-a", shared_backend.scripted_source(1), 2)]
        assert events[-1]["type"] == "trial-invalid"
        assert not any(e.get("lane") == "short" or e["type"] == "cancelled" for e in events)

    asyncio.run(verify())


def test_saturation_rejects_c_streaming_before_the_stop(monkeypatch):
    monkeypatch.setattr(shared_backend, "STOP_DELAY_S", 0)

    async def chunks(lane):
        for text in ("one", "two", "three"):
            yield text
        await asyncio.Future()

    async def verify():
        events = [e async for e in shared_backend.saturation_trial("stop-a", chunks, 2)]
        assert events[-1]["type"] == "trial-invalid"
        assert any(e.get("lane") == "short" and e["type"] == "delta" for e in events)
        assert not any(e["type"] == "cancelled" for e in events)

    asyncio.run(verify())


def test_saturation_disconnect_closes_all_inflight_sources():
    async def verify():
        closed = set()

        async def chunks(lane):
            try:
                if lane != "short":
                    for text in ("one", "two", "three"):
                        yield text
                await asyncio.Future()
            finally:
                closed.add(lane)

        async with aclosing(shared_backend.saturation_trial("keep-both", chunks, 2)) as stream:
            async for event in stream:
                if event.get("lane") == "short" and event["type"] == "dispatch":
                    break
        assert closed == {"long", "peer", "short"}

    asyncio.run(verify())


def test_shared_backend_page_and_local_only_live_api():
    async def verify():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1") as client:
            page = await client.get("/demo/shared-backend/")
            assert page.status_code == 200 and 'value="saturation"' in page.text
            response = await client.post(
                "/v1/chat/completions/shared-backend?source=ollama", headers={"Origin": "https://example.test"}
            )
            assert response.status_code == 403

    asyncio.run(verify())
