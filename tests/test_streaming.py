import asyncio
import json

import httpx
import pytest

from generation_cut.harness import ServerProc
from generation_cut import streaming
from generation_cut.streaming import CHUNKS


@pytest.fixture(scope="module")
def srv():
    with ServerProc() as server:
        yield server


def stream(srv, scenario, attempt):
    return httpx.stream(
        "POST", srv.url + "/v1/chat/completions/stream",
        params={"scenario": scenario, "attempt": attempt},
    )


@pytest.mark.parametrize("scenario, shown", [("before", 0), ("after", 3), ("late", 6)])
def test_scripted_fault_closes_transport_without_completion(srv, scenario, shown):
    events = []
    with stream(srv, scenario, 1) as response:
        assert response.status_code == 200
        with pytest.raises(httpx.RemoteProtocolError):
            for line in response.iter_lines():
                events.append(json.loads(line))
    assert [event["text"] for event in events if event["type"] == "delta"] == list(CHUNKS[:shown])
    assert events[0] == {"type": "work-limit", "limit": sum(streaming.SCRIPT_WORK)}
    assert [event["units"] for event in events if event["type"] == "generated"] == list(streaming.SCRIPT_WORK[:shown + 1])
    assert {"type": "done"} not in events


def test_retry_completes_with_a_terminal_event(srv):
    with stream(srv, "after", 2) as response:
        assert response.status_code == 200
        events = [json.loads(line) for line in response.iter_lines()]
    assert [event["text"] for event in events if event["type"] == "delta"] == list(CHUNKS)
    assert events[-1] == {"type": "done"}


def test_scripted_resume_emits_only_the_suffix(srv):
    with httpx.stream("POST", srv.url + "/v1/chat/completions/stream",
                      params={"scenario": "after", "attempt": "2", "cursor": 3}) as response:
        assert response.status_code == 200
        events = [json.loads(line) for line in response.iter_lines()]
    assert [event["text"] for event in events if event["type"] == "delta"] == list(CHUNKS[3:])
    assert events[0] == {"type": "work-limit", "limit": sum(streaming.SCRIPT_WORK[3:])}
    assert events[-1] == {"type": "done"}


def test_cursor_is_rejected_for_live_source(srv):
    response = httpx.post(srv.url + "/v1/chat/completions/stream",
                          params={"scenario": "after", "attempt": "2", "source": "ollama", "cursor": 3})
    assert response.status_code == 422


def test_demo_is_served_from_the_same_app(srv):
    response = httpx.get(srv.url + "/demo/")
    assert response.status_code == 200
    assert "Stream failure lab" in response.text
    assert httpx.get(srv.url + "/demo/../../pyproject.toml").status_code == 404
    assert "Recovery comparison" in httpx.get(srv.url + "/demo/recovery/").text
    assert "Streaming admission" in httpx.get(srv.url + "/demo/admission/").text


def test_cross_origin_live_request_is_rejected(srv):
    response = httpx.post(
        srv.url + "/v1/chat/completions/stream",
        params={"scenario": "healthy", "attempt": "1", "source": "ollama"},
        headers={"Origin": "https://example.test"},
    )
    assert response.status_code == 403


def mock_ollama(monkeypatch, records, status=200, chat_url=streaming.OLLAMA_CHAT, headers=None):
    def respond(request):
        assert str(request.url) == chat_url
        payload = json.loads(request.content)
        assert payload["stream"] is True
        assert payload["messages"][-1]["content"] == streaming.PROMPT
        return httpx.Response(status, headers=headers,
                              content="".join(f"{json.dumps(record)}\n" for record in records))

    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    monkeypatch.setattr(streaming.httpx, "AsyncClient", lambda **_: client)
    return client


async def collect_ollama(cut_at=None, **options):
    return [
        json.loads(line)
        for event in [event async for event in streaming.generate(streaming.ollama_chunks(**options), cut_at)]
        for line in event.splitlines()
    ]


def test_ollama_forwards_text_and_provider_completion(monkeypatch):
    mock_ollama(monkeypatch, [
        {"message": {"thinking": "not visible"}, "done": False},
        {"message": {"content": "A cache "}, "done": False},
        {"message": {"content": "saves work."}, "done": True},
    ])
    events = asyncio.run(collect_ollama())
    assert [event["text"] for event in events if event["type"] == "delta"] == ["A cache ", "saves work."]
    assert events[-1] == {"type": "done"}


def test_ollama_can_use_a_separate_loopback_port(monkeypatch):
    monkeypatch.setenv("GENERATION_CUT_OLLAMA_PORT", "11435")
    mock_ollama(monkeypatch, [{"message": {"content": "Ready."}, "done": True}],
                chat_url="http://127.0.0.1:11435/api/chat")
    assert asyncio.run(collect_ollama())[-1] == {"type": "done"}


@pytest.mark.parametrize("port", ["https://example.test", "0", "65536"])
def test_ollama_rejects_invalid_loopback_port(monkeypatch, port):
    monkeypatch.setenv("GENERATION_CUT_OLLAMA_PORT", port)
    assert asyncio.run(collect_ollama()) == [
        {"type": "error", "message": "GENERATION_CUT_OLLAMA_PORT must be a valid TCP port."}
    ]


def test_ollama_eof_is_a_source_error_not_completion(monkeypatch):
    mock_ollama(monkeypatch, [{"message": {"content": "Partial"}, "done": False}])
    events = asyncio.run(collect_ollama())
    assert [event["type"] for event in events] == ["generated", "delta", "error"]
    assert events[-1]["retryable"] is True


@pytest.mark.parametrize("status", [408, 429, 503])
def test_temporary_ollama_http_failures_carry_retry_hint(monkeypatch, status):
    mock_ollama(monkeypatch, [], status=status, headers={"Retry-After": "0.5"})
    assert asyncio.run(collect_ollama()) == [
        {"type": "error", "message": f"Ollama returned HTTP {status}.", "retryable": True, "retryAfterSeconds": .5}
    ]


def test_missing_ollama_model_is_not_retryable(monkeypatch):
    mock_ollama(monkeypatch, [], status=404)
    assert asyncio.run(collect_ollama()) == [{"type": "error", "message": "The configured Ollama model is not installed."}]


@pytest.mark.parametrize("limited", [False, True])
def test_completed_ollama_usage_is_reported_even_on_output_limit(monkeypatch, limited):
    record = {"message": {"content": "Some text"}, "done": True, "eval_count": 12}
    if limited:
        record["done_reason"] = "length"
    mock_ollama(monkeypatch, [record])

    async def run():
        meter = streaming.WorkMeter(96)
        chunks = streaming.ollama_chunks(output_usage=meter.record_output, accept_length_limit=False)
        return [json.loads(line) async for batch in streaming.generate(chunks, None, meter) for line in batch.splitlines()]

    events = asyncio.run(run())
    assert events[0] == {"type": "work-limit", "limit": 96}
    assert events[-2] == {"type": "usage", "output": 12}
    assert events[-1]["type"] == ("error" if limited else "done")


def test_length_limit_can_be_treated_as_incomplete(monkeypatch):
    mock_ollama(monkeypatch, [{"message": {"content": "Partial"}, "done": True, "done_reason": "length"}])
    events = asyncio.run(collect_ollama(accept_length_limit=False))
    assert [event["type"] for event in events] == ["generated", "delta", "error"]
    assert events[1]["text"] == "Partial"


def test_ollama_internal_error_is_not_displayed_verbatim(monkeypatch):
    mock_ollama(monkeypatch, [{"error": "internal provider detail"}])
    assert asyncio.run(collect_ollama()) == [{"type": "error", "message": "Ollama reported a generation error."}]


@pytest.mark.parametrize("record", [
    {},
    {"irrelevant": 1},
    {"error": ""},
    {"message": {"content": 17}, "done": True},
    {"message": {"content": 17}, "done": "true"},
    {"message": "invalid", "done": False},
    {"message": {"content": "A valid string"}, "done": "false"},
])
def test_malformed_ollama_events_are_terminal(monkeypatch, record):
    mock_ollama(monkeypatch, [record])
    assert asyncio.run(collect_ollama()) == [{"type": "error", "message": "Ollama returned an invalid stream event."}]


@pytest.mark.parametrize("malformed", [False, True])
def test_ollama_preserves_valid_partial_text_and_distinguishes_eof_from_bad_event(monkeypatch, malformed):
    records = [{"model": "test"}, {"message": {"content": "", "thinking": "hidden"}, "done": False},
               {"message": {"content": "Partial"}, "done": False}]
    if malformed:
        records += [{}, {"done": True}]
    mock_ollama(monkeypatch, records)
    events = asyncio.run(collect_ollama())
    assert events[:2] == [{"type": "generated"}, {"type": "delta", "text": "Partial"}]
    assert events[2]["type"] == "error"
    assert events[2].get("retryable", False) == (not malformed)
    assert len(events) == 3


def test_ollama_terminal_metadata_without_visible_output_is_not_complete(monkeypatch):
    mock_ollama(monkeypatch, [{"model": "test"}, {"done": True, "eval_count": 0}])

    async def run():
        meter = streaming.WorkMeter(96)
        chunks = streaming.ollama_chunks(output_usage=meter.record_output)
        return [json.loads(line) async for batch in streaming.generate(chunks, None, meter) for line in batch.splitlines()]

    assert asyncio.run(run())[-2:] == [
        {"type": "usage", "output": 0},
        {"type": "error", "message": "Ollama completed without visible text."},
    ]


def test_injected_cut_closes_ollama_connection(monkeypatch):
    monkeypatch.setattr(streaming, "INTERVAL_S", 0)
    client = mock_ollama(monkeypatch, [{"message": {"content": "Hidden"}, "done": False}])
    with pytest.raises(RuntimeError, match="Generation Interrupted injected a streaming disconnect"):
        asyncio.run(collect_ollama(cut_at=0))
    assert client.is_closed
