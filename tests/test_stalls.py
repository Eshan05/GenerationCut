import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from generation_cut import streaming


@pytest.mark.parametrize(("scenario", "attempt", "pause"), [
    ("temporary", "1", True),
    ("persistent", "1", True),
    ("persistent", "2", False),
])
def test_stalled_fixture_delays_only_the_first_attempt_without_cutting_it(monkeypatch, scenario, attempt, pause):
    sleep = AsyncMock()
    monkeypatch.setattr(streaming.asyncio, "sleep", sleep)

    async def verify():
        payloads = [payload async for payload in streaming.generate(streaming.stall_chunks(scenario, attempt), None)]
        events = [json.loads(line) for payload in payloads for line in payload.splitlines()]
        assert [event["text"] for event in events if event["type"] == "delta"] == list(streaming.CHUNKS)
        assert events[-1] == {"type": "done"}

    asyncio.run(verify())
    expected = [streaming.STALL_STEP_S] * len(streaming.CHUNKS)
    if pause:
        expected[streaming.STALL_AFTER] = streaming.STALL_GAP_S[scenario]
    assert [call.args[0] for call in sleep.await_args_list] == expected
    assert streaming.STALL_GAP_S["persistent"] > 15
