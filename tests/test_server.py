import asyncio, time
import httpx, pytest
from collections import Counter
from generation_cut.server import choose_fault, outage_failure
from generation_cut.harness import ServerProc

def test_fault_sequence_is_deterministic_and_matches_rates():
    rates = {"http429": 0.1, "http500": 0.2, "garbage": 0.05}
    a = [choose_fault(5, i, rates) for i in range(20000)]
    assert a == [choose_fault(5, i, rates) for i in range(20000)]
    assert a != [choose_fault(6, i, rates) for i in range(20000)]
    c = Counter(a)
    for k, r in rates.items(): assert abs(c[k] / 20000 - r) < 0.01
    assert abs(c["ok"] / 20000 - 0.65) < 0.01

@pytest.fixture(scope="module")
def srv():
    with ServerProc() as s: yield s

def post(srv, **kw): return httpx.post(srv.url + "/v1/chat/completions", json={}, **kw)

def test_healthy_server_returns_json_content(srv):
    srv.config(rates={}, service_s=0.01)
    r = post(srv); assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == '{"answer": 42}'

def test_each_fault_behaves_as_documented(srv):
    srv.config(rates={"http429": 1.0}, retry_after_s=0.3); r = post(srv)
    assert r.status_code == 429 and r.headers["retry-after"] == "0.3"
    srv.config(rates={"http500": 1.0}, service_s=0.01); assert post(srv).status_code == 500
    srv.config(rates={"garbage": 1.0}, service_s=0.01); r = post(srv)
    assert r.status_code == 200 and "forty-two" in r.text
    srv.config(rates={"truncate": 1.0}, service_s=0.01)
    with pytest.raises(httpx.TransportError): post(srv)
    srv.config(rates={"hang": 1.0}, hang_s=2.0, service_s=0.01)
    with pytest.raises(httpx.ReadTimeout): post(srv, timeout=0.3)

def test_outage_window_returns_500_then_recovers(srv):
    srv.config(rates={}, service_s=0.01, outage_start=0.0, outage_end=1.0)
    assert post(srv).status_code == 500
    time.sleep(1.1); assert post(srv).status_code == 200

def test_outage_profile_changes_at_equal_thirds_and_ends_at_cutoff():
    state = {"outage_start": 1, "outage_end": 7, "outage_profile": [1, 0, 1], "seed": 12,
             "outage_attempts": [0, 0, 0], "outage_failures": [0, 0, 0]}
    times = [.999, 1, 2.999, 3, 4.999, 5, 6.999, 7]
    assert [outage_failure(state, t, i) for i, t in enumerate(times)] == [False, True, True, False, False, True, True, False]
    assert state["outage_attempts"] == [2, 2, 2]
    assert state["outage_failures"] == [2, 0, 2]


def test_outage_profile_reuses_seeded_draws_for_the_same_attempt():
    base = {"outage_start": 1, "outage_end": 4, "seed": 12}

    def run(probability):
        state = {**base, "outage_profile": [probability] * 3, "outage_attempts": [0] * 3, "outage_failures": [0] * 3}
        return {i for i in range(100) if outage_failure(state, 1.5, i)}

    small, large = run(.3), run(.9)
    assert small == run(.3) and small < large


def test_profile_assignments_are_reported_and_failed_attempts_consume_service(srv):
    srv.config(service_s=.01, outage_start=0, outage_end=60, outage_profile=[1, 0, 1])
    assert post(srv).status_code == 500
    stats = srv.stats()
    assert stats["served"] == 1
    assert stats["outage"] == {"attempts": [1, 0, 0], "assigned_failures": [1, 0, 0]}
    srv.config(service_s=.01)
    assert post(srv).status_code == 200
    assert "outage" not in srv.stats()


@pytest.mark.parametrize("profile", [[.9, .6], [.9, .6, True], [.9, -1, .3]])
def test_outage_profile_rejects_invalid_probabilities(srv, profile):
    response = httpx.post(srv.url + "/_config", json={"outage_start": 1, "outage_end": 4, "outage_profile": profile})
    assert response.status_code == 422


@pytest.mark.parametrize(("cfg", "field"), [
    ({"slots": 0}, "slots"),
    ({"slots": True}, "slots"),
    ({"service_s": -1}, "service_s"),
    ({"hang_s": "slow"}, "hang_s"),
    ({"retry_after_s": -1}, "retry_after_s"),
    ({"rates": {"http500": 0.7, "hang": 0.5}}, "rates"),
    ({"rates": {"unknown": 0.25}}, "rates"),
    ({"outage_start": 1}, "outage_end"),
    ({"outage_start": 2, "outage_end": 1}, "outage_end"),
    ({"outage_profile": [1, 0, 1]}, "outage_profile"),
    ({"slot": 2}, "Unknown mock settings"),
])
def test_invalid_config_does_not_reset_running_trial(srv, cfg, field):
    srv.config(slots=1, service_s=0)
    assert post(srv).status_code == 200
    before = srv.stats()

    response = httpx.post(srv.url + "/_config", json=cfg)

    assert response.status_code == 422
    assert field in response.json()["detail"]
    assert srv.stats() == before


def test_abandoned_requests_still_consume_server_capacity():
  with ServerProc() as srv:   # own server: leftover handlers from other tests must not count
      srv.config(rates={}, service_s=0.2, slots=1)
      async def go():
          async with httpx.AsyncClient(base_url=srv.url) as c:
              await asyncio.gather(*[c.post("/v1/chat/completions", json={}, timeout=0.05) for _ in range(5)], return_exceptions=True)
      asyncio.run(go())
      time.sleep(1.4)    # 5 x 0.2 s of work was queued even though every client gave up after 0.05 s
      assert srv.stats()["served"] == 5
