import asyncio
import json
import random
import time

import httpx
import pytest

from generation_cut import harness, recovery, streaming
from generation_cut.harness import ServerProc
from generation_cut.policies import Outcome, URL
from generation_cut.recovery import CircuitPolicy, RetryStack, budget_denials, cases, main, recovery_windows, summarize
from generation_cut.sdk_retries import SdkRetryStack
from generation_cut.server import app


@pytest.mark.parametrize("layers, expected", [(1, 2), (3, 8)])
def test_stack_composes_attempt_limits(layers, expected):
    wire = 0

    def respond(_):
        nonlocal wire
        wire += 1
        return httpx.Response(500, json={})

    async def run():
        policy = RetryStack(layers, 0)
        for layer in policy.layers:
            layer.base = 0
        async with httpx.AsyncClient(base_url="http://local", transport=httpx.MockTransport(respond)) as client:
            return await policy.call(client)

    assert asyncio.run(run()).attempts == wire == expected


def test_shared_budget_is_charged_at_the_physical_attempt_boundary():
    async def run():
        stack = RetryStack(3, 0, shared=True)
        stack.budget.ratio, stack.budget.floor = 0, 1
        for layer in stack.layers:
            layer.base = 0
        transport = httpx.MockTransport(lambda _: httpx.Response(500))
        async with httpx.AsyncClient(base_url="http://local", transport=transport) as client:
            result = await stack.call(client)
        return result, stack.denied

    result, denied = asyncio.run(run())
    assert result.attempts == 2 and result.reason == "budget" and denied == 1


@pytest.mark.parametrize("case, attempts", [("retry-budget", 2), ("budget-whole", 1)])
def test_backoff_reports_budget_denials_and_labels_fractional_credit_separately(case, attempts):
    _, _, policy, _ = next(row for row in cases(0) if row[0] == case)
    policy._budget.floor = 0
    policy.base = 0
    sent = 0

    def respond(_):
        nonlocal sent
        sent += 1
        return httpx.Response(500)

    async def run():
        async with httpx.AsyncClient(base_url="http://local", transport=httpx.MockTransport(respond)) as client:
            return await policy.call(client)

    outcome = asyncio.run(run())
    assert outcome.attempts == sent == attempts
    assert budget_denials(policy, [(0.0, outcome, .1)]) == 1


def test_whole_credit_policy_releases_a_retry_only_when_one_credit_has_accrued():
    policy = next(row[2] for row in cases(0) if row[0] == "budget-whole")
    policy._budget.floor = 0
    for _ in range(9):
        policy._budget.request()
        assert not policy._budget.try_retry()
    policy._budget.request()
    assert policy._budget.try_retry()
    assert not policy._budget.try_retry()


def test_recovery_workloads_are_served_separately(tmp_path, monkeypatch):
    raw = tmp_path / "results" / "raw"
    raw.mkdir(parents=True)
    (raw / "recovery.json").write_text('{"test":"standard"}', encoding="utf-8")
    (raw / "recovery-timeout.json").write_text('{"test":"timeout"}', encoding="utf-8")
    (raw / "recovery-client-cap.json").write_text('{"test":"client-cap"}', encoding="utf-8")
    (raw / "recovery-sensitivity.json").write_text('{"test":"sensitivity"}', encoding="utf-8")
    (raw / "recovery-observation.json").write_text('{"test":"observation"}', encoding="utf-8")
    (raw / "recovery-backoff.json").write_text('{"test":"backoff"}', encoding="utf-8")
    (raw / "recovery-stability.json").write_text('{"test":"stability"}', encoding="utf-8")
    monkeypatch.setattr(streaming, "DEMO", tmp_path / "demo")

    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local") as client:
            path = "/demo/recovery-results.json"
            assert (await client.get(path)).json() == {"test": "standard"}
            assert (await client.get(path, params={"workload": "timeout"})).json() == {"test": "timeout"}
            assert (await client.get(path, params={"workload": "client-cap"})).json() == {"test": "client-cap"}
            assert (await client.get(path, params={"workload": "sensitivity"})).json() == {"test": "sensitivity"}
            assert (await client.get(path, params={"workload": "observation"})).json() == {"test": "observation"}
            assert (await client.get(path, params={"workload": "backoff"})).json() == {"test": "backoff"}
            assert (await client.get(path, params={"workload": "stability"})).json() == {"test": "stability"}
            assert (await client.get(path, params={"workload": "../standard"})).status_code == 422

    asyncio.run(run())


def test_worker_and_timeout_cases_keep_other_backoff_settings_fixed():
    rows = {case: (policy, config) for case, _, policy, config in cases(0)}
    expected = {"backoff": (1, 1.0), "short-timeout": (1, .4),
                "two-workers": (2, 1.0), "two-workers-short": (2, .4)}
    control = rows["backoff"][0]
    for case, pair in expected.items():
        policy, config = rows[case]
        assert (config.get("slots", 1), policy.timeout) == pair
        assert set(config) <= {"slots"}
        assert (policy.max_attempts, policy.max_in_flight, policy.backoff,
                policy.validate, policy.honor_retry_after) == (
            control.max_attempts, None, control.backoff, control.validate, control.honor_retry_after
        )


def test_backoff_sweep_keeps_other_client_and_server_settings_fixed():
    def options(policy):
        return {key: value for key, value in vars(policy).items() if key not in {"name", "base", "rng"}}

    rows = {case: (policy, config) for case, _, policy, config in cases(0)}
    control, settings = rows["backoff"]
    assert control.base == .1
    for case, base in [("backoff-25ms", .025), ("backoff-400ms", .4), ("backoff-800ms", .8)]:
        policy, config = rows[case]
        assert policy.base == base
        assert config == settings == {}
        assert options(policy) == options(control)
        assert policy.rng.getstate() == control.rng.getstate()


def test_backoff_sweep_is_opt_in_and_rotates_run_order_without_changing_arrival_seed(monkeypatch):
    calls = []

    class Server:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def stats(self):
            return dict(received=1, peak_queued=0, shed=0, cancelled=0)

    async def run_case(srv, policy, rate, duration, seed, config):
        assert (rate, duration, config["slots"], config["seed"]) == (16, 6, 1, seed)
        calls.append(seed)
        return [(0.0, Outcome(True, 1, .1), .1)]

    monkeypatch.setattr(recovery, "ServerProc", Server)
    monkeypatch.setattr(recovery, "run_case", run_case)
    defaults = recovery.experiment(16, 6, (1, 3), 1, 1)
    assert not any(run["case"].startswith("backoff-") for run in defaults["runs"])

    calls.clear()
    result = recovery.experiment(16, 6, (1, 3), 2, 1, selected=["backoff-400ms"])
    assert calls == [0, 0, 1, 1]
    assert [(run["case"], run["label"], run["backoff_base_s"]) for run in result["runs"]] == [
        ("backoff", "100 ms base", .1), ("backoff-400ms", "400 ms base", .4),
        ("backoff-400ms", "400 ms base", .4), ("backoff", "100 ms base", .1),
    ]


def test_client_cap_rejects_without_retry_or_http_and_recovers_a_slot_on_cancel():
    rows = {case: (policy, config) for case, _, policy, config in cases(0)}
    policy, config = rows["client-cap"]
    control, control_config = rows["backoff"]
    assert config == control_config == {}
    assert policy.max_in_flight == 4 and control.max_in_flight is None
    assert (policy.timeout, policy.max_attempts, policy.backoff, policy.validate) == (
        control.timeout, control.max_attempts, control.backoff, control.validate
    )

    async def run():
        arrived = asyncio.Event()
        finish = asyncio.Event()
        sent = 0

        async def respond(_):
            nonlocal sent
            sent += 1
            if sent == 4:
                arrived.set()
            await finish.wait()
            return httpx.Response(200, json={"choices": [{"message": {"content": "{}"}}]})

        async with httpx.AsyncClient(base_url="http://local", transport=httpx.MockTransport(respond)) as client:
            active = [asyncio.create_task(policy.call(client)) for _ in range(4)]
            try:
                await asyncio.wait_for(arrived.wait(), 2)
                rejected = await policy.call(client)
                assert not rejected.ok and rejected.reason == "client_busy"
                assert sent == 4 and policy.client_rejected == 1
                active[0].cancel()
                with pytest.raises(asyncio.CancelledError):
                    await active[0]
                assert policy.in_flight == 3
                finish.set()
                assert (await policy.call(client)).ok and sent == 5
                assert all(outcome.ok for outcome in await asyncio.gather(*active[1:]))
            finally:
                for task in active:
                    task.cancel()
                await asyncio.gather(*active, return_exceptions=True)
            assert policy.in_flight == 0 and policy.peak_in_flight == 4

    asyncio.run(run())


def test_breaker_blocks_then_closes_on_a_single_successful_probe():
    wire = 0

    def respond(_):
        nonlocal wire
        wire += 1
        if wire <= 2:
            return httpx.Response(500)
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps({})}}]})

    async def run():
        breaker = CircuitPolicy("test", threshold=2)
        async with httpx.AsyncClient(base_url="http://local", transport=httpx.MockTransport(respond)) as client:
            assert not (await breaker.call(client)).ok
            assert not (await breaker.call(client)).ok
            assert (await breaker.call(client)).reason == "circuit_open"
            assert wire == 2 and breaker.rejected == 1
            breaker.reopen_at = time.monotonic() - 1
            assert (await breaker.call(client)).ok
            assert breaker.reopen_at == 0 and wire == 3

    asyncio.run(run())


@pytest.fixture(scope="module")
def srv():
    with ServerProc() as server:
        yield server


@pytest.mark.parametrize("stacked, shared, wire, sdk_calls", [(False, False, 2, 1), (True, False, 8, 4), (True, True, 2, 2)])
def test_real_sdk_counts_match_the_fault_server(srv, stacked, shared, wire, sdk_calls):
    srv.config(rates={"http429": 1}, retry_after_s=.001)

    async def run():
        policy = SdkRetryStack(stacked=stacked, shared=shared)
        if shared:
            policy.budget.ratio, policy.budget.floor = 0, 1
        async with policy.connect(srv.url):
            outcome = await policy.call(None)
        assert not outcome.ok and outcome.attempts == wire
        assert outcome.reason == ("budget" if shared else "http429")
        assert policy.layers["transport_attempts"] == wire and policy.layers["sdk_calls"] == sdk_calls
        assert policy.denied == int(shared)

    asyncio.run(run())
    assert srv.stats()["received"] == wire


def test_recovery_has_paired_queue_cancellation_controls():
    settings = {case: options for case, _, _, options in cases(0)}
    assert settings["backoff"] == {}
    assert settings["bounded-queue"] == {"queue_limit": 4}
    assert settings["cancel"] == {"cancel_abandoned": True}
    assert settings["queue-cancel"] == settings["bounded-queue"] | settings["cancel"]


def test_retry_rng_is_repeatable_and_separate_from_arrival_rng():
    first, capped, *_ = cases(4)
    draw = first[2].rng.random()
    assert draw == capped[2].rng.random() == cases(4)[0][2].rng.random()
    assert draw != random.Random(4).random()
    assert draw != cases(5)[0][2].rng.random()


def test_configured_load_measures_completions_from_the_server_clock(monkeypatch):
    async def respond(request):
        assert request.url.path == "/_config"
        origin = time.monotonic()
        await asyncio.sleep(.06)
        return httpx.Response(200, json={"monotonic_start": origin})

    class Policy:
        async def call(self, client):
            return Outcome(True, 1, 0)

    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(respond)
    monkeypatch.setattr(harness.httpx, "AsyncClient", lambda **kw: real_client(transport=transport, **kw))
    server = type("Loopback", (), {"url": "http://127.0.0.1"})()
    [sample] = asyncio.run(harness.run_open(
        server, Policy(), 100, .025, 0, strict_window=True, record_completion=True, config={},
    ))
    arrival, _, completed = sample
    assert arrival < .025 < completed


async def until(client, field, value):
    for _ in range(100):
        if (await client.get("/_stats")).json()[field] >= value:
            return
        await asyncio.sleep(.01)
    pytest.fail(f"{field} did not reach {value}")


@pytest.mark.parametrize("queue_limit", [0, 1])
def test_admission_caps_waiting_work_and_returns_retry_after(srv, queue_limit):
    srv.config(service_s=.35, slots=1, queue_limit=queue_limit, retry_after_s=.2)

    async def run():
        async with httpx.AsyncClient(base_url=srv.url) as client:
            tasks = [asyncio.create_task(client.post(URL, json={})) for _ in range(queue_limit + 1)]
            await until(client, "in_system", queue_limit + 1)
            shed = await client.post(URL, json={})
            assert shed.status_code == 429 and shed.headers["Retry-After"] == "0.2"
            assert all(result.status_code == 200 for result in await asyncio.gather(*tasks))

    asyncio.run(run())
    assert srv.stats()["peak_queued"] == queue_limit and srv.stats()["shed"] == 1


def test_disconnect_cancels_work_and_releases_admission(srv):
    srv.config(service_s=.25, slots=1, queue_limit=0, cancel_abandoned=True)

    async def run():
        async with httpx.AsyncClient(base_url=srv.url) as client:
            with pytest.raises(httpx.ReadTimeout):
                await client.post(URL, json={}, timeout=.08)
            await until(client, "cancelled", 1)
            assert (await client.post(URL, json={})).status_code == 200

    asyncio.run(run())
    assert srv.stats()["cancelled"] == srv.stats()["served"] == 1


@pytest.mark.parametrize("cancel", [False, True])
def test_same_bounded_queue_handles_an_abandoned_waiter_differently(srv, cancel):
    srv.config(service_s=.30, slots=1, queue_limit=1, cancel_abandoned=cancel)

    async def run():
        async with httpx.AsyncClient(base_url=srv.url) as client:
            active = asyncio.create_task(client.post(URL, json={}))
            await until(client, "in_system", 1)
            with pytest.raises(httpx.ReadTimeout):
                await client.post(URL, json={}, timeout=.08)
            if cancel:
                await until(client, "cancelled", 1)
            assert (await active).status_code == 200
            await until(client, "served", 1 if cancel else 2)

    asyncio.run(run())
    stats = srv.stats()
    assert stats["peak_queued"] == 1 and stats["cancelled"] == int(cancel)
    assert stats["served"] == 2 - int(cancel)


def test_recovery_requires_three_populated_successful_buckets():
    outcome = Outcome(True, 1, .1)
    report = summarize([(t, outcome, t + .1) for t in (2.2, 4.2, 5.2, 6.2)],
                       {"received": 4, "peak_queued": 0, "shed": 0, "cancelled": 0}, 7, 2)
    assert report["on_time_by_arrival_second"][3] is None
    assert report["recovery_s"] == 5


def test_no_recovery_from_late_or_post_run_successes():
    outcome = Outcome(True, 1, 10)
    report = summarize([(t, outcome, t + 10) for t in (3.1, 4.1, 5.1)],
                       {"received": 3, "peak_queued": 0, "shed": 0, "cancelled": 0}, 6, 3)
    assert report["on_time"] == 0
    assert report["recovery_s"] is None


def test_window_recovery_uses_completions_before_each_cutoff_without_changing_deadline():
    outcome = Outcome(True, 1, .1)
    samples = [(2.1, outcome, 2.2), (3.1, outcome, 3.2), (4.8, outcome, 5.1),
               (5.0, outcome, 5.2), (7.0, outcome, 7.1)]
    assert recovery_windows(samples, 2, [3, 5], 1.0) == [
        {"post_outage_s": 3, "recovery_s": None},
        {"post_outage_s": 5, "recovery_s": 3.1},
    ]
    assert recovery_windows(samples, 2, [3, 5], .2) == [
        {"post_outage_s": 3, "recovery_s": None},
        {"post_outage_s": 5, "recovery_s": None},
    ]


@pytest.mark.parametrize("completed_by_cutoff", [8, 9])
def test_window_recovery_requires_nine_of_ten_completions_by_cutoff(completed_by_cutoff):
    outcome = Outcome(True, 1, .1)
    samples = [(2.1, outcome, 2.2), (3.1, outcome, 3.2)]
    samples += [(4.7, outcome, 4.9 if i < completed_by_cutoff else 5.1) for i in range(10)]
    assert recovery_windows(samples, 2, [3, 4], 1.0) == [
        {"post_outage_s": 3, "recovery_s": 3 if completed_by_cutoff == 9 else None},
        {"post_outage_s": 4, "recovery_s": 3 if completed_by_cutoff == 9 else 3.1},
    ]


@pytest.mark.parametrize("second_window_hits", [8, 9])
def test_sustained_recovery_checks_ninety_percent_of_offered_requests_in_each_window(second_window_hits):
    outcome = Outcome(True, 1, .1)
    samples = []
    for i in range(20):
        arrival = 5.25 + i / 2
        extra = 1.1 if i >= 10 + second_window_hits else .1
        samples.append((arrival, outcome, arrival + extra))
    existing = recovery.score_timing(samples, 16, 5, 1)
    assert existing["recovery_s"] == 3
    assert recovery.recovery_stability(samples, 16, 5, 1) == {
        "evaluable": True, "sustained_s": 11 if second_window_hits == 9 else None,
        "follow_up_evaluable": False, "relapse_s": None,
    }


def test_stability_distinguishes_empty_and_unfinished_windows_from_non_recovery():
    success = Outcome(True, 1, .1)
    samples = [(i + .25, success, i + .35) for i in range(5, 15)]
    assert not recovery.recovery_stability(samples, 15, 5, 1)["evaluable"]
    assert not recovery.recovery_stability(samples[:5], 20, 5, 1)["evaluable"]
    assert recovery.recovery_stability(samples, 16, 5, 1)["sustained_s"] == 11


def test_stability_only_flags_a_drop_after_the_follow_up_window_settles():
    success = Outcome(True, 1, .1)
    failure = Outcome(False, 1, .1)
    samples = [(i + .25, success if i != 17 else failure, i + .35) for i in range(5, 19)]
    samples.append((19.75, success, 20.35))
    early = recovery.recovery_stability(samples, 20, 5, 1)
    assert early["sustained_s"] == 11 and not early["follow_up_evaluable"] and early["relapse_s"] is None
    final = recovery.recovery_stability(samples, 21, 5, 1)
    assert final["sustained_s"] == 11 and final["follow_up_evaluable"] and final["relapse_s"] == 16
    samples[12] = (17.25, success, 17.35)
    assert recovery.recovery_stability(samples, 21, 5, 1)["relapse_s"] is None


def test_stability_cli_saves_separately_from_the_default_report(tmp_path, monkeypatch):
    def experiment(*args, **kwargs):
        assert kwargs["stability"] is True
        return {"version": 2}

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(recovery, "experiment", experiment)
    main(["--sustained-recovery", "--case", "client-cap"])
    assert json.loads((tmp_path / "results/raw/recovery-stability.json").read_text()) == {"version": 2}
    assert not (tmp_path / "results/raw/recovery.json").exists()


@pytest.mark.parametrize("window", ["2", "4"])
def test_cli_rejects_incomplete_or_out_of_run_observation_windows(window, capsys):
    with pytest.raises(SystemExit, match="2"):
        main(["--duration", "8", "--post-outage-windows", window])
    assert "Post-outage windows must be at least 3 s and fit inside the run" in capsys.readouterr().err


def test_confirmation_waits_for_the_last_on_time_completion_in_the_window():
    outcome = Outcome(True, 1, .5)
    report = summarize([(3.1, outcome, 3.6), (4.1, outcome, 4.6), (5.9, outcome, 6.4)],
                       {"received": 3, "peak_queued": 0, "shed": 0, "cancelled": 0}, 7, 3)
    assert report["on_time"] == 1
    assert report["recovery_s"] == 3.4
