import asyncio

import httpx

from generation_cut import single_fault, streaming
from generation_cut.policies import Outcome
from generation_cut.server import app


def test_matrix_pairs_policies_on_fresh_servers_and_alternates_order(monkeypatch):
    calls = []

    class Server:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def config(self, **settings):
            self.settings = settings

    async def run(srv, policy, requests, concurrency):
        calls.append((srv, policy.name))
        assert requests == 2 and concurrency == 8
        return [Outcome(True, 1, .1), Outcome(False, 4, .5)]

    monkeypatch.setattr(single_fault, "ServerProc", Server)
    monkeypatch.setattr(single_fault, "run_closed", run)
    monkeypatch.setattr(single_fault, "RATES_30", ["http429"])
    report = single_fault.experiment(seeds=2, requests=2)
    assert len({server for server, _ in calls}) == 4
    assert [name for _, name in calls] == ["immediate_x4", "backoff_x4", "backoff_x4", "immediate_x4"]
    assert [server.settings["seed"] for server, _ in calls] == [0, 0, 1, 1]
    assert all(server.settings["rates"] == {"http429": .30} for server, _ in calls)
    expected = {"success": .5, "attempts": 2.5, "p99_s": .5}
    assert report["runs"] == [
        {"fault": "http429", "seed": seed, "immediate": expected, "backoff": expected} for seed in (0, 1)
    ]


def test_single_fault_page_and_local_report(tmp_path, monkeypatch):
    demo = tmp_path / "demo"
    report = tmp_path / "results" / "raw" / "single-fault.json"
    report.parent.mkdir(parents=True)

    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local") as client:
            assert 'href="/demo/recovery/single-fault/"' in (await client.get("/demo/recovery/")).text
            assert "Paired runs" in (await client.get("/demo/recovery/single-fault/")).text
            monkeypatch.setattr(streaming, "DEMO", demo)
            path = "/demo/single-fault-results.json"
            assert (await client.get(path)).status_code == 404
            report.write_text('{"version": 1}', encoding="utf-8")
            response = await client.get(path)
            assert response.json() == {"version": 1}
            assert response.headers["Cache-Control"] == "no-store"

    asyncio.run(run())
