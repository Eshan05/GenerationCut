import asyncio

import httpx
import pytest

from generation_cut import load_sweep, streaming
from generation_cut.policies import Outcome
from generation_cut.server import app


def test_grid_distinguishes_matching_utilization_from_matching_offered_rate():
    cells = load_sweep.workloads([2, 1, 2], [.64, .32], .04, fixed_rate=16)
    assert [(cell["workers"], cell["rate"], cell["utilization"]) for cell in cells] == [
        (1, 8, .32), (1, 16, .64), (2, 16, .32), (2, 32, .64),
    ]
    extra = load_sweep.workloads([1, 2], [.64], .04, fixed_rate=12)
    assert [(c["workers"], c["utilization"]) for c in extra if c["rate"] == 12] == [(1, .48), (2, .24)]


@pytest.mark.parametrize("shape", [None, "early-heavy"])
def test_every_cell_pairs_fresh_policies_at_the_same_rate_and_seed(monkeypatch, shape):
    calls = []
    outcome_counts = {"attempts": [1, 0, 0], "assigned_failures": [1, 0, 0]}

    class Server:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def stats(self):
            return {"received": 1, "peak_queued": 0, "shed": 0, "cancelled": 0, "outage": outcome_counts}

    async def run_case(srv, policy, rate, duration, seed, config):
        expected = {"slots": int(rate / 16), "service_s": .04, "seed": seed, "outage_start": 1, "outage_end": 3}
        if shape:
            expected["outage_profile"] = [.9, .6, .3]
        assert duration == 7 and config == expected
        calls.append((srv, policy, rate, seed))
        return [(0.0, Outcome(True, 1, .1), .1)]

    monkeypatch.setattr(load_sweep, "ServerProc", Server)
    monkeypatch.setattr(load_sweep, "run_case", run_case)
    cells = load_sweep.workloads([1, 2], [.64], .04, outage_shapes=[shape] if shape else [])
    result = load_sweep.experiment(cells, seeds=2, seed_start=42, duration=7, outage=(1, 3),
                                   deadline=1.0, service_s=.04)
    assert len({srv for srv, *_ in calls}) == len(calls) == 8
    assert len({id(policy) for _, policy, *_ in calls}) == len(calls)
    assert [(rate, seed) for _, _, rate, seed in calls[::2]] == [(16, 42), (32, 42), (32, 43), (16, 43)]
    for first, second in zip(calls[::2], calls[1::2]):
        assert first[2:] == second[2:]
        assert {first[1].max_in_flight, second[1].max_in_flight} == {None, 4}
    for cell in result["cells"]:
        assert {(run["case"], run["seed"]) for run in cell["runs"]} == {
            (policy, seed) for seed in (42, 43) for policy in load_sweep.POLICIES
        }
        assert all(run["logical"] == 1 and run["amplification"] == 1 for run in cell["runs"])
        assert all(run.get("outage") == (outcome_counts if shape else None) for run in cell["runs"])
    assert result["config"].get("outage_shapes") == ({shape: (.9, .6, .3)} if shape else None)


def test_cli_rejects_rates_beyond_the_local_limit(capsys):
    with pytest.raises(SystemExit, match="2"):
        load_sweep.main(["--service-time", ".001"])
    assert "at most 100 requests/s" in capsys.readouterr().err


def test_load_sweep_page_and_report(tmp_path, monkeypatch):
    demo = tmp_path / "demo"
    report = tmp_path / "results" / "raw" / "load-sweep.json"
    report.parent.mkdir(parents=True)

    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local") as client:
            assert 'href="/demo/recovery/load/"' in (await client.get("/demo/recovery/")).text
            assert "Nominal utilization" in (await client.get("/demo/recovery/load/")).text
            monkeypatch.setattr(streaming, "DEMO", demo)
            path = "/demo/load-sweep-results.json"
            assert (await client.get(path)).status_code == 404
            report.write_text('{"version": 1}', encoding="utf-8")
            assert (await client.get(path)).json() == {"version": 1}
            assert (await client.get(path, params={"study": "shapes"})).status_code == 404
            report.with_name("load-shapes.json").write_text('{"shapes": true}', encoding="utf-8")
            assert (await client.get(path, params={"study": "shapes"})).json() == {"shapes": True}
            assert (await client.get(path, params={"study": "../load"})).status_code == 422

    asyncio.run(run())
