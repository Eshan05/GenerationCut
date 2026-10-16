import json
from pathlib import Path

import pytest

from generation_cut import experiment


def test_default_run_writes_new_report_without_changing_baseline(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    baseline = tmp_path / "docs" / "experiment.json"
    baseline.parent.mkdir()
    baseline.write_bytes(b'{"saved": true}\n')
    monkeypatch.setattr(experiment, "BASELINE_REPORT", baseline)
    monkeypatch.setattr(experiment, "matrix", lambda args: [])
    monkeypatch.setattr(experiment, "storm", lambda args: [{"seed": 0}])

    experiment.main([])

    reproduction = tmp_path / "results" / "raw" / "baseline-reproduction.json"
    assert json.loads(reproduction.read_text(encoding="utf-8"))["storm"] == [{"seed": 0}]
    assert baseline.read_bytes() == b'{"saved": true}\n'


def test_explicit_baseline_path_is_rejected_before_running(monkeypatch, capsys):
    baseline = Path(__file__).resolve().parents[1] / "docs" / "experiment.json"
    saved = baseline.read_bytes()
    monkeypatch.setattr(experiment, "matrix", lambda args: pytest.fail("The benchmark started"))

    with pytest.raises(SystemExit, match="2"):
        experiment.main(["--out", str(baseline)])

    assert "saved baseline is read-only" in capsys.readouterr().err
    assert baseline.read_bytes() == saved


@pytest.mark.parametrize(("args", "message"), [
    (["--n", "0"], "--n and --seeds"),
    (["--seeds", "0"], "--n and --seeds"),
    (["--rate", "0"], "--rate and --duration"),
    (["--rate", "nan"], "--rate and --duration"),
    (["--duration", "inf"], "--rate and --duration"),
    (["--outage", "5", "5"], "--outage must be"),
    (["--outage", "-1", "2"], "--outage must be"),
    (["--duration", "8"], "--outage must be"),
])
def test_invalid_settings_are_rejected_before_running(args, message, tmp_path, monkeypatch, capsys):
    output = tmp_path / "not-created" / "report.json"
    monkeypatch.setattr(experiment, "matrix", lambda _: pytest.fail("The matrix started"))
    monkeypatch.setattr(experiment, "storm", lambda _: pytest.fail("The storm started"))

    with pytest.raises(SystemExit, match="2"):
        experiment.main([*args, "--out", str(output)])

    assert message in capsys.readouterr().err
    assert not output.parent.exists()
