import json

from generation_cut import report


def test_report_uses_saved_settings_and_labels_the_right_seed(tmp_path, capsys):
    path = tmp_path / "trial.json"
    path.write_text(json.dumps({
        "config": {"n": 7, "rate": 12.5, "duration": 8, "outage": [1.25, 3.5]},
        "matrix": [{"policy": "no_retry", "fault": "hang", "success": 0.75, "attempts": 1, "p99": 0.4}],
        "storm": [
            {"policy": "immediate_x4", "seed": seed, "success_frac_per_s": [fraction],
             "overall_success": fraction, "amplification": 1, "peak_arrivals_per_s": 4, "recovery_s": None}
            for seed, fraction in [(1, 0.25), (0, 0.75)]
        ],
    }), encoding="utf-8")

    report.main(path)

    output = capsys.readouterr().out
    assert "SINGLE FAULT (7 requests each)" in output
    assert "RETRY STORM (offered 12.5 req/s for 8 s, outage 1.25-3.5 s)" in output
    assert "immediate_x4 seed 0: [0.75]" in output


def test_storm_only_report_without_seed_zero_omits_that_trace(tmp_path, capsys):
    path = tmp_path / "trial.json"
    path.write_text(json.dumps({
        "config": {"n": 7, "rate": 12.5, "duration": 8, "outage": [1.25, 3.5]},
        "storm": [{"policy": "immediate_x4", "seed": 1, "success_frac_per_s": [0.25],
                   "overall_success": 0.25, "amplification": 1, "peak_arrivals_per_s": 4, "recovery_s": None}],
    }), encoding="utf-8")

    report.main(path)

    output = capsys.readouterr().out
    assert "RETRY STORM" in output
    assert "SINGLE FAULT" not in output
    assert "immediate_x4 seed 0" not in output
