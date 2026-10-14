import json

import pandas as pd
import pytest

import calibrate

SERVICES = ["gateway", "core", "ledger", "loadgen", "risk", "rabbitmq", "postgres"]


def calib_run(root, rate, reps):
    """results/calib-<rate>-x with analysis/reps.csv and one meta.json per rep.
    reps: list of (transport, dict of overrides); defaults pass every rule."""
    run = root / f"calib-{rate}-x"
    rows = []
    for i, (t, over) in enumerate(reps, start=1):
        row = {"run": run.name, "scenario": "s1-baseline", "transport": t, "rep": i, "excluded": False,
               "p50": 1.0, "served_frac": 1.0, **{f"{c}_cpu": 10.0 for c in SERVICES}}
        late = over.pop("late_starts", 0)
        row.update(over)
        rows.append(row)
        d = run / "s1-baseline" / t / f"rep-{i}"
        d.mkdir(parents=True)
        (d / "meta.json").write_text(json.dumps({"late_starts": late, "load": {"rps": rate}}))
    (run / "analysis").mkdir(parents=True)
    pd.DataFrame(rows).to_csv(run / "analysis" / "reps.csv", index=False)
    return run


def test_table_aggregates_reps(tmp_path):
    run = calib_run(tmp_path, 500, [
        ("h3", {"served_frac": 1.0, "p50": 1.0, "gateway_cpu": 40, "risk_cpu": 5, "rabbitmq_cpu": 10, "postgres_cpu": 20}),
        ("h3", {"served_frac": 0.98, "p50": 3.0, "gateway_cpu": 60, "late_starts": 7}),
    ])
    row = calibrate.table([run]).set_index(["rate", "transport"]).loc[(500, "h3")]
    assert row["n"] == 2
    assert row["served_frac_min"] == pytest.approx(0.98)
    assert row["late_starts_max"] == 7
    assert row["p50_median"] == pytest.approx(2.0)
    assert row["gateway_cpu_max"] == pytest.approx(60)
    assert row["data_cpu_max"] == pytest.approx(35)  # risk + rabbitmq + postgres share CPU 5


def test_capacity_is_the_highest_h3_rate_serving_the_demand(tmp_path):
    # C is saturation, as in the POC: health (CPU, p50) does not decide it, the high rate's check does
    runs = [
        calib_run(tmp_path, 250, [("h3", {}), ("h2", {})]),
        calib_run(tmp_path, 750, [("h3", {"gateway_cpu": 85}), ("h2", {})]),  # unhealthy, still served
        calib_run(tmp_path, 1000, [("h3", {"served_frac": 0.995}), ("h2", {"served_frac": 0.5})]),  # only h3 decides
        calib_run(tmp_path, 1500, [("h3", {"served_frac": 0.94}), ("h2", {})]),  # saturated
    ]
    t = calibrate.table(runs).set_index(["rate", "transport"])
    assert calibrate.capacity(t.reset_index()) == 1000
    assert not t.loc[(750, "h3"), "healthy"] and t.loc[(250, "h3"), "healthy"]


@pytest.mark.parametrize("over", [
    {"served_frac": 0.989},
    {"p50": 1.51},  # > 1.5x the p50 of the lowest rate
    {"postgres_cpu": 30, "rabbitmq_cpu": 30, "risk_cpu": 11},  # CPU 5 summed: 71%
])
def test_each_rule_marks_a_rate_unhealthy(tmp_path, over):
    runs = [calib_run(tmp_path, 250, [("h3", {})]), calib_run(tmp_path, 500, [("h3", over)])]
    t = calibrate.table(runs).set_index(["rate", "transport"])
    assert t.loc[(250, "h3"), "healthy"] and not t.loc[(500, "h3"), "healthy"]


def test_high_rate():
    assert calibrate.high_rate(1000) == 400
    assert calibrate.high_rate(1130) == 450  # 0.4 x C, down to a multiple of 50


def test_main_without_a_served_rate(tmp_path, capsys):
    run = calib_run(tmp_path, 250, [("h3", {"served_frac": 0.5})])
    assert calibrate.main([str(run)]) == 1
    assert "no rate serves the demand" in capsys.readouterr().err


def test_main_prints_table_and_rates(tmp_path, capsys):
    runs = [calib_run(tmp_path, r, [("h3", {}), ("h2", {})]) for r in (250, 500)]
    assert calibrate.main([str(r) for r in runs]) == 0
    out = capsys.readouterr().out
    assert "| rate | transport |" in out and "C = 500" in out and "high rate = 200" in out


def test_late_starts_are_reported_not_judged(tmp_path):
    # at low rates the VM's timer wake-ups delay starts without any saturation (spec §9)
    runs = [calib_run(tmp_path, 250, [("h3", {})]), calib_run(tmp_path, 500, [("h3", {"late_starts": 900})])]
    t = calibrate.table(runs).set_index(["rate", "transport"])
    assert t.loc[(500, "h3"), "healthy"] and t.loc[(500, "h3"), "late_starts_max"] == 900


def test_table_counts_cpu_outliers(tmp_path):
    run = calib_run(tmp_path, 250, [("h3", {"cpu_outlier": True}), ("h3", {"cpu_outlier": False})])
    assert calibrate.table([run]).set_index(["rate", "transport"]).loc[(250, "h3"), "cpu_outliers"] == 1
