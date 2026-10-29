import pandas as pd

import fitness


def fit_run(root, cpu_per_krps, transport="h2"):
    """results/fitness-x with the analysis/reps.csv of one s1-baseline repetition."""
    run = root / "fitness-x"
    (run / "analysis").mkdir(parents=True)
    pd.DataFrame([{"run": run.name, "scenario": "s1-baseline", "transport": transport, "rep": 1,
                   "cpu_per_krps": cpu_per_krps}]).to_csv(run / "analysis" / "reps.csv", index=False)
    return run


def test_clean_environment_passes(tmp_path, capsys):
    assert fitness.main([str(fit_run(tmp_path, 134.0)), "--max", "200"]) == 0
    assert "ok" in capsys.readouterr().out


def test_degraded_environment_fails(tmp_path, capsys):
    # diag2 2026-10-12: the same 250 RPC/s cost ~2x the CPU under host noise
    assert fitness.main([str(fit_run(tmp_path, 270.0)), "--max", "200"]) == 1
    assert "degraded" in capsys.readouterr().out


def test_without_an_h2_rep_is_an_error(tmp_path, capsys):
    # no cpu_per_krps to judge (stats missing, rep invalid): never a silent pass
    assert fitness.main([str(fit_run(tmp_path, 134.0, transport="h3")), "--max", "200"]) == 2
    assert "no h2" in capsys.readouterr().err


def test_without_analysis_is_an_error(tmp_path, capsys):
    assert fitness.main([str(tmp_path), "--max", "200"]) == 2
    assert "no h2" in capsys.readouterr().err
