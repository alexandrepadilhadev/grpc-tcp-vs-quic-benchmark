import math

import numpy as np

import pandas as pd
import pytest

import analyze

S = 1_000_000_000  # ns per second
HEADER = "seq,worker,start_ns,latency_us,status,req_bytes,resp_bytes,phase\n"


def frame(rows):
    cols = ["seq", "worker", "start_ns", "latency_us", "status", "req_bytes", "resp_bytes", "phase"]
    return pd.DataFrame(rows, columns=cols)


def test_load_requests_keeps_measure_rows(tmp_path):
    p = tmp_path / "requests.csv"
    p.write_text(HEADER + "0,0,1,10,ok,1,1,warmup\n1,0,2,20,ok,1,1,measure\n2,1,3,30,unavailable,1,0,measure\n")
    df = analyze.load_requests(p)
    assert list(df.seq) == [1, 2]
    assert df.start_ns.dtype == "int64"


def test_latency_stats_ok_only():
    rows = [(i, 0, i, i, "ok", 1, 1, "measure") for i in range(1, 1001)]
    rows += [(1000 + i, 0, i, 5_000_000, "unavailable", 1, 0, "measure") for i in range(10)]
    s = analyze.latency_stats(frame(rows))
    assert s["p50"] == pytest.approx(500.5)
    assert s["p99"] == pytest.approx(990.01)
    assert s["p999"] == pytest.approx(999.001)
    assert s["mean"] == pytest.approx(500.5)


def test_throughput():
    rows = [(i, 0, i * 10_000_000, 10_000, "ok", 500, 500, "measure") for i in range(100)]
    t = analyze.throughput(frame(rows))
    assert t["ok_rps"] == pytest.approx(100.0)
    assert t["mb_s"] == pytest.approx(0.1)


def test_error_rate():
    rows = [(i, 0, i, 1, "ok" if i < 7 else "deadline_exceeded", 1, 1, "measure") for i in range(10)]
    assert analyze.error_rate(frame(rows)) == pytest.approx(0.3)


def test_throughput_series_fills_empty_bins():
    # ok completions at 0.5 s and 2.5 s, an error ending at 3.5 s
    rows = [
        (0, 0, 0, 500_000, "ok", 1, 1, "measure"),
        (1, 0, 2 * S, 500_000, "ok", 1, 1, "measure"),
        (2, 0, 3 * S, 500_000, "unavailable", 1, 0, "measure"),
    ]
    s = analyze.throughput_series(frame(rows))
    assert list(s.index) == [0, S, 2 * S, 3 * S]
    assert list(s) == [1, 0, 1, 0]


def outage_series(after):
    values = [100] * 30 + [0] * 5 + [50] + [after] * 10
    return pd.Series(values, index=[i * S for i in range(len(values))])


def test_recovery_time():
    assert analyze.recovery_time(outage_series(95), 30 * S, 35 * S) == pytest.approx(1.0)


def test_recovery_time_threshold_is_inclusive():
    assert analyze.recovery_time(outage_series(90), 30 * S, 35 * S) == pytest.approx(1.0)


def test_recovery_time_never():
    assert analyze.recovery_time(outage_series(89), 30 * S, 35 * S) is None


def test_parse_stats(tmp_path):
    p = tmp_path / "stats.csv"
    p.write_text(
        "ts_ns,name,cpu_perc,mem_usage\n"
        "1,bench-server-1,12.5%,100MiB / 7.6GiB\n"
        "1,bench-loadgen,200.1%,1.5GiB / 7.6GiB\n"
        "2,bench-server-1,0.00%,512KiB / 7.6GiB\n"
        "2,bench-loadgen,--,-- / --\n"
    )
    df = analyze.parse_stats(p)
    assert list(df.cpu_perc[:3]) == [12.5, 200.1, 0.0]
    assert list(df.mem_mib[:3]) == [100.0, 1536.0, 0.5]
    assert math.isnan(df.cpu_perc[3]) and math.isnan(df.mem_mib[3])


def test_summarize():
    reps = pd.DataFrame(
        {
            "scenario": ["s1"] * 6,
            "transport": ["h2"] * 3 + ["h3"] * 3,
            "rep": [1, 2, 3] * 2,
            "p50": [100.0, 110.0, 90.0, 50.0, 50.0, 50.0],
            "p99": [10.0, 12.0, 14.0, 7.0, 8.0, 9.0],
        }
    )
    s = analyze.summarize(reps).set_index("transport")
    assert s.loc["h2", "n"] == 3
    assert s.loc["h2", "p99_mean"] == pytest.approx(12.0)
    assert s.loc["h2", "p99_ci95"] == pytest.approx(4.968275, rel=1e-6)  # t(0.975, 2) * 2 / sqrt(3)
    assert s.loc["h2", "cv_p50"] == pytest.approx(0.1)
    assert s.loc["h3", "cv_p50"] == pytest.approx(0.0)
    assert list(s.mwu_p) == pytest.approx([0.1, 0.1])  # exact two-sided: 2 / C(6, 3)


def test_served_frac():
    meta = {"ok": 3000, "load": {"rps": 100, "duration_ns": 60 * S}}
    assert analyze.served_frac(meta) == pytest.approx(0.5)


def test_served_frac_closed_loop():
    assert math.isnan(analyze.served_frac({"ok": 3000, "load": {"rps": 0, "duration_ns": 60 * S}}))


def test_degraded_s():
    s = pd.Series([100, 100, 50, 100], index=[i * S for i in range(4)])  # edge bins are ignored
    assert analyze.degraded_s(s, 100) == 1
    assert analyze.degraded_s(s, 100, exclude=(2 * S, 3 * S)) == 0
    assert math.isnan(analyze.degraded_s(s, 0))  # closed loop: no target rate


def test_degraded_s_ignores_bins_after_last_send():
    # clean 100 rps for 5 s; the last RPC hits a 2 s deadline, so bins 5-6 hold only its tail
    rows = [(i, 0, i * 10_000_000, 1000, "ok", 1, 1, "measure") for i in range(499)]
    rows.append((499, 0, 4_990_000_000, 2_000_000, "deadline_exceeded", 1, 0, "measure"))
    df = frame(rows)
    assert analyze.degraded_s(analyze.throughput_series(df), 100, end_ns=df.start_ns.max()) == 0


def test_comparison():
    reps = pd.DataFrame(
        {
            "scenario": ["s2"] * 6,
            "transport": ["h2"] * 3 + ["h3"] * 3,
            "rep": [1, 2, 3] * 2,
            "p99": [10.0, 12.0, 14.0, 7.0, 8.0, 9.0],
            "recovery_s": [np.nan] * 6,
        }
    )
    c = analyze.comparison(reps)
    assert list(c.metric) == ["p99"]  # all-NaN metrics are dropped
    r = c.iloc[0]
    assert (r.h2_mean, r.h3_mean, r.n_h2, r.n_h3) == (12.0, 8.0, 3, 3)
    assert r.delta_pct == pytest.approx(-33.333, rel=1e-4)
    assert r.mwu_p == pytest.approx(0.1)


def write_rep(d, transport, outage=None, n=500, dials=1):
    """100 rps for n/100 s; requests started inside outage=(start_s, end_s) fail."""
    d.mkdir(parents=True)
    t0 = 1_700_000_000 * S
    lines = [HEADER]
    for i in range(n):
        start = t0 + i * 10_000_000
        down = outage and t0 + outage[0] * S <= start < t0 + outage[1] * S
        status, lat = ("unavailable", 1000) if down else ("ok", 1000 + i)
        lines.append(f"{i},0,{start},{lat},{status},100,100,measure\n")
    (d / "requests.csv").write_text("".join(lines))
    (d / "meta.json").write_text(
        f'{{"transport": "{transport}", "dials": {dials}, "interrupted": false,'
        f' "handshake_us": 4000, "ok": {n // 2}, "load": {{"rps": 100, "duration_ns": {n // 100 * S}}}}}'
    )
    (d / "stats.csv").write_text(  # the idle sample before the measured window is ignored
        "ts_ns,name,cpu_perc,mem_usage\n"
        f"{t0 - S},bench-server-1,0%,0MiB / 1GiB\n{t0 - S},bench-loadgen,0%,0MiB / 1GiB\n"
        f"{t0 + S},bench-server-1,10%,10MiB / 1GiB\n{t0 + S},bench-loadgen,30%,20MiB / 1GiB\n"
    )
    if outage:
        a, b = (int(t0 + x * S) for x in outage)
        (d / "events.csv").write_text(f"event,ts_ns\noutage_start,{a}\noutage_end,{b}\n")


def test_main(tmp_path):
    for t in ["h2", "h3"]:
        for r in [1, 2]:
            write_rep(tmp_path / "s1-baseline" / t / f"rep-{r}", t)
            write_rep(tmp_path / "s5-outage" / t / f"rep-{r}", t, outage=(1, 2))
    write_rep(tmp_path / "s1-baseline" / "h2" / "rep-3", "h2")
    (tmp_path / "s1-baseline" / "h2" / "rep-3" / "meta.json").unlink()  # no meta: skipped
    write_rep(tmp_path / "s1-baseline" / "h3" / "rep-3", "h3", dials=2)  # spec §9: invalid
    write_rep(tmp_path / "s1-baseline" / "h2" / "rep-4", "h2")
    (tmp_path / "s1-baseline" / "h2" / "rep-4" / "events.csv").write_text("event,ts_ns\n")
    (tmp_path / "failures.log").write_text("s1-baseline,h2,4\n")  # run.sh marked it failed

    assert analyze.main([str(tmp_path)]) == 0
    out = tmp_path / "analysis"
    s = pd.read_csv(out / "summary.csv").set_index(["scenario", "transport"])
    assert len(s) == 4
    assert s.loc[("s1-baseline", "h2"), "n"] == 2 and s.loc[("s1-baseline", "h3"), "n"] == 2
    assert s.loc[("s1-baseline", "h3"), "server_cpu_mean"] == pytest.approx(10.0)
    assert s.loc[("s1-baseline", "h3"), "loadgen_mem_mib_mean"] == pytest.approx(20.0)
    assert s.loc[("s5-outage", "h2"), "recovery_s_mean"] == pytest.approx(0.0)
    assert s.loc[("s5-outage", "h2"), "error_rate_mean"] == pytest.approx(0.2)
    assert s.loc[("s1-baseline", "h3"), "handshake_ms_mean"] == pytest.approx(4.0)
    krps = s.loc[("s1-baseline", "h3"), "ok_rps_mean"] / 1000  # ~0.1
    assert s.loc[("s1-baseline", "h3"), "cpu_per_krps_mean"] == pytest.approx(10 / krps)  # server at 10%
    assert s.loc[("s1-baseline", "h3"), "degraded_s_mean"] == 0
    assert s.loc[("s5-outage", "h2"), "degraded_s_mean"] == 0  # the outage window does not count
    assert s.loc[("s1-baseline", "h3"), "served_frac_mean"] == pytest.approx(0.5)  # meta ok = n / 2
    c = pd.read_csv(out / "comparison.csv")
    assert "served_frac" in set(c.metric[c.scenario == "s1-baseline"])
    assert set(c.scenario) == {"s1-baseline", "s5-outage"}
    assert "recovery_s" in set(c.metric[c.scenario == "s5-outage"])
    assert "recovery_s" not in set(c.metric[c.scenario == "s1-baseline"])
    for png in ["latency_cdf.png", "p99_boxplot.png", "s5_throughput.png"]:
        assert (out / png).stat().st_size > 0


def test_main_recovery_with_outage_off_bin_edge(tmp_path):
    # outage ends 3.5 s after the first request; throughput is back at once
    write_rep(tmp_path / "s5-outage" / "h3" / "rep-1", "h3", outage=(2, 3.5), n=700)
    assert analyze.main([str(tmp_path)]) == 0
    s = pd.read_csv(tmp_path / "analysis" / "summary.csv")
    assert s.recovery_s_mean[0] == pytest.approx(0.0)


def test_main_empty_run(tmp_path):
    assert analyze.main([str(tmp_path)]) == 1
