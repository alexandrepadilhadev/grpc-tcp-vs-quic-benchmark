import json
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
    assert s.loc["h2", "p99_median"] == 12.0
    assert (s.loc["h2", "p99_med_lo"], s.loc["h2", "p99_med_hi"]) == (10.0, 14.0)
    assert s.loc["h2", "p99_med_level"] == pytest.approx(0.75)  # n=3: 1 - 2 / 8


def test_served_frac():
    meta = {"ok": 3000, "load": {"rps": 100, "duration_ns": 60 * S}}
    assert analyze.served_frac(meta) == pytest.approx(0.5)


def test_served_frac_closed_loop():
    assert math.isnan(analyze.served_frac({"ok": 3000, "load": {"rps": 0, "duration_ns": 60 * S}}))


def test_median_ci():
    assert analyze.median_ci([1, 2, 3, 4, 100]) == pytest.approx((3, 1, 100, 0.9375))  # n=5: [min, max]


def test_median_ci_narrows_at_95():
    # n=30: x(10)..x(21) covers 1 - 2 P(Bin(30, .5) <= 9) = 0.9572, the narrowest >= 95%
    med, lo, hi, level = analyze.median_ci(range(1, 31))
    assert (med, lo, hi) == (15.5, 10, 21)
    assert level == pytest.approx(0.957226, rel=1e-5)


def test_median_ci_single_value():
    med, lo, hi, level = analyze.median_ci([5.0])
    assert med == 5.0
    assert math.isnan(lo) and math.isnan(hi) and math.isnan(level)


def test_hodges_lehmann():
    shift, lo, hi, level = analyze.hodges_lehmann([10, 12, 14], [7, 8, 9])
    assert shift == -4
    assert (lo, hi) == (-7, -1)  # 3 x 3: no interval reaches 95%, so the full range
    assert level == pytest.approx(0.9)  # 1 - 2 / C(6, 3)


def test_hodges_lehmann_5x5_level():
    shift, lo, hi, level = analyze.hodges_lehmann([0, 10, 20, 30, 40], [100, 110, 120, 130, 140])
    assert shift == 100
    assert (lo, hi) == (70, 130)  # 3rd and 23rd of the 25 sorted differences (60, 70, 70, 80, ...)
    assert level == pytest.approx(1 - 2 * 4 / 252)  # ~0.968


def test_hodges_lehmann_single_value():
    shift, lo, hi, level = analyze.hodges_lehmann([10], [7, 8, 9])
    assert shift == -2
    assert math.isnan(lo) and math.isnan(hi) and math.isnan(level)


def test_cliff_delta():
    assert analyze.cliff_delta([10, 12, 14], [7, 8, 9]) == (-1.0, "large")
    assert analyze.cliff_delta([1, 2, 3], [1, 2, 3]) == (0.0, "negligible")


@pytest.mark.parametrize("d, mag", [(0.146, "negligible"), (0.147, "small"), (-0.33, "medium"), (0.474, "large")])
def test_cliff_magnitude_thresholds(d, mag):
    assert analyze.cliff_magnitude(d) == mag


def test_holm():
    assert list(analyze.holm([0.01, 0.04, 0.03])) == pytest.approx([0.03, 0.06, 0.06])


def test_holm_keeps_nan_out_of_the_family():
    p = analyze.holm([0.01, np.nan, 0.04])
    assert p[0] == pytest.approx(0.02) and p[2] == pytest.approx(0.04)  # m = 2
    assert math.isnan(p[1])


def test_latency_all_counts_failures_at_deadline():
    rows = [(i, 0, i, 10_000, "ok", 1, 1, "measure") for i in range(98)]
    rows += [(98 + i, 0, 98 + i, 5_000, "unavailable", 1, 0, "measure") for i in range(2)]
    df = frame(rows)
    assert analyze.latency_stats(df)["p99"] == 10_000
    assert analyze.latency_all(df, unsent=0, deadline_us=2_000_000)["p99_all"] == 2_000_000


def test_latency_all_keeps_queued_failures_at_their_latency():
    # a failure that waited 5 s in the queue before its 2 s deadline counts at 5 s, not 2 s
    rows = [(i, 0, i, 10_000, "ok", 1, 1, "measure") for i in range(98)]
    rows += [(98 + i, 0, 98 + i, 5_000_000, "deadline_exceeded", 1, 0, "measure") for i in range(2)]
    assert analyze.latency_all(frame(rows), unsent=0, deadline_us=2_000_000)["p99_all"] == 5_000_000


def test_latency_all_counts_unsent_slots():
    df = frame([(i, 0, i, 10_000, "ok", 1, 1, "measure") for i in range(98)])
    assert analyze.latency_all(df, unsent=2, deadline_us=2_000_000)["p99_all"] == 2_000_000


def test_latency_all_without_failures_matches_ok_percentiles():
    df = frame([(i, 0, i, i, "ok", 1, 1, "measure") for i in range(1, 1001)])
    s, a = analyze.latency_stats(df), analyze.latency_all(df, unsent=0, deadline_us=2_000_000)
    assert (a["p50_all"], a["p99_all"], a["p999_all"]) == (s["p50"], s["p99"], s["p999"])


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
            "p50": [1.0, 2.0, 3.0, 2.0, 3.0, 4.0],
            "recovery_s": [np.nan] * 6,
        }
    )
    c = analyze.comparison(reps).set_index("metric")
    assert list(c.index) == ["p50", "p99"]  # all-NaN metrics are dropped
    assert c.mwu_p_holm.p99 == pytest.approx(0.2)  # Holm over the scenario's 2 tests: 0.1 x 2
    assert c.mwu_p_holm.p50 == pytest.approx(max(0.2, min(1, c.mwu_p.p50)))
    r = c.loc["p99"]
    assert (r.h2_mean, r.h3_mean, r.n_h2, r.n_h3) == (12.0, 8.0, 3, 3)
    assert r.delta_pct == pytest.approx(-33.333, rel=1e-4)
    assert r.mwu_p == pytest.approx(0.1)
    assert (r.h2_median, r.h3_median) == (12.0, 8.0)
    assert (r.hl_shift, r.hl_lo, r.hl_hi) == (-4.0, -7.0, -1.0)
    assert r.hl_level == pytest.approx(0.9)
    assert (r.cliff_delta, r.cliff_mag) == (-1.0, "large")


def throughput_reps(h2, h3):
    return pd.DataFrame(
        {"scenario": "s2", "transport": ["h2"] * len(h2) + ["h3"] * len(h3), "rep": [1, 2, 3] * 2, "ok_rps": h2 + h3}
    )


def test_comparison_ratio():
    c = analyze.comparison(throughput_reps([100.0, 100.0, 100.0], [200.0, 200.0, 200.0])).iloc[0]
    assert (c.ratio, c.ratio_lo, c.ratio_hi) == pytest.approx((2.0, 2.0, 2.0))


def test_comparison_ratio_spread():
    c = analyze.comparison(throughput_reps([100.0, 200.0, 400.0], [200.0, 400.0, 800.0])).iloc[0]
    assert c.ratio == pytest.approx(2.0)  # median of the 9 log-ratios
    assert (c.ratio_lo, c.ratio_hi) == pytest.approx((0.5, 8.0))  # 3 x 3: full range


def test_comparison_ratio_needs_positive_values(capsys):
    c = analyze.comparison(throughput_reps([0.0, 100.0, 100.0], [200.0, 200.0, 200.0])).iloc[0]
    assert math.isnan(c.ratio) and math.isnan(c.ratio_lo) and math.isnan(c.ratio_hi)
    assert "ok_rps" in capsys.readouterr().err


def test_comparison_ratio_only_for_throughput():
    reps = throughput_reps([100.0, 100.0, 100.0], [200.0, 200.0, 200.0]).rename(columns={"ok_rps": "p99"})
    assert math.isnan(analyze.comparison(reps).iloc[0].ratio)


def write_rep(d, transport, outage=None, n=500, dials=1, rps=100, hops=None, names=("server",), cpu=10,
              migration=None, peers=None):
    """100 rps for n/100 s; requests started inside outage=(start_s, end_s) fail.
    hops: {"svc-<name>.json": {peer: dials}}; names: service containers in stats.csv, each at 10%.
    migration: {"at_s", "ok"} -> meta migrations/migrate_to and the s4 events.csv; peers: gateway peers."""
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
        f' "handshake_us": 4000, "ok": {n // 2}, "unsent": 0,'
        + (f' "migrations": 1, "migrate_to": "10.10.0.250", "migrate_at_ns": {migration["at_s"] * S},'
           if migration else "") +
        f' "load": {{"rps": {rps}, "duration_ns": {n // 100 * S}, "deadline_ns": {2 * S}}}}}'
    )
    stats = ["ts_ns,name,cpu_perc,mem_usage\n"]  # the idle sample before the measured window is ignored
    for ts, pct, mem in [(t0 - S, 0, 0), (t0 + S, cpu, 10)]:
        stats += [f"{ts},bench-{c}-1,{pct}%,{mem}MiB / 1GiB\n" for c in names]
        stats.append(f"{ts},bench-loadgen,{3 * pct}%,{2 * mem}MiB / 1GiB\n")
    (d / "stats.csv").write_text("".join(stats))
    for name, dials_by_peer in (hops or {}).items():
        rep = {"service": name, "dials": dials_by_peer, "counters": {}}
        if name == "svc-gateway.json" and peers is not None:
            rep["peers"] = peers
        (d / name).write_text(json.dumps(rep))
    if migration:
        m = int(t0 + migration["at_s"] * S)
        end = "migrated" if migration["ok"] else "migrate_failed"
        (d / "events.csv").write_text(
            f"event,ts_ns\nip_added,{m - 1}\nmigrate_start,{m}\n{end},{m + 1_000_000}\nip_removed,{m + 2_000_000}\n")
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


def noisy_run(root):
    """s1-baseline h2 with 3 reps; rep-2 has 2 s of failures (degraded_s = 2) but no outage event."""
    for r in [1, 2, 3]:
        d = root / "s1-baseline" / "h2" / f"rep-{r}"
        write_rep(d, "h2", outage=(2, 4) if r == 2 else None)
        (d / "events.csv").unlink(missing_ok=True)


def test_main_exclude_degraded(tmp_path, capsys):
    noisy_run(tmp_path)
    assert analyze.main([str(tmp_path), "--exclude-degraded=s1-baseline,s5-outage"]) == 0
    reps = pd.read_csv(tmp_path / "analysis" / "reps.csv").set_index("rep")
    assert reps.degraded_s[2] == 2 and list(reps.excluded) == [False, True, False]
    s = pd.read_csv(tmp_path / "analysis" / "summary.csv").iloc[0]
    assert (s.n, s.n_excluded) == (2, 1)
    assert s.error_rate_mean == 0  # the excluded rep's errors are out of the summary
    assert "s1-baseline/h2: 1 rep(s) excluded" in capsys.readouterr().err


def test_main_exclude_degraded_other_scenario(tmp_path):
    noisy_run(tmp_path)
    assert analyze.main([str(tmp_path), "--exclude-degraded=s5-outage"]) == 0
    s = pd.read_csv(tmp_path / "analysis" / "summary.csv").iloc[0]
    assert (s.n, s.n_excluded) == (3, 0)


def test_main_multiple_runs(tmp_path):
    for run in ["run-a", "run-b"]:
        for r in [1, 2]:
            write_rep(tmp_path / run / "s1-baseline" / "h2" / f"rep-{r}", "h2")
    out = tmp_path / "merged"
    assert analyze.main([str(tmp_path / "run-a"), str(tmp_path / "run-b"), "--out", str(out)]) == 0
    assert pd.read_csv(out / "summary.csv").n[0] == 4
    reps = pd.read_csv(out / "reps.csv")
    assert sorted(zip(reps.run, reps.rep)) == [("run-a", 1), ("run-a", 2), ("run-b", 1), ("run-b", 2)]
    assert not (tmp_path / "run-a" / "analysis").exists()


def test_main_single_run_records_run(tmp_path):
    write_rep(tmp_path / "run-a" / "s1-baseline" / "h2" / "rep-1", "h2")
    assert analyze.main([str(tmp_path / "run-a")]) == 0
    assert list(pd.read_csv(tmp_path / "run-a" / "analysis" / "reps.csv").run) == ["run-a"]


def test_main_multiple_runs_need_out(tmp_path, capsys):
    assert analyze.main([str(tmp_path / "a"), str(tmp_path / "b")]) == 2
    assert "usage" in capsys.readouterr().err


def test_main_warns_on_different_load(tmp_path, capsys):
    write_rep(tmp_path / "run-a" / "s1-baseline" / "h2" / "rep-1", "h2")
    write_rep(tmp_path / "run-b" / "s1-baseline" / "h2" / "rep-1", "h2", rps=200)
    assert analyze.main([str(tmp_path / "run-a"), str(tmp_path / "run-b"), "--out", str(tmp_path / "m")]) == 0
    assert "s1-baseline: load differs between runs" in capsys.readouterr().err


def test_main_p_all(tmp_path):
    write_rep(tmp_path / "s5-outage" / "h2" / "rep-1", "h2", outage=(1, 2))  # 100 of 500 fail
    assert analyze.main([str(tmp_path)]) == 0
    s = pd.read_csv(tmp_path / "analysis" / "summary.csv").iloc[0]
    assert s.p99_all_mean == 2 * S / 1000  # the failures sit at the 2 s deadline
    assert s.p99_mean < 2000


def test_main_needs_deadline(tmp_path, capsys):
    d = tmp_path / "s1-baseline" / "h2" / "rep-1"
    write_rep(d, "h2")
    (d / "meta.json").write_text((d / "meta.json").read_text().replace(', "deadline_ns": 2000000000', ""))
    assert analyze.main([str(tmp_path)]) == 1
    assert "deadline_ns" in capsys.readouterr().err


def test_main_usage():
    assert analyze.main([]) == 2


def test_main_recovery_with_outage_off_bin_edge(tmp_path):
    # outage ends 3.5 s after the first request; throughput is back at once
    write_rep(tmp_path / "s5-outage" / "h3" / "rep-1", "h3", outage=(2, 3.5), n=700)
    assert analyze.main([str(tmp_path)]) == 0
    s = pd.read_csv(tmp_path / "analysis" / "summary.csv")
    assert s.recovery_s_mean[0] == pytest.approx(0.0)


def test_main_empty_run(tmp_path):
    assert analyze.main([str(tmp_path)]) == 1


HOPS = {  # a clean payment repetition: every service wrote its report
    "svc-gateway.json": {"core:8443": 1},
    "svc-core.json": {"ledger-rpc:8443": 1},
    "svc-ledger.json": {},
    "svc-risk.json": {},
}


def hop_run(tmp_path):
    """s1-baseline, 2 payment reps per transport; returns h3 rep-2 to spoil."""
    for t in ["h2", "h3"]:
        for r in [1, 2]:
            write_rep(tmp_path / "s1-baseline" / t / f"rep-{r}", t, hops=HOPS)
    return tmp_path / "s1-baseline" / "h3" / "rep-2"


def assert_h3_rep_skipped(tmp_path, capsys, reason):
    assert analyze.main([str(tmp_path)]) == 0
    s = pd.read_csv(tmp_path / "analysis" / "summary.csv").set_index(["scenario", "transport"])
    assert s.loc[("s1-baseline", "h3"), "n"] == 1 and s.loc[("s1-baseline", "h2"), "n"] == 2
    assert f"{reason}, skipped" in capsys.readouterr().err


def test_main_skips_bad_hop_dials(tmp_path, capsys):
    (hop_run(tmp_path) / "svc-gateway.json").write_text('{"service": "gateway", "dials": {"core:8443": 2}, "counters": {}}')
    assert_h3_rep_skipped(tmp_path, capsys, "svc-gateway.json core:8443 dials=2")


def test_main_skips_missing_hop_file(tmp_path, capsys):
    # core killed before its report: the core->ledger hop cannot be checked
    (hop_run(tmp_path) / "svc-core.json").unlink()
    assert_h3_rep_skipped(tmp_path, capsys, "svc-core.json missing")


def test_main_skips_unreadable_hop_file(tmp_path, capsys):
    (hop_run(tmp_path) / "svc-ledger.json").write_text('{"service": "led')  # write cut by SIGKILL
    assert_h3_rep_skipped(tmp_path, capsys, "svc-ledger.json unreadable")


def test_main_without_hop_files_uses_meta_only(tmp_path):
    for t in ["h2", "h3"]:
        write_rep(tmp_path / "s1-baseline" / t / "rep-1", t)
    assert analyze.main([str(tmp_path)]) == 0
    s = pd.read_csv(tmp_path / "analysis" / "summary.csv").set_index(["scenario", "transport"])
    assert s.loc[("s1-baseline", "h2"), "n"] == 1 and s.loc[("s1-baseline", "h3"), "n"] == 1


PEERS = {"10.10.0.5": 4512, "10.10.0.250": 2990}


def s4_run(tmp_path, transport, ok, dials, peers=PEERS):
    write_rep(tmp_path / "s4-migration" / transport / "rep-1", transport, dials=dials, hops=HOPS,
              migration={"at_s": 2, "ok": ok}, peers=peers)
    assert analyze.main([str(tmp_path)]) in (0, 1)


def s4_valid(tmp_path):
    return (tmp_path / "analysis" / "reps.csv").exists()


def test_s4_h2_two_dials_is_valid(tmp_path):
    s4_run(tmp_path, "h2", ok=True, dials=2)
    assert s4_valid(tmp_path)


def test_s4_h3_migrated_two_dials_is_skipped(tmp_path, capsys):
    s4_run(tmp_path, "h3", ok=True, dials=2)
    assert not s4_valid(tmp_path)
    assert "dials=2, expected 1, skipped" in capsys.readouterr().err


def test_s4_h3_failed_migration_two_dials_is_valid(tmp_path):
    s4_run(tmp_path, "h3", ok=False, dials=2)
    assert s4_valid(tmp_path)


def test_s4_h3_failed_migration_one_dial_is_valid_not_migrated(tmp_path):
    s4_run(tmp_path, "h3", ok=False, dials=1)  # passive server migration kept the connection
    assert s4_valid(tmp_path)
    assert pd.read_csv(tmp_path / "analysis" / "reps.csv").migrated[0] == 0


def test_s4_h3_failed_migration_three_dials_is_skipped(tmp_path, capsys):
    s4_run(tmp_path, "h3", ok=False, dials=3)
    assert not s4_valid(tmp_path)
    assert "dials=3, expected 1 or 2, skipped" in capsys.readouterr().err


def test_main_warns_on_different_migration(tmp_path, capsys):
    for run, at_s in [("run-a", 2), ("run-b", 3)]:
        write_rep(tmp_path / run / "s4-migration" / "h2" / "rep-1", "h2", dials=2, hops=HOPS,
                  migration={"at_s": at_s, "ok": True}, peers=PEERS)
    assert analyze.main([str(tmp_path / "run-a"), str(tmp_path / "run-b"), "--out", str(tmp_path / "m")]) == 0
    assert "s4-migration: load differs between runs" in capsys.readouterr().err


def test_s4_without_new_ip_in_peers_is_skipped(tmp_path, capsys):
    s4_run(tmp_path, "h3", ok=True, dials=1, peers={"10.10.0.5": 4512})
    assert not s4_valid(tmp_path)
    assert "migration not seen by the gateway, skipped" in capsys.readouterr().err


@pytest.mark.parametrize("transport", ["h2", "h3"])
def test_s4_without_migrate_start_is_skipped(tmp_path, capsys, transport):
    d = tmp_path / "s4-migration" / transport / "rep-1"
    write_rep(d, transport, dials=1, hops=HOPS, migration={"at_s": 2, "ok": True}, peers=PEERS)
    (d / "events.csv").unlink()
    assert analyze.main([str(tmp_path)]) == 1
    assert "migrations=1 but no migrate_start in events.csv, skipped" in capsys.readouterr().err


@pytest.mark.parametrize("content", ["", "foo,bar\n1,2\n", "event,x\na,1\n"])
def test_bad_events_csv_skips_only_that_rep(tmp_path, capsys, content):
    for r in [1, 2]:
        write_rep(tmp_path / "s1-baseline" / "h2" / f"rep-{r}", "h2")
    (tmp_path / "s1-baseline" / "h2" / "rep-2" / "events.csv").write_text(content)
    assert analyze.main([str(tmp_path)]) == 0
    assert pd.read_csv(tmp_path / "analysis" / "summary.csv").n[0] == 1
    assert "events.csv unreadable, skipped" in capsys.readouterr().err


def test_main_outage_events_unchanged(tmp_path):
    write_rep(tmp_path / "s5-outage" / "h2" / "rep-1", "h2", outage=(1, 2))
    assert analyze.main([str(tmp_path)]) == 0
    assert pd.read_csv(tmp_path / "analysis" / "summary.csv").recovery_s_mean[0] == pytest.approx(0.0)


def test_rpc_cpu(tmp_path):
    for t in ["h2", "h3"]:
        write_rep(tmp_path / "s1-baseline" / t / "rep-1", t, names=("gateway", "core", "ledger", "postgres"))
    assert analyze.main([str(tmp_path)]) == 0
    s = pd.read_csv(tmp_path / "analysis" / "summary.csv").set_index(["scenario", "transport"])
    row = s.loc[("s1-baseline", "h3")]
    assert row["rpc_cpu_mean"] == pytest.approx(30.0)  # gateway + core + ledger, not postgres
    assert row["cpu_per_krps_mean"] == pytest.approx(30.0 / (row["ok_rps_mean"] / 1000))


def outlier_reps(cpus):
    return pd.DataFrame({"scenario": "s1-baseline", "transport": "h2", "run": "r", "rep": range(1, len(cpus) + 1),
                         "cpu_per_krps": cpus})


def test_cpu_outlier_flags_the_noisy_rep():
    cpus = [39, 40, 41, 40, 39, 41, 40, 40, 39, 41, 60]
    assert list(analyze.cpu_outlier(outlier_reps(cpus))) == [False] * 10 + [True]


def test_cpu_outlier_without_spread():
    assert not analyze.cpu_outlier(outlier_reps([40.0] * 5)).any()  # MAD = 0: nothing flagged, no division


def test_cpu_outlier_per_run_and_transport():
    a = outlier_reps([40, 40, 41, 39, 40])
    b = outlier_reps([80, 81, 79, 80, 80]).assign(transport="h3")  # h3 costs more: not an outlier of h2
    assert not analyze.cpu_outlier(pd.concat([a, b], ignore_index=True)).any()


def test_main_exclude_cpu_outlier(tmp_path, capsys):
    for t in ["h2", "h3"]:
        for r in range(1, 6):
            write_rep(tmp_path / "s1-baseline" / t / f"rep-{r}", t, cpu=20 + r % 2)
    write_rep(tmp_path / "s1-baseline" / "h2" / "rep-6", "h2", cpu=50)  # host noise: same load, 2.4x the CPU
    assert analyze.main([str(tmp_path), "--exclude-degraded=s1-baseline"]) == 0
    err = capsys.readouterr().err
    assert "s1-baseline/h2/rep-6: cpu_outlier" in err
    reps = pd.read_csv(tmp_path / "analysis" / "reps.csv")
    assert reps.cpu_outlier.sum() == 1 and reps.excluded.sum() == 1
    s = pd.read_csv(tmp_path / "analysis" / "summary.csv").set_index(["scenario", "transport"])
    assert s.loc[("s1-baseline", "h2"), "n"] == 5 and s.loc[("s1-baseline", "h2"), "n_excluded"] == 1


def test_cpu_outlier_ignores_missing_cpu():
    # reps without stats.csv have no cpu_per_krps: never flagged
    assert not analyze.cpu_outlier(outlier_reps([float("nan")] * 3)).any()
    assert not analyze.cpu_outlier(outlier_reps([40.0, 40.0, float("nan")])).any()


def test_cpu_outlier_needs_a_real_jump():
    # a very stable group has a tiny MAD: +6% is above median + 3 MAD but is not host noise (2x seen)
    assert not analyze.cpu_outlier(outlier_reps([312.4, 314.5, 330.1])).any()
    assert list(analyze.cpu_outlier(outlier_reps([133.4, 134.6, 166.8]))) == [False, False, True]  # +24%


def orphan_rep(tmp_path, transport, settled):
    """s5-like rep: 500 RPCs, 100 failed in the outage; the ledger committed `settled`."""
    d = tmp_path / "s5-outage" / transport / "rep-1"
    write_rep(d, transport, outage=(1, 2), hops=HOPS)
    (d / "svc-ledger.json").write_text(json.dumps({"service": "ledger", "dials": {}, "counters": {"settled": settled}}))
    return d


def test_orphans_are_settled_payments_the_client_saw_fail(tmp_path):
    # 400 ok + the probe were answered; 30 more were settled while the client saw an error
    for t, settled in [("h2", 431), ("h3", 401)]:
        orphan_rep(tmp_path, t, settled)
    assert analyze.main([str(tmp_path)]) == 0
    r = pd.read_csv(tmp_path / "analysis" / "reps.csv").set_index("transport")
    assert r.loc["h2", "orphans"] == 30 and r.loc["h2", "orphan_share"] == pytest.approx(0.3)
    assert r.loc["h3", "orphans"] == 0 and r.loc["h3", "orphan_share"] == 0
    c = pd.read_csv(tmp_path / "analysis" / "comparison.csv")
    assert "orphans" in set(c.metric)


def test_orphans_need_the_ledger_counter(tmp_path):
    # runs before the counter existed: no orphans column value, no crash
    for t in ["h2", "h3"]:
        write_rep(tmp_path / "s1-baseline" / t / "rep-1", t, hops=HOPS)
    assert analyze.main([str(tmp_path)]) == 0
    r = pd.read_csv(tmp_path / "analysis" / "reps.csv")
    assert "orphans" not in r or r.orphans.isna().all()


def completions(ms_list, status="ok"):
    """measure rows completing at the given Unix ns (latency 0)."""
    return frame([(i, 0, t, 0, status, 1, 1, "measure") for i, t in enumerate(ms_list)])


def test_max_gap_ms():
    t0 = 1_700_000_000 * S
    ms = 1_000_000
    ok = [t0 + i * 10 * ms for i in range(0, 50)] + [t0 + 690 * ms + i * 10 * ms for i in range(0, 30)]  # hole 490..690
    df = pd.concat([completions(ok), completions([t0 + 300 * ms], "unavailable")], ignore_index=True)
    assert analyze.max_gap_ms(df, t0, t0 + 980 * ms) == pytest.approx(200.0)
    assert analyze.max_gap_ms(df, t0 + 100 * ms, t0 + 300 * ms) == pytest.approx(10.0)  # not-ok row ignored
    assert analyze.max_gap_ms(df, t0 + 10 * 1000 * ms, t0 + 10 * 1000 * ms + 500 * ms) == pytest.approx(500.0)  # no ok inside
    far = completions([t0 + 5000 * ms])  # outside the window: ignored; edges count as completions
    assert analyze.max_gap_ms(far, t0, t0 + 100 * ms) == pytest.approx(100.0)


def migration_df(t_m):
    ms = 1_000_000
    ok = [t_m + i * 10 * ms for i in range(-1500, 2500) if not 1 <= i <= 4]  # hole: t_m .. t_m + 50 ms
    bad = completions([t_m + 1 * S, t_m + 2 * S], "unavailable")
    early = completions([t_m - 3 * S, t_m + 6 * S], "unavailable")  # outside [t_m - 1 s, t_m + 5 s]
    return pd.concat([completions(ok), bad, early], ignore_index=True)


def test_migration_metrics():
    t_m = 1_700_000_030 * S
    df = migration_df(t_m)
    ev = pd.Series({"migrate_start": t_m, "migrated": t_m + 1_000_000})
    m = analyze.migration_metrics(df, ev, {"transport": "h3", "dials": 1})
    assert m == {"interrupt_ms": pytest.approx(50.0), "gap_base_ms": pytest.approx(10.0),
                 "migration_errors": 2, "migrate_ms": pytest.approx(1.0), "migrated": 1}
    h2 = analyze.migration_metrics(df, ev, {"transport": "h2", "dials": 2})
    assert h2["migrated"] == 0
    failed = analyze.migration_metrics(df, pd.Series({"migrate_start": t_m, "migrate_failed": t_m + 2_000_000}),
                                       {"transport": "h3", "dials": 2})
    assert failed["migrated"] == 0 and failed["migrate_ms"] == pytest.approx(2.0)


def test_main_s4_columns(tmp_path):
    for t in ["h2", "h3"]:
        write_rep(tmp_path / "s4-migration" / t / "rep-1", t, n=2500, dials=2 if t == "h2" else 1, hops=HOPS,
                  migration={"at_s": 13, "ok": t == "h3"}, peers=PEERS)  # windows inside the 25 s of load
        write_rep(tmp_path / "s1-baseline" / t / "rep-1", t)
    assert analyze.main([str(tmp_path)]) == 0
    reps = pd.read_csv(tmp_path / "analysis" / "reps.csv").set_index(["scenario", "transport"])
    cols = ["interrupt_ms", "gap_base_ms", "migration_errors", "migrate_ms", "migrated"]
    assert reps.loc[("s4-migration", "h3"), cols].notna().all()
    assert reps.loc[("s4-migration", "h3"), "migrated"] == 1 and reps.loc[("s4-migration", "h2"), "migrated"] == 0
    assert reps.loc[("s1-baseline", "h3"), cols].isna().all()
    c = pd.read_csv(tmp_path / "analysis" / "comparison.csv")
    assert {"interrupt_ms", "gap_base_ms", "migration_errors", "migrate_ms"} <= set(c.metric[c.scenario == "s4-migration"])


def test_s4_degraded_excludes_migration_window(tmp_path):
    d = tmp_path / "s4-migration" / "h3" / "rep-1"
    write_rep(d, "h3", n=1000, hops=HOPS, migration={"at_s": 3, "ok": True}, peers=PEERS)
    t0 = 1_700_000_000 * S
    lines = [HEADER]
    for i in range(1000):
        start = t0 + i * 10_000_000
        if t0 + 3 * S + 100_000_000 <= start < t0 + 5 * S + 100_000_000:
            continue  # 2 s hole right after t_m
        lines.append(f"{i},0,{start},1000,ok,100,100,measure\n")
    (d / "requests.csv").write_text("".join(lines))
    assert analyze.main([str(tmp_path)]) == 0
    assert pd.read_csv(tmp_path / "analysis" / "reps.csv").degraded_s[0] == 0


def test_s4_migrate_start_without_end_event_skips_only_that_rep(tmp_path, capsys):
    for r in [1, 2]:
        write_rep(tmp_path / "s4-migration" / "h2" / f"rep-{r}", "h2", dials=2, hops=HOPS,
                  migration={"at_s": 2, "ok": True}, peers=PEERS)
    ev = tmp_path / "s4-migration" / "h2" / "rep-2" / "events.csv"
    ev.write_text("\n".join(ev.read_text().splitlines()[:3]) + "\n")  # header, ip_added, migrate_start
    assert analyze.main([str(tmp_path)]) == 0
    assert pd.read_csv(tmp_path / "analysis" / "summary.csv").n[0] == 1
    assert "migrate_start without migrated/migrate_failed in events.csv, skipped" in capsys.readouterr().err


def test_migration_metrics_window_before_measure_is_nan(capsys):
    t_m = 1_700_000_030 * S
    df = migration_df(t_m)
    df = df[df.start_ns >= t_m - 8 * S]  # measure starts 8 s before t_m: base window [-11, -5] sticks out
    ev = pd.Series({"migrate_start": t_m, "migrated": t_m + 1_000_000})
    m = analyze.migration_metrics(df, ev, {"transport": "h3", "dials": 1}, "s4/h3/rep-1")
    assert math.isnan(m["gap_base_ms"]) and m["interrupt_ms"] == pytest.approx(50.0) and m["migration_errors"] == 2
    assert capsys.readouterr().err.count("s4/h3/rep-1: gap_base_ms window outside the measure phase, left empty") == 1


def test_migration_metrics_window_after_measure_is_nan(capsys):
    t_m = 1_700_000_030 * S
    df = migration_df(t_m)
    df = df[df.start_ns <= t_m + 3 * S]  # load ends 3 s after t_m
    ev = pd.Series({"migrate_start": t_m, "migrated": t_m + 1_000_000})
    m = analyze.migration_metrics(df, ev, {"transport": "h3", "dials": 1}, "r")
    assert math.isnan(m["interrupt_ms"]) and math.isnan(m["migration_errors"]) and m["gap_base_ms"] == pytest.approx(10.0)
    assert m["migrated"] == 1 and m["migrate_ms"] == pytest.approx(1.0)
