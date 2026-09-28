"""Analysis of results/<run_id>/ (spec §6.5)."""

import json
import math
import re
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter  # noqa: E402
from scipy import stats  # noqa: E402

NS = 1_000_000_000
KEYS = ["scenario", "transport"]
QUANTILES = np.linspace(0, 100, 1001)
COLORS = {"h2": "#2a78d6", "h3": "#eb6834"}  # dataviz categorical slots 1-2, validated (light)
COMPARE = ["p50", "p99", "p999", "error_rate", "recovery_s", "cpu_per_krps", "handshake_ms"]
INK, MUTED, GRID = "#1a1a19", "#6b6a63", "#e4e3dc"
MIB = {"B": 1 / 2**20, "KiB": 1 / 2**10, "MiB": 1.0, "GiB": 2**10, "TiB": 2**20}


def load_requests(path) -> pd.DataFrame:
    """requests.csv, measure rows only."""
    df = pd.read_csv(path, dtype={"start_ns": "int64", "latency_us": "int64", "status": "string"})
    return df[df.phase == "measure"].reset_index(drop=True)


def latency_stats(df) -> dict:
    """Latency percentiles, mean and std in us over ok rows."""
    lat = df.latency_us[df.status == "ok"].to_numpy(dtype=float)
    p50, p90, p99, p999 = np.percentile(lat, [50, 90, 99, 99.9])
    return {"p50": p50, "p90": p90, "p99": p99, "p999": p999, "mean": lat.mean(), "std": lat.std(ddof=1)}


def _window_s(df) -> float:
    end = df.start_ns + df.latency_us * 1000
    return (end.max() - df.start_ns.min()) / NS


def throughput(df) -> dict:
    """ok RPCs/s and payload MB/s (request + response bytes) over the measured window."""
    ok = df[df.status == "ok"]
    w = _window_s(df)
    return {"ok_rps": len(ok) / w, "mb_s": (ok.req_bytes + ok.resp_bytes).sum() / w / 1e6}


def error_rate(df) -> float:
    return float((df.status != "ok").mean())


def throughput_series(df, bin_s=1.0, origin=None) -> pd.Series:
    """ok completions per bin, empty bins included; index = bin start (Unix ns).
    With origin, bin edges fall on it and the first bin is the first full one."""
    width = int(bin_s * NS)
    t0 = int(df.start_ns.min())
    if origin is not None:
        t0 = int(origin) - (int(origin) - t0) // width * width
    n = int((df.start_ns + df.latency_us * 1000).max() - t0) // width + 1
    ok = df[df.status == "ok"]
    bins = (ok.start_ns + ok.latency_us * 1000 - t0) // width
    bins = bins[bins >= 0]
    counts = bins.value_counts().reindex(range(n), fill_value=0).sort_index()
    return pd.Series(counts.to_numpy(), index=t0 + np.arange(n) * width)


def recovery_time(series, outage_start_ns, outage_end_ns, threshold=0.9, windows=3) -> float | None:
    """Seconds from the outage end until `windows` consecutive bins reach
    `threshold` of the pre-outage mean; None if throughput never recovers."""
    width = series.index[1] - series.index[0]
    base = series[series.index + width <= outage_start_ns].mean()
    after = series[series.index >= outage_end_ns]
    good = (after / base >= threshold).to_numpy()
    for i in range(len(good) - windows + 1):
        if good[i : i + windows].all():
            return (after.index[i] - outage_end_ns) / NS
    return None


def degraded_s(series, rps, exclude=None, end_ns=None, threshold=0.9) -> float:
    """Number of 1 s bins with ok throughput below threshold x rps, outside
    exclude=(start_ns, stop_ns); NaN in closed loop (rps == 0). The first bin and
    bins ending after end_ns (the last scheduled send) are partial and ignored.
    In s1 and s5 a non-zero value points to host noise; with loss or jitter it
    can also be the transport (retransmission stalls)."""
    if not rps:
        return np.nan
    width = series.index[1] - series.index[0]
    s = series.iloc[1:-1]
    if end_ns is not None:
        s = s[s.index + width <= end_ns]
    if exclude is not None:
        s = s[(s.index + width <= exclude[0]) | (s.index >= exclude[1])]
    return float((s < threshold * rps).sum())


def _mib(s: str) -> float:
    m = re.fullmatch(r"([\d.]+)\s*([KMGT]?i?B)", s.split("/")[0].strip())
    return float(m[1]) * MIB[m[2]] if m and m[2] in MIB else np.nan


def parse_stats(path) -> pd.DataFrame:
    """stats.csv with cpu_perc (%) and mem_mib as floats; '--' becomes NaN."""
    df = pd.read_csv(path, dtype=str)
    df["ts_ns"] = df.ts_ns.astype("int64")
    df["cpu_perc"] = pd.to_numeric(df.cpu_perc.str.rstrip("%"), errors="coerce")
    df["mem_mib"] = df.mem_usage.map(_mib)
    return df.drop(columns="mem_usage")


def summarize(reps: pd.DataFrame) -> pd.DataFrame:
    """One row per scenario x transport: n, <metric>_mean, <metric>_ci95 (Student t),
    cv_p50 and mwu_p (two-sided Mann-Whitney U, h2 vs h3, over per-rep p99)."""
    metrics = [c for c in reps.columns if c not in KEYS + ["rep"]]
    rows = []
    for (scenario, transport), g in reps.groupby(KEYS, sort=True):
        row = {"scenario": scenario, "transport": transport, "n": len(g)}
        for m in metrics:
            v = g[m].dropna()
            row[f"{m}_mean"] = v.mean()
            row[f"{m}_ci95"] = (
                stats.t.ppf(0.975, len(v) - 1) * v.std(ddof=1) / math.sqrt(len(v)) if len(v) > 1 else np.nan
            )
        row["cv_p50"] = g.p50.std(ddof=1) / g.p50.mean()
        rows.append(row)
    out = pd.DataFrame(rows)
    out["mwu_p"] = np.nan
    for scenario, g in reps.groupby("scenario"):
        a, b = g.p99[g.transport == "h2"], g.p99[g.transport == "h3"]
        if len(a) and len(b):
            out.loc[out.scenario == scenario, "mwu_p"] = stats.mannwhitneyu(a, b, alternative="two-sided").pvalue
    return out


def comparison(reps: pd.DataFrame) -> pd.DataFrame:
    """h3 vs h2 per scenario x metric: means, delta_pct = (h3 - h2) / h2 x 100 and the
    two-sided Mann-Whitney U p over per-rep values; metrics missing on one side are dropped."""
    rows = []
    for scenario, g in reps.groupby("scenario", sort=True):
        for m in COMPARE:
            if m not in g:
                continue
            a, b = g[m][g.transport == "h2"].dropna(), g[m][g.transport == "h3"].dropna()
            if a.empty or b.empty:
                continue
            rows.append({
                "scenario": scenario, "metric": m, "h2_mean": a.mean(), "h3_mean": b.mean(),
                "delta_pct": (b.mean() - a.mean()) / a.mean() * 100 if a.mean() else np.nan,
                "mwu_p": stats.mannwhitneyu(a, b, alternative="two-sided").pvalue, "n_h2": len(a), "n_h3": len(b),
            })
    cols = ["scenario", "metric", "h2_mean", "h3_mean", "delta_pct", "mwu_p", "n_h2", "n_h3"]
    return pd.DataFrame(rows, columns=cols)


def _container_stats(path, lo, hi) -> dict:
    """Mean CPU% and memory per container over [lo, hi] ns
    (bench-server-1 -> server_*, bench-loadgen -> loadgen_*)."""
    df = parse_stats(path)
    df = df[df.ts_ns.between(lo, hi)]
    out = {}
    for c, g in df.groupby(df.name.str.extract(r"^bench-([a-z]+)")[0]):
        out[f"{c}_cpu"] = g.cpu_perc.mean()
        out[f"{c}_mem_mib"] = g.mem_mib.mean()
    return out


def _warn(msg):
    print(f"analyze: {msg}", file=sys.stderr)


def _style(ax):
    ax.grid(color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ["top", "right"]:
        ax.spines[side].set_visible(False)
    for side in ["left", "bottom"]:
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelcolor=INK)


def _log_axis(axis):
    """Log scale labelled 1-2-5 as plain numbers (no crowded minor labels)."""
    axis.set_major_locator(LogLocator(base=10, subs=(1, 2, 5)))
    axis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    axis.set_minor_formatter(NullFormatter())


def _legend(fig, transports):
    handles = [plt.Line2D([], [], color=COLORS[t], linewidth=2, label=t) for t in transports]
    fig.legend(handles=handles, loc="upper right", frameon=False, labelcolor=INK)


def _save(fig, path):
    fig.tight_layout(rect=(0, 0, 0.92, 1))
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_cdf(curves, path):
    """Latency CDF per scenario; each curve averages the reps' quantiles."""
    scenarios = sorted({s for s, _ in curves})
    cols = min(3, len(scenarios))
    rows = math.ceil(len(scenarios) / cols)
    fig, axes = plt.subplots(rows, cols, figsize=(4.2 * cols + 0.6, 3.2 * rows), squeeze=False)
    for ax, scenario in zip(axes.flat, scenarios):
        for t in COLORS:
            if (scenario, t) in curves:
                ax.plot(np.mean(curves[(scenario, t)], axis=0) / 1000, QUANTILES / 100, color=COLORS[t], linewidth=2)
        ax.set_xscale("log")
        _log_axis(ax.xaxis)
        ax.set_title(scenario, color=INK, fontsize=10)
        ax.set_xlabel("latency (ms)", color=MUTED)
        ax.set_ylabel("fraction of ok RPCs", color=MUTED)
        _style(ax)
    for ax in axes.flat[len(scenarios) :]:
        ax.set_visible(False)
    _legend(fig, [t for t in COLORS if any(k[1] == t for k in curves)])
    _save(fig, path)


def plot_p99(reps, path):
    """p99 per repetition: a box per scenario x transport plus the rep points."""
    scenarios = sorted(reps.scenario.unique())
    fig, ax = plt.subplots(figsize=(1.6 * len(scenarios) + 2, 4))
    for i, scenario in enumerate(scenarios):
        for j, t in enumerate(COLORS):
            v = reps.p99[(reps.scenario == scenario) & (reps.transport == t)] / 1000
            if v.empty:
                continue
            x = i + (j - 0.5) * 0.36
            line = {"color": COLORS[t]}
            ax.boxplot(
                v, positions=[x], widths=0.28, showfliers=False,
                medianprops={**line, "linewidth": 2}, boxprops=line, whiskerprops=line, capprops=line,
            )
            ax.scatter(np.full(len(v), x), v, s=16, color=COLORS[t], edgecolors="white", linewidths=1, zorder=3)
    ax.set_xticks(range(len(scenarios)), scenarios, rotation=20)
    ax.set_xlim(-0.6, len(scenarios) - 0.4)
    ax.set_yscale("log")
    _log_axis(ax.yaxis)
    ax.set_ylabel("p99 latency (ms)", color=MUTED)
    _style(ax)
    _legend(fig, [t for t in COLORS if t in set(reps.transport)])
    _save(fig, path)


def plot_outage(series, path):
    """s5 ok RPCs per 1 s bin, aligned at the outage start; one line per rep."""
    fig, ax = plt.subplots(figsize=(8, 3.6))
    dur = np.mean([(end - start) / NS for _, _, start, end in series])
    ax.axvspan(0, dur, color=GRID, linewidth=0)
    ax.text(dur / 2, 1.0, "outage", transform=ax.get_xaxis_transform(), ha="center", va="bottom", color=MUTED)
    for t, s, start, _ in series:
        s = s.iloc[1:-1]  # edge bins are partial
        ax.plot((s.index - start) / NS, s.to_numpy(), color=COLORS[t], linewidth=1.2, alpha=0.7)
    ax.set_xlabel("time since outage start (s)", color=MUTED)
    ax.set_ylabel("ok RPCs per second", color=MUTED)
    _style(ax)
    _legend(fig, [t for t in COLORS if any(x[0] == t for x in series)])
    _save(fig, path)


def _rep_row(d, name):
    """Metrics of one repetition dir, or None when it is not valid."""
    meta_path = d / "meta.json"
    if not meta_path.exists():
        _warn(f"{name}: no meta.json, skipped")
        return None
    meta = json.loads(meta_path.read_text())
    if meta.get("interrupted"):
        _warn(f"{name}: interrupted, skipped")
        return None
    if meta.get("dials") != 1:  # spec §9: more than one connection invalidates the rep
        _warn(f"{name}: dials={meta.get('dials')}, skipped")
        return None
    df = load_requests(d / "requests.csv")
    if not (df.status == "ok").any():
        _warn(f"{name}: no ok RPC, skipped")
        return None
    row = {("lat_" + k if k in ("mean", "std") else k): v for k, v in latency_stats(df).items()}
    if "handshake_us" in meta:
        row["handshake_ms"] = meta["handshake_us"] / 1000
    row.update(throughput(df))
    row["error_rate"] = error_rate(df)
    if (d / "stats.csv").exists():
        end = df.start_ns + df.latency_us * 1000
        row.update(_container_stats(d / "stats.csv", df.start_ns.min(), end.max()))
        if "server_cpu" in row:
            row["cpu_per_krps"] = row["server_cpu"] / (row["ok_rps"] / 1000)
    else:
        _warn(f"{name}: no stats.csv")
    return row, df, meta.get("load", {}).get("rps", 0)


def main(argv: list[str]) -> int:
    """analyze.py <run_dir> -> <run_dir>/analysis/{reps,summary,comparison}.csv and *.png"""
    if len(argv) != 1:
        print("usage: analyze.py <run_dir>", file=sys.stderr)
        return 2
    run = Path(argv[0])
    log = run / "failures.log"
    failed = {tuple(line.split(",")) for line in log.read_text().split()} if log.exists() else set()
    rows, curves, series = [], {}, []
    for d in sorted(run.glob("*/*/rep-*")):
        scenario, transport = d.parts[-3], d.parts[-2]
        name = f"{scenario}/{transport}/{d.name}"
        if (scenario, transport, d.name.removeprefix("rep-")) in failed:
            _warn(f"{name}: in failures.log, skipped")
            continue
        got = _rep_row(d, name)
        if got is None:
            continue
        row, df, rps = got
        s, exclude = throughput_series(df), None
        if (d / "events.csv").exists():
            ev = pd.read_csv(d / "events.csv").set_index("event").ts_ns
            s = throughput_series(df, origin=ev.outage_end)
            rec = recovery_time(s, ev.outage_start, ev.outage_end)
            if rec is None:
                _warn(f"{name}: throughput never recovered")
            row["recovery_s"] = np.nan if rec is None else rec
            exclude = (ev.outage_start, math.inf if rec is None else ev.outage_end + rec * NS)
            series.append((transport, s, ev.outage_start, ev.outage_end))
        row["degraded_s"] = degraded_s(s, rps, exclude, end_ns=df.start_ns.max())
        if row["degraded_s"] > 0:
            _warn(f"{name}: degraded_s={row['degraded_s']:.0f} (1 s bins below 90% of --rps={rps}, outage excluded)")
        curves.setdefault((scenario, transport), []).append(np.percentile(df.latency_us[df.status == "ok"], QUANTILES))
        rows.append({"scenario": scenario, "transport": transport, "rep": int(d.name.removeprefix("rep-")), **row})
    if not rows:
        _warn(f"no valid repetitions under {run}")
        return 1

    out = run / "analysis"
    out.mkdir(exist_ok=True)
    reps = pd.DataFrame(rows)
    reps.to_csv(out / "reps.csv", index=False)
    summarize(reps).to_csv(out / "summary.csv", index=False)
    comparison(reps).to_csv(out / "comparison.csv", index=False)
    plot_cdf(curves, out / "latency_cdf.png")
    plot_p99(reps, out / "p99_boxplot.png")
    if series:
        plot_outage(series, out / "s5_throughput.png")
    print(f"analyze: {len(reps)} reps -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
