"""Analysis of results/<run_id>/ (spec §6.5)."""

import argparse
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
ID = ["run", "rep", "excluded", "cpu_outlier"]  # reps.csv columns that are not metrics
QUANTILES = np.linspace(0, 100, 1001)
COLORS = {"h2": "#2a78d6", "h3": "#eb6834"}  # dataviz categorical slots 1-2, validated (light)
COMPARE = [
    "p50", "p99", "p999", "p50_all", "p99_all", "p999_all", "error_rate", "ok_rps", "served_frac", "recovery_s", "cpu_per_krps", "handshake_ms",
    "orphans", "orphan_share", "interrupt_ms", "gap_base_ms", "migration_errors", "migrate_ms",
]
RATIO = {"ok_rps", "served_frac"}  # throughput metrics: comparison also reports h3/h2 with a CI
RPC_SERVICES = ["gateway", "core", "ledger"]  # the gRPC services whose CPU makes rpc_cpu
SVC_REPORTS = ["gateway", "core", "ledger", "risk"]  # svc-<name>.json each payment repetition must have
CPU_OUTLIER_MIN = 0.20  # cpu_outlier also needs +20% over the median (host noise seen: 2.3x)
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


def latency_all(df, unsent, deadline_us) -> dict:
    """Percentiles in us over every scheduled measure slot (no survivorship bias): ok RPCs at
    their latency, failed RPCs at max(latency, deadline) and unsent slots at the deadline.
    Like p*, only meaningful without saturation (served_frac ~ 1): the unsent count low."""
    lat = np.where(df.status == "ok", df.latency_us, np.maximum(df.latency_us, deadline_us)).astype(float)
    lat = np.concatenate([lat, np.full(int(unsent), float(deadline_us))])
    p50, p99, p999 = np.percentile(lat, [50, 99, 99.9])
    return {"p50_all": p50, "p99_all": p99, "p999_all": p999}


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


def served_frac(meta) -> float:
    """Fraction of the open-loop schedule (rps x duration) that ended ok, from meta.json;
    slots the pacer never sent count as not served. NaN in closed loop (rps == 0)."""
    load = meta.get("load", {})
    if not load.get("rps"):
        return np.nan
    return meta["ok"] / (load["rps"] * load["duration_ns"] / NS)


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


def _order_ci(n, cdf, level=0.95):
    """1-based rank k of the narrowest symmetric interval [x(k), x(n+1-k)] over n sorted
    values covering >= level, where cdf(j) = P(rank statistic <= j) under the null, and its
    coverage 1 - 2 cdf(k - 1); the full range (k = 1) when no interval reaches level."""
    k = 1
    while k < n // 2 and 1 - 2 * cdf(k) >= level:
        k += 1
    return k, 1 - 2 * cdf(k - 1)


def median_ci(values) -> tuple:
    """Median and distribution-free CI from order statistics (Binomial(n, 0.5)):
    (median, lo, hi, level); the CI is NaN with fewer than 2 values."""
    x = np.sort(np.asarray(values, dtype=float))
    if len(x) < 2:
        return (x[0] if len(x) else np.nan), np.nan, np.nan, np.nan
    k, level = _order_ci(len(x), lambda j: stats.binom.cdf(j, len(x), 0.5))
    return np.median(x), x[k - 1], x[-k], level


def _u_cdf(n1, n2) -> np.ndarray:
    """Exact null CDF of the Mann-Whitney U for n1 x n2 values without ties: the counts
    are the coefficients of the Gaussian binomial [n1 + n2 choose n2] in q."""
    c = [1] + [0] * (n1 * n2)
    for i in range(1, n2 + 1):  # c *= (1 - q^(n1 + i)) / (1 - q^i)
        for j in range(n1 * n2, n1 + i - 1, -1):
            c[j] -= c[j - n1 - i]
        for j in range(i, n1 * n2 + 1):
            c[j] += c[j - i]
    return np.cumsum(c) / math.comb(n1 + n2, n2)


def hodges_lehmann(a, b) -> tuple:
    """Hodges-Lehmann shift of b over a (median of all pairwise b - a) and its exact CI
    from the U distribution: (shift, lo, hi, level); the CI is NaN with a group below 2."""
    a, b = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    d = np.sort((b[:, None] - a[None, :]).ravel())
    if min(len(a), len(b)) < 2:
        return np.median(d), np.nan, np.nan, np.nan
    cdf = _u_cdf(len(a), len(b))
    k, level = _order_ci(len(d), lambda j: cdf[j])
    return np.median(d), d[k - 1], d[-k], level


def cliff_magnitude(d) -> str:
    """Romano et al. (2006) thresholds for |Cliff's delta|."""
    d = abs(d)
    return "negligible" if d < 0.147 else "small" if d < 0.33 else "medium" if d < 0.474 else "large"


def cliff_delta(a, b) -> tuple:
    """Cliff's delta of b over a, (#b > a - #b < a) / (n_a x n_b) in [-1, 1], and its magnitude."""
    diff = np.asarray(b, dtype=float)[:, None] - np.asarray(a, dtype=float)[None, :]
    d = float(np.sign(diff).mean())
    return d, cliff_magnitude(d)


def holm(p) -> np.ndarray:
    """Holm-Bonferroni adjusted p-values; NaN stays NaN and is not counted in the family."""
    p = np.asarray(p, dtype=float)
    out = np.full(len(p), np.nan)
    idx = np.flatnonzero(~np.isnan(p))
    order = idx[np.argsort(p[idx])]
    m = len(order)
    out[order] = np.minimum(1, np.maximum.accumulate(p[order] * (m - np.arange(m))))
    return out


def summarize(reps: pd.DataFrame) -> pd.DataFrame:
    """One row per scenario x transport: n, <metric>_mean, <metric>_ci95 (Student t),
    cv_p50 and mwu_p (two-sided Mann-Whitney U, h2 vs h3, over per-rep p99)."""
    metrics = [c for c in reps.columns if c not in KEYS + ID]
    rows = []
    for (scenario, transport), g in reps.groupby(KEYS, sort=True):
        row = {"scenario": scenario, "transport": transport, "n": len(g)}
        for m in metrics:
            v = g[m].dropna()
            row[f"{m}_mean"] = v.mean()
            row[f"{m}_ci95"] = (
                stats.t.ppf(0.975, len(v) - 1) * v.std(ddof=1) / math.sqrt(len(v)) if len(v) > 1 else np.nan
            )
            ci = median_ci(v)
            row[f"{m}_median"], row[f"{m}_med_lo"], row[f"{m}_med_hi"], row[f"{m}_med_level"] = ci
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
    """h3 vs h2 per scenario x metric: means, delta_pct = (h3 - h2) / h2 x 100, the
    two-sided Mann-Whitney U p over per-rep values (raw and Holm-adjusted within the
    scenario), medians, Hodges-Lehmann shift with CI and Cliff's delta; for RATIO metrics,
    the h3/h2 ratio with CI (Hodges-Lehmann over log values). Metrics missing on one side
    are dropped."""
    rows = []
    for scenario, g in reps.groupby("scenario", sort=True):
        for m in COMPARE:
            if m not in g:
                continue
            a, b = g[m][g.transport == "h2"].dropna(), g[m][g.transport == "h3"].dropna()
            if a.empty or b.empty:
                continue
            shift, lo, hi, level = hodges_lehmann(a, b)
            cd, mag = cliff_delta(a, b)
            ratio = (np.nan,) * 3
            if m in RATIO:
                if (a > 0).all() and (b > 0).all():
                    ratio = tuple(np.exp(hodges_lehmann(np.log(a), np.log(b))[:3]))
                else:
                    _warn(f"{scenario}/{m}: value <= 0, no h3/h2 ratio")
            rows.append({
                "scenario": scenario, "metric": m, "h2_mean": a.mean(), "h3_mean": b.mean(),
                "delta_pct": (b.mean() - a.mean()) / a.mean() * 100 if a.mean() else np.nan,
                "mwu_p": stats.mannwhitneyu(a, b, alternative="two-sided").pvalue, "n_h2": len(a), "n_h3": len(b),
                "h2_median": a.median(), "h3_median": b.median(),
                "hl_shift": shift, "hl_lo": lo, "hl_hi": hi, "hl_level": level, "cliff_delta": cd, "cliff_mag": mag,
                "ratio": ratio[0], "ratio_lo": ratio[1], "ratio_hi": ratio[2],
            })
    cols = [
        "scenario", "metric", "h2_mean", "h3_mean", "delta_pct", "mwu_p", "mwu_p_holm", "n_h2", "n_h3",
        "h2_median", "h3_median", "hl_shift", "hl_lo", "hl_hi", "hl_level", "cliff_delta", "cliff_mag",
        "ratio", "ratio_lo", "ratio_hi",
    ]
    out = pd.DataFrame(rows, columns=cols)
    out["mwu_p_holm"] = out.groupby("scenario").mwu_p.transform(holm)  # one family per scenario
    return out


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


def hop_problems(d) -> list[str]:
    """Why the per-hop reports of a payment repetition invalidate it: a missing or
    unreadable svc-<name>.json, or a hop that did not use exactly one connection.
    POC repetitions (no svc-*.json at all) have none."""
    if not any(d.glob("svc-*.json")):
        return []
    bad = []
    for svc in SVC_REPORTS:
        f = d / f"svc-{svc}.json"
        if not f.exists():
            bad.append(f"{f.name} missing")
            continue
        try:
            dials = json.loads(f.read_text())["dials"].items()
        except (ValueError, KeyError, TypeError, AttributeError):
            bad.append(f"{f.name} unreadable")
            continue
        bad += [f"{f.name} {peer} dials={n}" for peer, n in dials if n != 1]
    return bad


def cpu_outlier(reps: pd.DataFrame) -> pd.Series:
    """True for reps whose cpu_per_krps is above the median + 3 MAD of their run x scenario x
    transport and at least CPU_OUTLIER_MIN above the median: host noise makes the same load
    cost more CPU without lowering the throughput, which degraded_s cannot see. The floor keeps
    a very stable group (tiny MAD) from flagging a few percent. MAD 0 (or no CPU data) flags nothing."""
    if "cpu_per_krps" not in reps:
        return pd.Series(False, index=reps.index)

    def flag(x):
        med = x.median()
        mad = (x - med).abs().median()
        if not mad > 0:
            return pd.Series(False, index=x.index)
        return (x > med + 3 * mad) & (x > med * (1 + CPU_OUTLIER_MIN))

    return reps.groupby(["run"] + KEYS, group_keys=False).cpu_per_krps.apply(flag).reindex(reps.index).astype(bool)


def orphans(d) -> dict:
    """Payments the ledger settled while the client saw an error: the ledger's committed
    count minus the RPCs answered ok (every phase, plus the probe). orphan_share divides by
    the failed RPCs (NaN without failures). Empty without the ledger counter (older runs)."""
    try:
        settled = json.loads((d / "svc-ledger.json").read_text())["counters"]["settled"]
    except (OSError, ValueError, KeyError, TypeError):
        return {}
    status = pd.read_csv(d / "requests.csv", usecols=["status"]).status
    ok = int((status == "ok").sum())
    failed = len(status) - ok
    n = settled - ok - 1  # 1: the probe, answered ok and not in requests.csv
    return {"orphans": n, "orphan_share": n / failed if failed else np.nan}


def read_events(d) -> pd.Series | None:
    """ts_ns of events.csv indexed by event name; None when the rep has no events.csv."""
    f = d / "events.csv"
    return pd.read_csv(f).set_index("event")["ts_ns"] if f.exists() else None


def expected_dials(meta, events) -> int:
    """Client dials of a valid rep (s4 spec §7.1): h2 redials on every migration, h3 only
    when the migration failed."""
    n = meta.get("migrations", 0)
    if n == 0 or (meta.get("transport") == "h3" and events is not None and "migrated" in events.index):
        return 1
    return 1 + n


def peer_problems(d, meta) -> list[str]:
    """Why a migration rep is invalid: the gateway has not seen migrate_to plus another client IP."""
    if meta.get("migrations", 0) <= 0:
        return []
    try:
        peers = json.loads((d / "svc-gateway.json").read_text())["peers"]
    except (OSError, ValueError, KeyError, TypeError):
        peers = {}
    if meta.get("migrate_to") not in peers or len(peers) < 2:
        return ["migration not seen by the gateway"]
    return []


def max_gap_ms(df, lo_ns, hi_ns) -> float:
    """Largest gap in ms between consecutive ok completions inside [lo, hi]; lo and hi count
    as completions, so a window without ok gives hi - lo."""
    done = df.start_ns + df.latency_us * 1000
    t = np.sort(done[(df.status == "ok") & (done > lo_ns) & (done < hi_ns)].to_numpy(dtype="int64"))
    return float(np.diff(np.concatenate([[lo_ns], t, [hi_ns]])).max() / 1e6)


def migration_metrics(df, events, meta) -> dict:
    """s4 interruption metrics (spec §7.2) around t_m = migrate_start: interrupt_ms, the largest
    ok gap in [t_m - 1 s, t_m + 5 s]; gap_base_ms, the same in [t_m - 11 s, t_m - 5 s];
    migration_errors, failed RPCs completing in the first window; migrate_ms, migrate_start to
    migrated/migrate_failed; migrated, 1 when h3 kept its single connection."""
    t_m = int(events["migrate_start"])
    lo, hi = t_m - NS, t_m + 5 * NS
    done = df.start_ns + df.latency_us * 1000
    end = events["migrated" if "migrated" in events.index else "migrate_failed"]
    return {
        "interrupt_ms": max_gap_ms(df, lo, hi),
        "gap_base_ms": max_gap_ms(df, t_m - 11 * NS, t_m - 5 * NS),
        "migration_errors": int(((df.status != "ok") & done.between(lo, hi)).sum()),
        "migrate_ms": (end - t_m) / 1e6,
        "migrated": int("migrated" in events.index and meta.get("dials") == 1),
    }


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
    try:
        events = read_events(d)
    except (ValueError, KeyError, pd.errors.EmptyDataError):
        _warn(f"{name}: events.csv unreadable, skipped")
        return None
    if meta.get("migrations", 0) > 0 and (events is None or "migrate_start" not in events.index):
        _warn(f"{name}: migrations={meta['migrations']} but no migrate_start in events.csv, skipped")
        return None
    if events is not None and "migrate_start" in events.index and not {"migrated", "migrate_failed"} & set(events.index):
        _warn(f"{name}: migrate_start without migrated/migrate_failed in events.csv, skipped")
        return None
    want = expected_dials(meta, events)
    if meta.get("dials") != want:  # spec §9 / s4 §7.1
        _warn(f"{name}: dials={meta.get('dials')}, expected {want}, skipped")
        return None
    for problem in hop_problems(d) + peer_problems(d, meta):  # the same rule on every internal hop (payment spec §10)
        _warn(f"{name}: {problem}, skipped")
        return None
    df = load_requests(d / "requests.csv")
    if not (df.status == "ok").any():
        _warn(f"{name}: no ok RPC, skipped")
        return None
    row = {("lat_" + k if k in ("mean", "std") else k): v for k, v in latency_stats(df).items()}
    if "deadline_ns" not in meta.get("load", {}):
        raise ValueError(f"{name}: meta.json has no load.deadline_ns, needed for p*_all")
    row.update(latency_all(df, meta.get("unsent", 0), meta["load"]["deadline_ns"] / 1000))
    if "handshake_us" in meta:
        row["handshake_ms"] = meta["handshake_us"] / 1000
    row.update(throughput(df))
    row["error_rate"] = error_rate(df)
    row["served_frac"] = served_frac(meta)
    row.update(orphans(d))
    if (d / "stats.csv").exists():
        end = df.start_ns + df.latency_us * 1000
        row.update(_container_stats(d / "stats.csv", df.start_ns.min(), end.max()))
        if all(f"{c}_cpu" in row for c in RPC_SERVICES):
            row["rpc_cpu"] = sum(row[f"{c}_cpu"] for c in RPC_SERVICES)
        cpu = row.get("rpc_cpu", row.get("server_cpu"))  # payment system, else the POC server
        if cpu is not None:
            row["cpu_per_krps"] = cpu / (row["ok_rps"] / 1000)
    else:
        _warn(f"{name}: no stats.csv")
    return row, df, meta


def _scan(run):
    """Valid reps of one run dir as (row, latency quantiles, s5 series entry or None, meta)."""
    log = run / "failures.log"
    failed = {tuple(line.split(",")) for line in log.read_text().split()} if log.exists() else set()
    found = []
    for d in sorted(run.glob("*/*/rep-*")):
        scenario, transport = d.parts[-3], d.parts[-2]
        name = f"{scenario}/{transport}/{d.name}"
        if (scenario, transport, d.name.removeprefix("rep-")) in failed:
            _warn(f"{name}: in failures.log, skipped")
            continue
        got = _rep_row(d, name)
        if got is None:
            continue
        row, df, meta = got
        rps = meta.get("load", {}).get("rps", 0)
        s, exclude, entry = throughput_series(df), None, None
        ev = read_events(d)
        if ev is not None and "migrate_start" in ev.index:
            row.update(migration_metrics(df, ev, meta))
            exclude = (ev.migrate_start - NS, ev.migrate_start + 5 * NS)
        elif ev is not None and "outage_start" in ev.index:
            s = throughput_series(df, origin=ev.outage_end)
            rec = recovery_time(s, ev.outage_start, ev.outage_end)
            if rec is None:
                _warn(f"{name}: throughput never recovered")
            row["recovery_s"] = np.nan if rec is None else rec
            exclude = (ev.outage_start, math.inf if rec is None else ev.outage_end + rec * NS)
            entry = (transport, s, ev.outage_start, ev.outage_end)
        row["degraded_s"] = degraded_s(s, rps, exclude, end_ns=df.start_ns.max())
        if row["degraded_s"] > 0:
            _warn(f"{name}: degraded_s={row['degraded_s']:.0f} (1 s bins below 90% of --rps={rps}, outage excluded)")
        curve = np.percentile(df.latency_us[df.status == "ok"], QUANTILES)
        rep = int(d.name.removeprefix("rep-"))
        row = {"run": run.name, "scenario": scenario, "transport": transport, "rep": rep, **row}
        found.append((row, curve, entry, meta))
    return found


def _args(argv):
    p = argparse.ArgumentParser(prog="analyze.py", description="Analyze one or more results/<run_id>/ dirs.")
    p.add_argument("runs", type=Path, nargs="+", metavar="run", help="run dir(s); reps of the same scenario are pooled")
    p.add_argument("--out", type=Path, help="output dir (default <run>/analysis; required with several runs)")
    p.add_argument(
        "--exclude-degraded", default="", metavar="SCENARIO,...",
        help="drop reps with degraded_s > 0 or cpu_outlier in these scenarios (host noise; only without injected loss or jitter)",
    )
    try:
        args = p.parse_args(argv)
        if len(args.runs) > 1 and args.out is None:
            p.error("--out is required with several runs")
    except SystemExit:
        return None
    return args


def _check_runs(found):
    """Warn when runs pooled for the same scenario differ in load config or commit."""
    seen = {}
    for row, _, _, meta in found:
        s = seen.setdefault(row["scenario"], {"load": set(), "commit": set()})
        s["load"].add(json.dumps(meta.get("load"), sort_keys=True))
        s["commit"].add(meta.get("version"))
    for scenario, s in seen.items():
        for what, values in s.items():
            if len(values) > 1:
                _warn(f"{scenario}: {what} differs between runs")


def main(argv: list[str]) -> int:
    """analyze.py <run_dir>... [--out DIR] [--exclude-degraded=...] -> {reps,summary,comparison}.csv and *.png"""
    args = _args(argv)
    if args is None:
        return 2
    try:
        found = [x for run in args.runs for x in _scan(run)]
    except ValueError as e:
        _warn(str(e))
        return 1
    if not found:
        _warn(f"no valid repetitions under {' '.join(map(str, args.runs))}")
        return 1
    _check_runs(found)

    reps = pd.DataFrame([row for row, _, _, _ in found])
    reps["cpu_outlier"] = cpu_outlier(reps)
    for r in reps[reps.cpu_outlier].itertuples():
        _warn(f"{r.scenario}/{r.transport}/rep-{r.rep}: cpu_outlier (cpu_per_krps={r.cpu_per_krps:.1f}, run {r.run})")
    noisy = {s for s in args.exclude_degraded.split(",") if s}
    reps["excluded"] = reps.scenario.isin(noisy) & ((reps.degraded_s > 0) | reps.cpu_outlier)
    n_excluded = reps.groupby(KEYS).excluded.sum()
    for (scenario, transport), n in n_excluded[n_excluded > 0].items():
        _warn(f"{scenario}/{transport}: {n} rep(s) excluded (degraded_s > 0 or cpu_outlier)")
    kept = reps[~reps.excluded]
    curves, series = {}, []
    for (row, curve, entry, _), excluded in zip(found, reps.excluded):
        if not excluded:
            curves.setdefault((row["scenario"], row["transport"]), []).append(curve)
            series += [entry] if entry else []

    out = args.out or args.runs[0] / "analysis"
    out.mkdir(parents=True, exist_ok=True)
    reps.to_csv(out / "reps.csv", index=False)
    summary = summarize(kept)
    summary.insert(3, "n_excluded", [n_excluded[k] for k in zip(summary.scenario, summary.transport)])
    summary.to_csv(out / "summary.csv", index=False)
    comparison(kept).to_csv(out / "comparison.csv", index=False)
    plot_cdf(curves, out / "latency_cdf.png")
    plot_p99(kept, out / "p99_boxplot.png")
    if series:
        plot_outage(series, out / "s5_throughput.png")
    print(f"analyze: {len(reps)} reps -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
