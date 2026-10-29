"""Load calibration (payment spec §9): one row per rate x transport from the
s1-baseline sweep (make calibrate, then make analyze on each run), the capacity C
(the highest rate h3 still serves) and the high rate 0.4 x C. "healthy" (CPU and
p50 rules) is the check the chosen rate must pass, not what defines C.

usage: python calibrate.py <calib run dir>...
"""

import json
import math
import sys
from pathlib import Path

import pandas as pd

CPU_MAX = 70.0  # % of the container's one-CPU cpuset, docker stats scale (100 = 1 vCPU)
P50_MAX = 1.5  # x the p50 at the lowest rate
SERVED_MIN = 0.99
OWN_CPU = ["gateway", "core", "ledger", "loadgen"]  # one cpuset each
DATA = ["risk", "rabbitmq", "postgres"]  # share CPU 5: checked summed


def load(run: Path) -> pd.DataFrame:
    """reps.csv of one analyzed run plus each rep's rate and late_starts from meta.json."""
    reps = pd.read_csv(run / "analysis" / "reps.csv")
    rates, late = [], []
    for r in reps.itertuples():
        meta = json.loads((run / r.scenario / r.transport / f"rep-{r.rep}" / "meta.json").read_text())
        rates.append(meta["load"]["rps"])
        late.append(meta.get("late_starts", 0))
    reps["rate"], reps["late_starts"] = rates, late
    reps["data_cpu"] = sum(reps.get(f"{c}_cpu", 0) for c in DATA)
    return reps[reps.scenario == "s1-baseline"]


def table(runs: list[Path]) -> pd.DataFrame:
    reps = pd.concat([load(r) for r in runs], ignore_index=True)
    cpu = [f"{c}_cpu" for c in OWN_CPU if f"{c}_cpu" in reps] + ["data_cpu"]
    g = reps.groupby(["rate", "transport"])
    # late_starts is reported, not judged: in the VM, timer wake-ups at low rates
    # delay starts without saturation, which the CPU and p50 rules catch (spec §9)
    t = pd.DataFrame({
        "n": g.size(),
        "served_frac_min": g.served_frac.min(),
        "late_starts_max": g.late_starts.max(),
        "p50_median": g.p50.median(),
        # host noise in the sweep itself: rerun the rate rather than trust its CPU
        "cpu_outliers": g.cpu_outlier.sum() if "cpu_outlier" in reps else 0,
        **{f"{c}_max": g[c].max() for c in cpu},
    }).reset_index()
    base = t.sort_values("rate").groupby("transport").p50_median.transform("first")
    t["healthy"] = (
        (t.served_frac_min >= SERVED_MIN)
        & (t.p50_median <= P50_MAX * base)
        & (t[[f"{c}_max" for c in cpu]].max(axis=1) <= CPU_MAX)
    )
    return t.sort_values(["rate", "transport"]).reset_index(drop=True)


def capacity(t: pd.DataFrame) -> int | None:
    """C: the highest rate where h3 (the CPU worst case) serves the demand,
    the saturation capacity the POC's 40% rule refers to. A rate with fewer valid
    reps than the sweep ran (analyze skipped failed ones) does not serve."""
    ok = t[(t.transport == "h3") & (t.served_frac_min >= SERVED_MIN) & (t.n == t.n.max())]
    return int(ok.rate.max()) if len(ok) else None


def short_rates(t: pd.DataFrame) -> list[str]:
    """Rows with fewer valid reps than the sweep ran, as warnings."""
    full = t.n.max()
    return [f"{r.rate} {r.transport}: {r.n} of {full} reps valid (see analyze warnings)"
            for r in t.itertuples() if r.n < full]


def high_rate(c: int) -> int:
    return int(math.floor(0.4 * c / 50) * 50)


def markdown(t: pd.DataFrame) -> str:
    fmt = lambda v: f"{v:.3f}" if isinstance(v, float) else str(v)  # noqa: E731
    lines = ["| " + " | ".join(t.columns) + " |", "|" + "---|" * len(t.columns)]
    lines += ["| " + " | ".join(fmt(v) for v in row) + " |" for row in t.itertuples(index=False)]
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    t = table([Path(a) for a in argv])
    print(markdown(t))
    for w in short_rates(t):
        print(f"calibrate: {w}", file=sys.stderr)
    c = capacity(t)
    if c is None:
        print("calibrate: no rate serves the demand for h3; sweep lower rates", file=sys.stderr)
        return 1
    print(f"\nC = {c} RPC/s (h3 serves >= {SERVED_MIN:.0%})\nhigh rate = {high_rate(c)} RPC/s"
          " (check: s1 h3 at that rate must be healthy)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
