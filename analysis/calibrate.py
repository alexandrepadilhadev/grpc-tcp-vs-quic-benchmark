"""Load calibration (payment spec §9): one row per rate x transport from the
s1-baseline sweep (make calibrate, then make analyze on each run) and the rates
the spec's rules give.

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
    t = pd.DataFrame({
        "n": g.size(),
        "served_frac_min": g.served_frac.min(),
        "late_starts_max": g.late_starts.max(),
        "p50_median": g.p50.median(),
        **{f"{c}_max": g[c].max() for c in cpu},
    }).reset_index()
    base = t.sort_values("rate").groupby("transport").p50_median.transform("first")
    t["pass"] = (
        (t.served_frac_min >= SERVED_MIN)
        & (t.late_starts_max == 0)
        & (t.p50_median <= P50_MAX * base)
        & (t[[f"{c}_max" for c in cpu]].max(axis=1) <= CPU_MAX)
    )
    return t.sort_values(["rate", "transport"]).reset_index(drop=True)


def capacity(t: pd.DataFrame) -> int | None:
    """C: the highest rate where h3 (the CPU worst case) passes every rule."""
    ok = t[(t.transport == "h3") & t["pass"]]
    return int(ok.rate.max()) if len(ok) else None


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
    c = capacity(t)
    if c is None:
        print("calibrate: no rate passes every rule for h3; sweep lower rates", file=sys.stderr)
        return 1
    print(f"\nC = {c} RPC/s (h3)\nhigh rate = {high_rate(c)} RPC/s")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
