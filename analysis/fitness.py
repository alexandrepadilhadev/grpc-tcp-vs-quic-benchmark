"""Environment fitness (payment spec §9): judges the s1-baseline h2 repetition of
`make fitness` by its CPU per 1000 RPC/s. Host noise makes the same load cost
up to ~2x the CPU, which no repetition-to-repetition comparison sees when the
whole run is degraded; a long run must not start in that state.

usage: python fitness.py <fitness run dir> [--max CPU_PER_KRPS]
exit: 0 fit, 1 degraded, 2 nothing to judge
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

MAX_CPU_PER_KRPS = 200.0  # clean at 250 RPC/s: 134–172 (2026-10-12/13, vCPU reserved); degraded: 268–600


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(prog="fitness.py")
    p.add_argument("run", type=Path)
    p.add_argument("--max", type=float, default=MAX_CPU_PER_KRPS)
    args = p.parse_args(argv)
    path = args.run / "analysis" / "reps.csv"
    reps = pd.read_csv(path) if path.exists() else pd.DataFrame(columns=["transport"])
    h2 = reps[reps.transport == "h2"].get("cpu_per_krps", pd.Series(dtype=float)).dropna()
    if h2.empty:
        print(f"fitness: no h2 cpu_per_krps in {args.run}", file=sys.stderr)
        return 2
    v = h2.max()
    verdict = "ok" if v <= args.max else "degraded"
    print(f"fitness: h2 cpu_per_krps={v:.1f} (max {args.max:.0f}) {verdict}")
    return 0 if verdict == "ok" else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
