"""Timeline of a trace run (payment spec §11): merges the JSON logs of every
container in <dir>/*.log by time and writes <dir>/timeline.txt, one block per payment_id.

usage: python timeline.py <dir>
"""

import json
import sys
from pathlib import Path

import pandas as pd

SKIP = {"time", "level", "msg", "payment_id"}  # shown in the header or the line itself


def read_logs(d: Path) -> list[tuple[str, dict]]:
    """(service, record) of every JSON log line with a payment_id; the service is the file stem."""
    out = []
    for f in sorted(d.glob("*.log")):
        for line in f.read_text(errors="replace").splitlines():
            try:
                rec = json.loads(line)
            except ValueError:
                continue  # netem.sh and other plain-text lines
            if isinstance(rec, dict) and rec.get("payment_id") and "time" in rec:
                out.append((f.stem, rec))
    return out


def render(entries: list[tuple[str, dict]]) -> str:
    if not entries:
        return ""
    ns = pd.to_datetime([r["time"] for _, r in entries], utc=True, format="ISO8601").asi8  # ns precision
    by_id: dict[str, list] = {}
    for t, (svc, rec) in sorted(zip(ns, entries), key=lambda x: x[0]):
        by_id.setdefault(rec["payment_id"], []).append((t, svc, rec))
    blocks = []
    for pid, rows in sorted(by_id.items(), key=lambda kv: kv[1][0][0]):
        proto = next((r["protocol"] for _, _, r in rows if r.get("msg") == "rpc" and "protocol" in r), "?")
        lines = [f"payment {pid} ({proto})"]
        t0 = rows[0][0]
        for t, svc, rec in rows:
            fields = " ".join(f"{k}={v}" for k, v in rec.items() if k not in SKIP)
            lines.append(f"  +{(t - t0) / 1e6:.3f} ms  {svc:<8} {rec.get('msg', '')}" + (f" {fields}" if fields else ""))
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks) + "\n"


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    d = Path(argv[0])
    entries = read_logs(d)
    if not entries:  # LOG_LEVEL=debug not applied, or the logs were lost
        print(f"timeline: no payment_id in {d}/*.log", file=sys.stderr)
        return 1
    (d / "timeline.txt").write_text(render(entries))
    print(f"timeline: {len({r['payment_id'] for _, r in entries})} payments -> {d / 'timeline.txt'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
