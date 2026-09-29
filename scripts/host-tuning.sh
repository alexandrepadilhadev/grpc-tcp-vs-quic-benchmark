#!/usr/bin/env bash
# VM prerequisites, once per boot: sudo bash scripts/host-tuning.sh
# Raises UDP buffer limits for quic-go (never lowers them) and loads sch_netem.
set -euo pipefail

want=7500000
for key in net.core.rmem_max net.core.wmem_max; do
	cur=$(sysctl -n "$key")
	if ((cur < want)); then
		sysctl -w "$key=$want"
	else
		echo "$key = $cur (kept)"
	fi
done

if ! modprobe sch_netem; then
	echo "sch_netem not available: sudo apt-get install linux-modules-extra-$(uname -r)" >&2
	exit 1
fi
echo "sch_netem loaded"
