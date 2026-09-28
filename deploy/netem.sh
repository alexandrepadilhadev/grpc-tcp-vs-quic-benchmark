#!/bin/sh
# Egress fault injection with tc netem (spec §6.2).
# Usage: netem.sh apply | clear | outage <seconds> | run <cmd...>
set -eu

IFACE=${NETEM_IFACE:-eth0}

clear_qdisc() {
	tc qdisc del dev "$IFACE" root 2>/dev/null || true
}

# apply sets NETEM_DELAY (ms), NETEM_JITTER (ms), NETEM_DIST and NETEM_LOSS (%);
# zero values are omitted and all zeros remove the qdisc.
apply() {
	set --
	if [ "${NETEM_DELAY:-0}" != 0 ] || [ "${NETEM_JITTER:-0}" != 0 ]; then
		set -- delay "${NETEM_DELAY:-0}ms"
		if [ "${NETEM_JITTER:-0}" != 0 ]; then
			set -- "$@" "${NETEM_JITTER}ms" distribution "${NETEM_DIST:-normal}"
		fi
	fi
	if [ "${NETEM_LOSS:-0}" != 0 ]; then
		set -- "$@" loss "${NETEM_LOSS}%"
	fi
	if [ $# -eq 0 ]; then
		clear_qdisc
		echo "netem: $IFACE clear" >&2
	else
		tc qdisc replace dev "$IFACE" root netem "$@"
		echo "netem: $IFACE $*" >&2
	fi
}

# outage drops all egress for <seconds>, restores the scenario and prints
# the real start/end times as CSV (Unix ns).
outage() {
	secs=${1:?usage: netem.sh outage <seconds>}
	echo "event,ts_ns"
	tc qdisc replace dev "$IFACE" root netem loss 100%
	echo "outage_start,$(date +%s%N)"
	sleep "$secs"
	apply
	echo "outage_end,$(date +%s%N)"
}

case "${1:-}" in
apply) apply ;;
clear) clear_qdisc ;;
outage)
	shift
	outage "$@"
	;;
run)
	shift
	apply
	exec "$@"
	;;
*)
	echo "usage: netem.sh apply | clear | outage <seconds> | run <cmd...>" >&2
	exit 2
	;;
esac
