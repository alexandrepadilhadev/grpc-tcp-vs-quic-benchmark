#!/usr/bin/env bash
# Experiment runner (spec §6.4).
#   bash scripts/run.sh                              every scenario x {h2,h3} x REPS
#   bash scripts/run.sh rep <env> <transport> <dir>  one repetition (own process)
# Env: SCENARIOS (glob over experiments/scenarios, default *), REPS (default 5).
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f deploy/compose.yaml)

die() {
	echo "run.sh: $*" >&2
	exit 1
}

# sample_stats prints container CPU/memory as CSV; each docker stats call takes ~1-2 s.
sample_stats() {
	echo "ts_ns,name,cpu_perc,mem_usage"
	while :; do
		ts=$(date +%s%N)
		docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' 2>/dev/null |
			awk -v ts="$ts" '/^bench-/ { print ts "," $0 }' || true
	done
}

sampler= outage=
cleanup() {
	[[ -z $sampler ]] || kill "$sampler" 2>/dev/null || true
	[[ -z $outage ]] || kill "$outage" 2>/dev/null || true
	"${COMPOSE[@]}" logs --no-color server >"$dir/server.log" 2>&1 || true
	"${COMPOSE[@]}" --profile load down --remove-orphans >/dev/null 2>&1 || true
}

rep() {
	local env=$1
	dir=$2
	set -a
	# shellcheck disable=SC1090
	. "$env"
	set +a
	mkdir -p "$dir"
	trap cleanup EXIT

	"${COMPOSE[@]}" up -d --wait server
	sample_stats >"$dir/stats.csv" &
	sampler=$!
	if [[ -n ${OUTAGE_AT:-} ]]; then
		(sleep "$OUTAGE_AT" && "${COMPOSE[@]}" exec -T server netem.sh outage "${OUTAGE_SECS:?}") >"$dir/events.csv" &
		outage=$!
	fi
	# shellcheck disable=SC2086 # LOADGEN_ARGS is a list of flags
	"${COMPOSE[@]}" run --rm --name bench-loadgen loadgen $LOADGEN_ARGS \
		--target=server:8443 --out="/results/${dir#results/}/requests.csv" >"$dir/loadgen.log" 2>&1
	if [[ -n $outage ]]; then
		wait "$outage"
		outage=
	fi
}

matrix() {
	local reps=${REPS:-5}
	[[ $reps =~ ^[1-9][0-9]*$ ]] || die "REPS must be a positive integer, got '$reps'"
	[[ -f certs/server.crt ]] || die "certs/server.crt missing: run make certs"
	local rmem
	rmem=$(sysctl -n net.core.rmem_max)
	((rmem >= 7500000)) || echo "warning: net.core.rmem_max=$rmem < 7500000; run: sudo bash scripts/host-tuning.sh" >&2

	local run_id root
	run_id=$(date -u +%Y%m%dT%H%M%SZ)-$(git rev-parse --short HEAD)
	root=results/$run_id
	mkdir -p "$root"

	local env scenario r t order
	for env in experiments/scenarios/${SCENARIOS:-*}.env; do
		[[ -f $env ]] || die "no scenario matches '${SCENARIOS:-*}'"
		scenario=$(basename "$env" .env)
		for ((r = 1; r <= reps; r++)); do
			order=(h2 h3)
			((r % 2)) || order=(h3 h2)
			for t in "${order[@]}"; do
				echo "== $scenario $t rep-$r"
				if ! bash scripts/run.sh rep "$env" "$t" "$root/$scenario/$t/rep-$r"; then
					echo "$scenario,$t,$r" >>"$root/failures.log"
					echo "!! $scenario $t rep-$r failed (see failures.log)" >&2
				fi
			done
		done
	done
	echo "run_id=$run_id"
}

case "${1:-}" in
"") matrix ;;
rep)
	[[ $# -eq 4 ]] || die "usage: run.sh rep <env> <transport> <dir>"
	export TRANSPORT=$3
	rep "$2" "$4"
	;;
*) die "usage: run.sh [rep <env> <transport> <dir>]" ;;
esac
