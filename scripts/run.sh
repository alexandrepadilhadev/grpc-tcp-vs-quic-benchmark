#!/usr/bin/env bash
# Experiment runner (spec §6.4).
#   bash scripts/run.sh                              every scenario x {h2,h3} x REPS
#   bash scripts/run.sh rep <env> <transport> <dir>  one repetition (own process)
#   bash scripts/run.sh trace <transport> [scenario] debug-level trace run + timeline.txt
# Env: SCENARIOS (glob over experiments/scenarios, default *), REPS (default 5),
# RUN_PREFIX (prefix of the run folder), LOADGEN_EXTRA (flags appended to the
# scenario's LOADGEN_ARGS; the last value of a flag wins), OUTAGE_SVC (default gateway),
# DRAIN_SECS (wait for the async path before down, default 2),
# QLOG=1 (diagnostics only: qlog of every QUIC endpoint in <rep>/qlog/ and the
# gateway's TCP counters in <rep>/nstat.txt; tracing costs CPU, do not use for measurements).
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f deploy/compose.yaml)
SERVICES=(gateway core ledger risk rabbitmq postgres)

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
	# stop first: shutdown writes svc-<name>.json and logs "<svc> stopped" (or why it could not)
	"${COMPOSE[@]}" --profile load stop >/dev/null 2>&1 || true
	local s
	for s in "${SERVICES[@]}"; do # JSON lines only: timeline.py parses them
		"${COMPOSE[@]}" logs --no-color --no-log-prefix "$s" >"$dir/$s.log" 2>&1 || true
	done
	# -v: postgres and rabbitmq declare volumes that would pile up every repetition
	"${COMPOSE[@]}" --profile load down -v --remove-orphans >/dev/null 2>&1 || true
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
	[[ -z ${QLOG:-} ]] || export QLOGDIR=/results/${dir#results/}/qlog # path inside the containers
	export REP_DIR=/results/${dir#results/}                             # svc-<name>.json at shutdown

	"${COMPOSE[@]}" up -d --wait gateway risk
	sample_stats >"$dir/stats.csv" &
	sampler=$!
	if [[ -n ${OUTAGE_AT:-} ]]; then
		(sleep "$OUTAGE_AT" && "${COMPOSE[@]}" exec -T "${OUTAGE_SVC:-gateway}" netem.sh outage "${OUTAGE_SECS:?}") >"$dir/events.csv" &
		outage=$!
	fi
	# --no-deps: the stack is already up; LOADGEN_ARGS and LOADGEN_EXTRA are lists of flags
	# shellcheck disable=SC2086
	"${COMPOSE[@]}" run --rm --no-deps --name bench-loadgen loadgen $LOADGEN_ARGS ${LOADGEN_EXTRA:-} \
		--target=gateway:8443 --out="/results/${dir#results/}/requests.csv" >"$dir/loadgen.log" 2>&1
	if [[ -n $outage ]]; then
		wait "$outage"
		outage=
	fi
	sleep "${DRAIN_SECS:-2}" # let the async path (core -> rabbitmq -> risk -> core) drain
	if [[ -n ${QLOG:-} ]]; then
		"${COMPOSE[@]}" exec -T gateway nstat -az >"$dir/nstat.txt" 2>&1 || true
	fi
}

# run_rep runs repetition r into <base>/rep-<r> in its own process. Handshake packets
# lost under netem can outlast the probe deadline, so a probe failure is retried once;
# the failed attempt is kept as <base>/probe-failed-rep-<r> (ignored by the analysis).
run_rep() {
	local env=$1 t=$2 base=$3 r=$4
	bash scripts/run.sh rep "$env" "$t" "$base/rep-$r" && return 0
	grep -q '"loadgen failed","error":"probe:' "$base/rep-$r/loadgen.log" 2>/dev/null || return 1
	echo "!! rep-$r probe failed, retrying once" >&2
	mv "$base/rep-$r" "$base/probe-failed-rep-$r"
	bash scripts/run.sh rep "$env" "$t" "$base/rep-$r"
}

matrix() {
	local reps=${REPS:-5}
	[[ $reps =~ ^[1-9][0-9]*$ ]] || die "REPS must be a positive integer, got '$reps'"
	[[ -f certs/server.crt ]] || die "certs/server.crt missing: run make certs"
	mkdir -p results
	[[ -w results ]] || die "results/ not writable: sudo chown -R $USER: results"
	local rmem
	rmem=$(sysctl -n net.core.rmem_max)
	((rmem >= 7500000)) || echo "warning: net.core.rmem_max=$rmem < 7500000; run: sudo bash scripts/host-tuning.sh" >&2

	local run_id root
	run_id=$(date +%Y-%m-%d-%H-%M-%S) # VM local time; the commit is in meta.json
	root=results/${RUN_PREFIX:-}$run_id
	mkdir "$root" || die "$root already exists (results are immutable)"

	local env scenario r t order
	for env in experiments/scenarios/${SCENARIOS:-*}.env; do
		[[ -f $env ]] || die "no scenario matches '${SCENARIOS:-*}'"
		scenario=$(basename "$env" .env)
		for ((r = 1; r <= reps; r++)); do
			order=(h2 h3)
			((r % 2)) || order=(h3 h2)
			for t in "${order[@]}"; do
				echo "== $scenario $t rep-$r"
				if ! run_rep "$env" "$t" "$root/$scenario/$t" "$r"; then
					echo "$scenario,$t,$r" >>"$root/failures.log"
					echo "!! $scenario $t rep-$r failed (see failures.log)" >&2
				fi
			done
		done
	done
	echo "run_id=${root#results/}" # the folder name, prefix included
}

# trace runs one short repetition with every service at debug level into
# results/trace-<time>/<transport>/ (outside the analysis glob) and writes timeline.txt.
trace() {
	local t=$1 env=experiments/scenarios/${2:-s1-baseline}.env dir
	[[ -f $env ]] || die "no scenario file $env"
	[[ -f certs/server.crt ]] || die "certs/server.crt missing: run make certs"
	dir=results/trace-$(date +%Y-%m-%d-%H-%M-%S)/$t
	mkdir -p "$(dirname "$dir")"
	LOG_LEVEL=debug LOADGEN_EXTRA="--requests=20 --rps=5 --warmup=0" \
		bash scripts/run.sh rep "$env" "$t" "$dir" || die "trace repetition failed (see $dir)"
	"${COMPOSE[@]}" run --rm --build --entrypoint python analysis timeline.py "/$dir" || die "timeline failed (see $dir)"
	echo "trace_dir=$dir"
}

case "${1:-}" in
"") matrix ;;
trace)
	[[ $# -ge 2 && $# -le 3 ]] || die "usage: run.sh trace <transport> [scenario]"
	trace "$2" "${3:-}"
	;;
rep)
	[[ $# -eq 4 ]] || die "usage: run.sh rep <env> <transport> <dir>"
	export TRANSPORT=$3
	rep "$2" "$4"
	;;
*) die "usage: run.sh [rep <env> <transport> <dir> | trace <transport> [scenario]]" ;;
esac
