COMPOSE   := docker compose -f deploy/compose.yaml
VERSION   ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)
TRANSPORT ?= h2
REPS      ?= 5
export VERSION TRANSPORT

.PHONY: certs build test test-db test-py smoke trace fitness calibrate calibrate-report run-all analyze down

certs:
	bash scripts/gen-certs.sh

build:
	$(COMPOSE) --profile load build

test:
	docker run --rm -v "$(CURDIR)":/src -v bench-gomod:/go/pkg/mod -w /src golang:1.27.1-trixie go test -race ./...

# PgStore integration tests against the compose postgres (on datanet)
test-db:
	$(COMPOSE) up -d --wait postgres
	docker run --rm --network bench_datanet -e TEST_DATABASE_URL=postgres://postgres:bench@postgres:5432/postgres \
		-v "$(CURDIR)":/src -v bench-gomod:/go/pkg/mod -w /src golang:1.27.1-trixie \
		go test -race -count=1 -v -run TestPgStore ./internal/ledger/; rc=$$?; $(COMPOSE) down -v; exit $$rc

# 1000 payments through the whole chain; svc-*.json and the entries count land in results/smoke/<transport>
smoke:
	mkdir -p results/smoke/$(TRANSPORT)
	REP_DIR=/results/smoke/$(TRANSPORT) $(COMPOSE) up -d --wait gateway risk
	$(COMPOSE) run --rm --no-deps loadgen --target=gateway:8443 --requests=1000 --warmup=0 --strict \
		--out=/results/smoke/$(TRANSPORT)/requests.csv; rc=$$?; sleep 2; \
	$(COMPOSE) exec -T postgres psql -U postgres -tAc 'select count(*) from entries'; \
	$(COMPOSE) --profile load down -v; exit $$rc

# every service at debug level, 20 payments; timeline.txt in results/trace-<time>/<transport>/
SCENARIO ?= s1-baseline
trace: build
	mkdir -p results
	bash scripts/run.sh trace $(TRANSPORT) $(SCENARIO)

# a long run starts only on a fit host (spec §9): s1 h2 at 250 RPC/s, ~2 min; FITNESS=0 skips it
FIT := $(if $(filter 0,$(FITNESS)),,fitness)
fitness: build
	mkdir -p results
	bash scripts/run.sh fitness

# load calibration (spec §9): s1-baseline x REPS=3 per rate into results/calib-<rate>-<time>/, then the report
RATES ?= 250 500 750 1000 1500
calibrate: build $(FIT)
	mkdir -p results
	for r in $(RATES); do \
		LOADGEN_EXTRA=--rps=$$r RUN_PREFIX=calib-$$r- SCENARIOS=s1-baseline REPS=3 bash scripts/run.sh || exit 1; \
	done
	$(MAKE) calibrate-report

# CALIB=<glob under results/> picks the sweep (default: every calib-* run)
CALIB ?= calib-*
calibrate-report:
	for d in $(notdir $(wildcard results/$(CALIB))); do $(MAKE) analyze RUN=$$d || exit 1; done
	$(COMPOSE) run --rm --entrypoint python analysis calibrate.py $(addprefix /results/,$(notdir $(wildcard results/$(CALIB))))

run-all: build $(FIT)
	mkdir -p results
	REPS=$(REPS) bash scripts/run.sh

# RUN="<run_id> ..." pools runs (OUT=<dir under results/> required); EXCLUDE=<scenario>,... drops degraded reps
analyze:
	$(if $(RUN),,$(error usage: make analyze RUN="<run_id> ..." [OUT=<dir>] [EXCLUDE=<scenario>,...]))
	mkdir -p results
	$(COMPOSE) run --rm --build analysis $(addprefix /results/,$(RUN)) \
		$(if $(OUT),--out=/results/$(OUT)) $(if $(EXCLUDE),--exclude-degraded=$(EXCLUDE))

test-py:
	$(COMPOSE) run --rm --build --entrypoint python analysis -m pytest

down:
	$(COMPOSE) --profile load down -v --remove-orphans
