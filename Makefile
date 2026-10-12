COMPOSE   := docker compose -f deploy/compose.yaml
VERSION   ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)
TRANSPORT ?= h2
REPS      ?= 5
export VERSION TRANSPORT

.PHONY: certs build test test-db test-py smoke run-all analyze down

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

run-all: build
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
