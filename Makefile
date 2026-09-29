COMPOSE   := docker compose -f deploy/compose.yaml
VERSION   ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)
TRANSPORT ?= h2
REPS      ?= 5
export VERSION TRANSPORT

.PHONY: certs build test test-py smoke run-all analyze down

certs:
	bash scripts/gen-certs.sh

build:
	$(COMPOSE) --profile load build

test:
	docker run --rm -v "$(CURDIR)":/src -v bench-gomod:/go/pkg/mod -w /src golang:1.27.1-trixie go test -race ./...

smoke:
	mkdir -p results
	$(COMPOSE) up -d --wait server
	$(COMPOSE) run --rm loadgen --target=server:8443 --requests=1000 --warmup=0 --strict \
		--out=/results/smoke/$(TRANSPORT)/requests.csv; rc=$$?; $(COMPOSE) down; exit $$rc

run-all: build
	mkdir -p results
	REPS=$(REPS) bash scripts/run.sh

analyze:
	$(if $(RUN),,$(error usage: make analyze RUN=<run_id>))
	mkdir -p results
	$(COMPOSE) run --rm --build analysis /results/$(RUN)

test-py:
	$(COMPOSE) run --rm --build --entrypoint python analysis -m pytest

down:
	$(COMPOSE) --profile load down --remove-orphans
