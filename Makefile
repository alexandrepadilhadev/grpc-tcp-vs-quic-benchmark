COMPOSE   := docker compose -f deploy/compose.yaml
VERSION   ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)
TRANSPORT ?= h2
export VERSION TRANSPORT

.PHONY: certs build test smoke down

certs:
	bash scripts/gen-certs.sh

build:
	$(COMPOSE) --profile load build

test:
	docker run --rm -v "$(CURDIR)":/src -v bench-gomod:/go/pkg/mod -w /src golang:1.27.1-trixie go test -race ./...

smoke:
	$(COMPOSE) up -d --wait server
	$(COMPOSE) run --rm loadgen --target=server:8443 --requests=1000 --warmup=0 --strict \
		--out=/results/smoke/$(TRANSPORT)/requests.csv; rc=$$?; $(COMPOSE) down; exit $$rc

down:
	$(COMPOSE) --profile load down --remove-orphans
