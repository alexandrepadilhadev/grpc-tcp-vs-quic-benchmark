SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

UV ?= uv
PROTO_DIR := proto
GEN_DIR := src/gen

.PHONY: help install lock lint fmt typecheck test check proto certs clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

install: ## Create .venv and install all dependencies (creates uv.lock if missing)
	$(UV) sync

lock: ## Update uv.lock
	$(UV) lock

lint: ## Lint and check formatting
	$(UV) run ruff check .
	$(UV) run ruff format --check .

fmt: ## Auto-fix lint issues and format code
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

typecheck: ## Static type check (mypy strict)
	$(UV) run mypy

test: ## Run tests that do not need Docker
	$(UV) run pytest -m "not docker"

check: lint typecheck test ## Run lint, typecheck and tests

proto: ## Generate protobuf code into src/gen
	@mkdir -p $(GEN_DIR)
	@files="$$(find $(PROTO_DIR) -name '*.proto' 2>/dev/null || true)"; \
	if [[ -z "$$files" ]]; then echo "No .proto files found in $(PROTO_DIR)/"; exit 0; fi; \
	$(UV) run python -m grpc_tools.protoc -I $(PROTO_DIR) \
		--python_out=$(GEN_DIR) --pyi_out=$(GEN_DIR) $$files; \
	echo "Generated code in $(GEN_DIR)/"

certs: ## Generate local CA and server certificate (use FORCE=1 to regenerate)
	bash deploy/certs/gen-certs.sh $(if $(filter 1,$(FORCE)),--force,)

clean: ## Remove caches and generated code (keeps .venv)
	rm -rf .pytest_cache .mypy_cache .ruff_cache $(GEN_DIR)
	find . -type d -name __pycache__ -not -path './.venv/*' -prune -exec rm -rf {} +
