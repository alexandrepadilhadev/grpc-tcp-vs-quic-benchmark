# gRPC TCP vs QUIC Benchmark

A distributed mock payment processing ecosystem designed to benchmark and compare the performance, latency, and resilience of **gRPC over HTTP/2 (TCP)** versus **gRPC over HTTP/3 (QUIC)** under simulated network stress conditions such as packet loss, jitter, and connection migration.

## Documentation

- [`doc/CONTEXT.md`](doc/CONTEXT.md) — scope, architecture, conventions and decision log.
- [`doc/plan/POC_PLAN.md`](doc/plan/POC_PLAN.md) — proof-of-concept plan.
- `doc/TCC_AlexndrePadilha .pdf` — thesis proposal.

## Requirements

- Docker host: **Ubuntu 24.04 or 26.04 LTS VM** (Hyper-V or similar) with Docker Engine.
  Docker Desktop with the WSL2 backend is not supported: its kernel lacks `sch_netem`.
- Python 3.13 and [`uv`](https://docs.astral.sh/uv/) (installed by the setup script).

## Quickstart (inside the VM)

```bash
git clone <repo-url> grpc-tcp-vs-quic-benchmark
cd grpc-tcp-vs-quic-benchmark

bash scripts/setup-vm.sh          # Docker, netem, UDP buffers, uv, Python
# log out and back in (docker group)
bash scripts/setup-vm.sh --check-only

make install                      # create .venv and uv.lock
make check                        # lint + typecheck + tests
make certs                        # local CA and server certificate
```

## Make targets

| Target | Description |
|---|---|
| `make install` | Create `.venv` and install all dependencies |
| `make lock` | Update `uv.lock` |
| `make lint` / `make fmt` | Check / fix lint and formatting (ruff) |
| `make typecheck` | mypy strict |
| `make test` | Tests that do not need Docker |
| `make check` | lint + typecheck + test |
| `make proto` | Generate protobuf code into `src/gen/` |
| `make certs` | Generate TLS material into `deploy/certs/out/` (`FORCE=1` to regenerate) |
| `make clean` | Remove caches and generated code |

## License

[MIT](LICENSE)
