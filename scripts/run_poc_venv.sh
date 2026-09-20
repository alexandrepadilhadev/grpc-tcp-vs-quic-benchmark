#!/bin/bash

# Run the TCP and QUIC proof-of-concept workflows and validate their logs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CERT_DIR="$PROJECT_ROOT/certs"
LOG_DIR="$PROJECT_ROOT/logs"
CERT_DIR_NATIVE="$(cygpath -w "$CERT_DIR")"
LOG_DIR_NATIVE="$(cygpath -w "$LOG_DIR")"
SERVER_LOG="$LOG_DIR/poc-server-process.log"

cd "$PROJECT_ROOT"

if [[ -f "$PROJECT_ROOT/.env" ]]; then
  set -a
  source "$PROJECT_ROOT/.env"
  set +a
fi
export PAYMENT_FAILURE_PROBABILITY=0.25

# ==============================================================================
# Venv setup
# ==============================================================================
VENV_DIR="$PROJECT_ROOT/venv"

if [[ -f "$VENV_DIR/Scripts/python.exe" ]]; then
  #  Windows (Git Bash / Cygwin / MSYS2)
  PYTHON_BIN="$VENV_DIR/Scripts/python.exe"
elif [[ -f "$VENV_DIR/bin/python" ]]; then
  #  Linux / macOS / WSL
  PYTHON_BIN="$VENV_DIR/bin/python"
else
  echo "Erro: Ambiente virtual não encontrado em $VENV_DIR" >&2
  echo "Crie o ambiente com: python -m venv .venv && source .venv/Scripts/activate (ou bin/activate)" >&2
  exit 1
fi
# ==============================================================================

bash "$SCRIPT_DIR/gen_certs.sh" "$CERT_DIR"
mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR/server.log" "$LOG_DIR/client.log" "$SERVER_LOG"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

run_mode() {
  local transport="$1"
  local port="$2"
  local transport_label="${transport^^}"
  local ready_marker="$transport_label server listening"

  echo "Running $transport mode on port $port..."
  export APP_TRANSPORT="$transport"
  export SERVER_HOST="127.0.0.1"
  export SERVER_PORT="$port"
  export TLS_CERT_PATH="$CERT_DIR_NATIVE\\localhost.pem"
  export TLS_KEY_PATH="$CERT_DIR_NATIVE\\localhost_key.pem"
  export LOG_DIR="$LOG_DIR_NATIVE"
  export LOG_LEVEL="INFO"
  export PAYMENT_FAILURE_PROBABILITY=0.75
  SSLKEYLOG_PATH="$LOG_DIR/sslkeys.log"
  export SSLKEYLOGFILE="$(cygpath -w "$SSLKEYLOG_PATH")"
  export CLIENT_RUNS=1

  #optional log options
  #   export GRPC_VERBOSITY=DEBUG
  #   export GRPC_TRACE=http,call_error,connectivity_state

  "$PYTHON_BIN" -m src.cmd.server >"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!

  for attempt in $(seq 1 30); do
    if grep -q "$ready_marker" "$LOG_DIR/server.log" 2>/dev/null; then
      break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      cat "$SERVER_LOG" >&2 || true
      return 1
    fi
    sleep 1
  done

  if ! grep -q "$ready_marker" "$LOG_DIR/server.log" 2>/dev/null; then
    echo "Server did not become ready for $transport" >&2
    cat "$SERVER_LOG" >&2 || true
    return 1
  fi

  echo "Executing client $CLIENT_RUNS time(s) for $transport..."
  for i in $(seq 1 "$CLIENT_RUNS"); do
    echo "  -> Client run #$i of $CLIENT_RUNS ($transport)"
    "$PYTHON_BIN" -m src.cmd.client
  done
  cleanup
  unset SERVER_PID
}

run_mode tcp 50051
run_mode quic 50052
# Validate logs for both transports
"$PYTHON_BIN" - "$LOG_DIR" <<'PY'
import json
import sys
from pathlib import Path

log_dir = Path(sys.argv[1])
required = {
    "timestamp",
    "level",
    "transport",
    "rpc_method",
    "duration_ms",
    "status_code",
    "payload_size_bytes",
}

rpc_records = []
for log_name in ("server.log", "client.log"):
    path = log_dir / log_name
    if not path.is_file():
        raise SystemExit(f"Missing log file: {path}")
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        record = json.loads(line)
        if "rpc_method" in record:
            missing = required - record.keys()
            if missing:
                raise SystemExit(
                    f"{path}:{line_number} missing fields: {sorted(missing)}"
                )
            rpc_records.append(record)

if not rpc_records:
    raise SystemExit("No RPC records found in server.log or client.log")

transports = {record["transport"] for record in rpc_records}
if not {"TCP_HTTP2", "QUIC_HTTP3"}.issubset(transports):
    raise SystemExit(f"Missing transport records; found: {sorted(transports)}")

print(f"Validated {len(rpc_records)} RPC log records")
print("POC completed successfully for TCP and QUIC")
PY