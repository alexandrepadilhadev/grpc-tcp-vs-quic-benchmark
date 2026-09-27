#!/usr/bin/env bash
# Generate a local CA and a server certificate (ECDSA P-256) shared by the h2 and h3 transports.
# Usage: deploy/certs/gen-certs.sh [--force]
# Env: OUT_DIR, CERT_DAYS, SERVER_SANS
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly OUT_DIR="${OUT_DIR:-${SCRIPT_DIR}/out}"
readonly CERT_DAYS="${CERT_DAYS:-365}"
readonly SERVER_SANS="${SERVER_SANS:-DNS:localhost,IP:127.0.0.1,IP:::1,DNS:echo-server,DNS:gateway,DNS:core,DNS:ledger}"
readonly CURVE="prime256v1"

force=0
work_dir=""

log() { printf '[certs] %s\n' "$*"; }
die() { printf '[certs] ERROR: %s\n' "$*" >&2; exit 1; }

usage() {
    cat <<EOF
Usage: $(basename "$0") [--force]

Generates into ${OUT_DIR}:
  ca.crt, ca.key          local certificate authority
  server.crt, server.key  server certificate signed by the CA

Options:
  --force   overwrite existing files
  -h        show this help

Environment:
  OUT_DIR      output directory (default: ${SCRIPT_DIR}/out)
  CERT_DAYS    validity in days (default: 365)
  SERVER_SANS  subjectAltName list (default: ${SERVER_SANS})
EOF
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --force) force=1 ;;
            -h | --help) usage; exit 0 ;;
            *) usage >&2; die "unknown argument: $1" ;;
        esac
        shift
    done
}

validate() {
    command -v openssl >/dev/null 2>&1 || die "openssl not found"
    [[ "${CERT_DAYS}" =~ ^[1-9][0-9]*$ ]] || die "CERT_DAYS must be a positive integer"
    [[ -n "${SERVER_SANS}" ]] || die "SERVER_SANS must not be empty"
}

generate() {
    work_dir="$(mktemp -d)"
    trap 'rm -rf "${work_dir}"' EXIT
    local work="${work_dir}"

    log "creating CA"
    openssl req -x509 -newkey ec -pkeyopt "ec_paramgen_curve:${CURVE}" -nodes \
        -keyout "${work}/ca.key" -out "${work}/ca.crt" -days "${CERT_DAYS}" -sha256 \
        -subj "/CN=grpc-bench-ca" \
        -addext "basicConstraints=critical,CA:TRUE" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

    log "creating server key and CSR"
    openssl req -new -newkey ec -pkeyopt "ec_paramgen_curve:${CURVE}" -nodes \
        -keyout "${work}/server.key" -out "${work}/server.csr" \
        -subj "/CN=grpc-bench-server" 2>/dev/null

    cat >"${work}/server.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth
subjectAltName=${SERVER_SANS}
EOF

    log "signing server certificate"
    openssl x509 -req -in "${work}/server.csr" \
        -CA "${work}/ca.crt" -CAkey "${work}/ca.key" \
        -set_serial "0x$(openssl rand -hex 16)" -days "${CERT_DAYS}" -sha256 \
        -extfile "${work}/server.ext" -out "${work}/server.crt" 2>/dev/null

    openssl verify -CAfile "${work}/ca.crt" "${work}/server.crt" >/dev/null \
        || die "generated server certificate failed verification"

    mkdir -p "${OUT_DIR}"
    install -m 0644 "${work}/ca.crt" "${work}/server.crt" "${OUT_DIR}/"
    install -m 0600 "${work}/ca.key" "${work}/server.key" "${OUT_DIR}/"
}

main() {
    parse_args "$@"
    validate

    if [[ -f "${OUT_DIR}/server.crt" && ${force} -eq 0 ]]; then
        log "certificates already exist in ${OUT_DIR} (use --force to regenerate)"
        exit 0
    fi

    generate
    log "done: ${OUT_DIR}"
    log "SANs: ${SERVER_SANS}"
}

main "$@"
