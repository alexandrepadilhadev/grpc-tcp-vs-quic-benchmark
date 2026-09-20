#!/bin/bash

# Generate a self-signed localhost certificate for TCP and QUIC.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CERT_DIR="${1:-$PROJECT_ROOT/certs}"
CERT_PATH="$CERT_DIR/localhost.pem"
KEY_PATH="$CERT_DIR/localhost_key.pem"
OPENSSL_CERT_PATH="$(cygpath -w "$CERT_PATH")"
OPENSSL_KEY_PATH="$(cygpath -w "$KEY_PATH")"

mkdir -p "$CERT_DIR"

if [[ -f "$CERT_PATH" && -f "$KEY_PATH" ]]; then
  echo "TLS certificate already exists: $CERT_PATH"
  exit 0
fi

MSYS_NO_PATHCONV=1 openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$OPENSSL_KEY_PATH" \
  -out "$OPENSSL_CERT_PATH" \
  -days 365 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

chmod 600 "$KEY_PATH"
echo "Generated TLS certificate: $CERT_PATH"
echo "Generated TLS private key: $KEY_PATH"
