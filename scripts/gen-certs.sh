#!/usr/bin/env bash
# Generates a CA and a server certificate (ECDSA P-256) in certs/.
set -euo pipefail

dir=${1:-certs}
if [[ -f "$dir/server.crt" ]]; then
  echo "certs already exist in $dir"
  exit 0
fi
mkdir -p "$dir"
cd "$dir"

cat > ca.cnf <<'EOF'
[req]
distinguished_name = dn
x509_extensions = v3_ca
prompt = no
[dn]
CN = bench-ca
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF

cat > server.ext <<'EOF'
subjectAltName = DNS:server,DNS:localhost,IP:127.0.0.1
extendedKeyUsage = serverAuth
keyUsage = critical,digitalSignature
authorityKeyIdentifier = keyid
EOF

openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out ca.key
openssl req -x509 -new -key ca.key -config ca.cnf -days 3650 -out ca.crt
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out server.key
openssl req -new -key server.key -subj /CN=server -out server.csr
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 825 -extfile server.ext -out server.crt
rm -f ca.cnf server.ext server.csr ca.srl
openssl verify -CAfile ca.crt server.crt
