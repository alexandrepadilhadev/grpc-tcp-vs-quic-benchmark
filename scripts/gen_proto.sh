#!/bin/bash

# Generate Python gRPC stubs from protobuf definitions
# This script compiles payment.proto into Python modules

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

PROTO_DIR="$PROJECT_ROOT/api/proto"
GENERATED_DIR="$PROJECT_ROOT/src/generated"

echo "Generating gRPC stubs from protobuf definitions..."
echo "Proto directory: $PROTO_DIR"
echo "Output directory: $GENERATED_DIR"

python -m grpc_tools.protoc \
  -I"$PROTO_DIR" \
  --python_out="$GENERATED_DIR" \
  --grpc_python_out="$GENERATED_DIR" \
  "$PROTO_DIR/payment.proto"

# Keep generated imports valid when loaded as src.generated modules.
sed -i 's/^import payment_pb2 as payment__pb2$/from . import payment_pb2 as payment__pb2/' \
  "$GENERATED_DIR/payment_pb2_grpc.py"

echo "Stub generation completed successfully."
echo "Generated files:"
ls -la "$GENERATED_DIR"/*.py 2>/dev/null || echo "  (no .py files generated yet)"
