# gRPC TCP vs QUIC Benchmark

## Poc summary

This project is a lightweight proof of concept for comparing the same payment service over **gRPC over HTTP/2 on TCP** and **gRPC over HTTP/3 on QUIC**. It keeps the protobuf contract and business logic fixed while switching the transport layer, so the behavior and performance can be evaluated under the same workload. The project also records structured JSON logs for both transports, making the comparison easy to validate and reproduce.

## How to run

1. Create and activate a virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   # Windows PowerShell: .\.venv\Scripts\Activate.ps1
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Generate local TLS certificates:

   ```bash
   bash scripts/gen_certs.sh
   ```

4. Run the full POC validation script:

   ```bash
   bash scripts/run_poc.sh
   ```

This script runs the TCP and QUIC flows, validates the generated log records, and confirms that both transports complete successfully.