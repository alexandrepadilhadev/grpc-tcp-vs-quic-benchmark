FROM golang:1.27.1-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/ ./cmd/...

FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 && rm -rf /var/lib/apt/lists/*
COPY --chmod=755 deploy/netem.sh /usr/local/bin/netem.sh

FROM runtime AS server
COPY --from=build /out/server /usr/local/bin/server
ENTRYPOINT ["/usr/local/bin/netem.sh", "run", "/usr/local/bin/server"]

FROM runtime AS loadgen
COPY --from=build /out/loadgen /usr/local/bin/loadgen
ENTRYPOINT ["/usr/local/bin/netem.sh", "run", "/usr/local/bin/loadgen"]

FROM python:3.13-slim-trixie AS analysis
COPY analysis/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY analysis /app/analysis
WORKDIR /app/analysis
ENTRYPOINT ["python", "analyze.py"]
