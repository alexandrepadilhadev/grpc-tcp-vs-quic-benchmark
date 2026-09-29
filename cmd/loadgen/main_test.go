package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/bench/v1/benchv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/benchsvc"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/loadgen"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport/transporttest"
)

var discard = slog.New(slog.DiscardHandler)

// startBench serves BenchService in-process and returns its address and CA file.
func startBench(t *testing.T, tr transport.Transport) (addr, ca string) {
	t.Helper()
	cert, key, ca := transporttest.NewCertFiles(t)
	cfg := transport.DefaultConfig()
	cfg.Transport, cfg.Addr, cfg.CertFile, cfg.KeyFile = tr, "127.0.0.1:0", cert, key
	mux := http.NewServeMux()
	mux.Handle(benchv1connect.NewBenchServiceHandler(benchsvc.New(), connect.WithCompression("gzip", nil, nil)))
	srv, err := transport.Listen(cfg, mux, discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return srv.Addr().String(), ca
}

func TestSmoke(t *testing.T) {
	for _, tr := range []transport.Transport{transport.H2, transport.H3} {
		t.Run(tr.String(), func(t *testing.T) {
			addr, ca := startBench(t, tr)
			out := filepath.Join(t.TempDir(), "run", "requests.csv")
			err := run(context.Background(), []string{
				"--transport=" + tr.String(), "--target=" + addr, "--tls-ca=" + ca,
				"--requests=1000", "--warmup=0", "--concurrency=50", "--strict", "--out=" + out,
			}, discard)
			if err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(out)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(string(b)), "\n")
			if lines[0] != loadgen.Header || len(lines) != 1001 {
				t.Fatalf("header=%q rows=%d", lines[0], len(lines)-1)
			}
			for _, l := range lines[1:] {
				if !strings.HasSuffix(l, ",1024,1024,measure") || !strings.Contains(l, ",ok,") {
					t.Fatalf("bad row %q", l)
				}
			}
			var m loadgen.Meta
			b, err = os.ReadFile(filepath.Join(filepath.Dir(out), "meta.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatal(err)
			}
			if m.Dials != 1 || m.OK != 1000 || m.Transport != tr.String() || m.HandshakeUs <= 0 {
				t.Fatalf("meta = %+v", m)
			}
		})
	}
}

func TestTransportMismatch(t *testing.T) {
	addr, ca := startBench(t, transport.H2)
	out := filepath.Join(t.TempDir(), "requests.csv")
	start := time.Now()
	err := run(context.Background(), []string{
		"--transport=h3", "--target=" + addr, "--tls-ca=" + ca, "--handshake-timeout=500ms",
		"--requests=10", "--out=" + out,
	}, discard)
	if err == nil {
		t.Fatal("run succeeded against the wrong transport")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("took %v", d)
	}
	if b, _ := os.ReadFile(out); strings.Count(string(b), "\n") > 1 {
		t.Fatalf("CSV has data rows: %q", b)
	}
}

func TestBadOut(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	err := run(context.Background(), []string{
		"--target=127.0.0.1:1", "--tls-ca=ca.crt", "--requests=10", "--out=" + filepath.Join(file, "requests.csv"),
	}, discard)
	if err == nil || !strings.Contains(err.Error(), "out") {
		t.Fatalf("err = %v; want out error before any RPC", err)
	}
}

func TestInterrupted(t *testing.T) {
	addr, ca := startBench(t, transport.H2)
	out := filepath.Join(t.TempDir(), "requests.csv")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := run(ctx, []string{
		"--target=" + addr, "--tls-ca=" + ca, "--duration=10s", "--warmup=0", "--out=" + out,
	}, discard)
	if err == nil || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("err = %v; want interrupted", err)
	}
	var m loadgen.Meta
	b, err := os.ReadFile(filepath.Join(filepath.Dir(out), "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &m); err != nil || !m.Interrupted {
		t.Fatalf("meta interrupted=%v err=%v", m.Interrupted, err)
	}
}
