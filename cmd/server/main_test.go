package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	benchv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/bench/v1"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/bench/v1/benchv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport/transporttest"
)

func testConfig(t *testing.T, tr transport.Transport) transport.Config {
	cert, key, ca := transporttest.NewCertFiles(t)
	cfg := transport.DefaultConfig()
	cfg.Transport, cfg.Addr, cfg.CertFile, cfg.KeyFile, cfg.CAFile = tr, "127.0.0.1:0", cert, key, ca
	return cfg
}

func TestHealthcheck(t *testing.T) {
	for _, tr := range []transport.Transport{transport.H2, transport.H3} {
		t.Run(tr.String(), func(t *testing.T) {
			cfg := testConfig(t, tr)
			srv, err := transport.Listen(cfg, newMux(slog.New(slog.DiscardHandler)), slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- srv.Serve(ctx) }()
			defer func() { cancel(); <-done }()

			cfg.Addr = srv.Addr().String()
			if err := healthcheck(context.Background(), cfg); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHealthcheckDown(t *testing.T) {
	cfg := testConfig(t, transport.H3)
	cfg.Addr, cfg.HandshakeTimeout = "127.0.0.1:1", 300*time.Millisecond
	start := time.Now()
	if err := healthcheck(context.Background(), cfg); err == nil {
		t.Fatal("healthcheck succeeded with no server")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("took %v", d)
	}
}

func TestMuxLogsRPCs(t *testing.T) {
	var buf bytes.Buffer
	srv := httptest.NewServer(newMux(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))))
	defer srv.Close()

	cli := benchv1connect.NewBenchServiceClient(srv.Client(), srv.URL)
	if _, err := cli.Unary(context.Background(), connect.NewRequest(&benchv1.UnaryRequest{ResponseSize: 8})); err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if n := strings.Count(buf.String(), `"msg":"rpc"`); n != 1 {
		t.Fatalf("rpc lines = %d; want 1 (healthz not logged): %s", n, buf.String())
	}
}

func TestInvalidLogLevel(t *testing.T) {
	cert, key, _ := transporttest.NewCertFiles(t)
	args := []string{"--addr=127.0.0.1:0", "--tls-cert=" + cert, "--tls-key=" + key}
	t.Run("env", func(t *testing.T) {
		t.Setenv("LOG_LEVEL", "verbose")
		if err := run(context.Background(), args); err == nil || !strings.Contains(err.Error(), "LOG_LEVEL") {
			t.Fatalf("err = %v; want LOG_LEVEL error", err)
		}
	})
	t.Run("flag", func(t *testing.T) {
		if err := run(context.Background(), append(args, "--log-level=verbose")); err == nil || !strings.Contains(err.Error(), "log-level") {
			t.Fatalf("err = %v; want log-level error", err)
		}
	})
}
