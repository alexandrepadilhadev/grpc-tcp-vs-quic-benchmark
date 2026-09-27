package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

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
			srv, err := transport.Listen(cfg, newMux(), slog.New(slog.DiscardHandler))
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
