// Command server serves BenchService over h2 or h3.
// "server healthcheck" probes a running server over the same transport.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"connectrpc.com/connect"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/bench/v1/benchv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/benchsvc"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
)

var version = "dev" // set with -ldflags "-X main.version=..."

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	probe := len(args) > 0 && args[0] == "healthcheck"
	if probe {
		args = args[1:]
	}
	cfg := transport.DefaultConfig()
	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	cfg.RegisterFlags(fs, transport.RoleServer|transport.RoleClient)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if probe {
		return healthcheck(ctx, cfg)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	srv, err := transport.Listen(cfg, newMux(), log)
	if err != nil {
		return err
	}
	log.Info("server started",
		"version", version,
		"transport", cfg.Transport,
		"addr", srv.Addr().String(),
		"max_streams", cfg.MaxConcurrentStreams,
		"handshake_timeout", cfg.HandshakeTimeout.String(),
		"idle_timeout", cfg.IdleTimeout.String())
	err = srv.Serve(ctx)
	log.Info("server stopped", "error", err)
	return err
}

func newMux() http.Handler {
	mux := http.NewServeMux()
	// nil constructors unregister gzip: no compression in the measured path
	mux.Handle(benchv1connect.NewBenchServiceHandler(benchsvc.New(), connect.WithCompression("gzip", nil, nil)))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	return mux
}

// healthcheck GETs /healthz on cfg.Addr over the configured transport.
func healthcheck(ctx context.Context, cfg transport.Config) error {
	cli, err := transport.NewClient(cfg)
	if err != nil {
		return err
	}
	defer cli.Close()

	host, port, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return err
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "localhost"
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.HandshakeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+net.JoinHostPort(host, port)+"/healthz", nil)
	if err != nil {
		return err
	}
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthz: %s", resp.Status)
	}
	return transport.VerifyServedProto(cfg.Transport, resp.Header)
}
