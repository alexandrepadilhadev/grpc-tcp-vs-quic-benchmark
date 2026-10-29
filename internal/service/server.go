package service

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"

	"connectrpc.com/connect"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/rpclog"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
)

// NewLogger logs JSON to stdout at level.
func NewLogger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

// HandlerOptions are the options of every service handler: no compression
// in the measured path, per-RPC lines at rpclog.Level.
func HandlerOptions(log *slog.Logger) []connect.HandlerOption {
	return []connect.HandlerOption{
		connect.WithCompression("gzip", nil, nil),
		connect.WithInterceptors(rpclog.Interceptor(log)),
	}
}

// Listen serves h at path plus GET /healthz on cfg, and logs "<name> started".
// The caller runs Serve on the returned server.
func Listen(name, version string, cfg transport.Config, log *slog.Logger, path string, h http.Handler) (*transport.Server, error) {
	mux := http.NewServeMux()
	mux.Handle(path, h)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	srv, err := transport.Listen(cfg, mux, log)
	if err != nil {
		return nil, err
	}
	log.Info(name+" started",
		"version", version,
		"transport", cfg.Transport,
		"addr", srv.Addr().String(),
		"max_streams", cfg.MaxConcurrentStreams,
		"handshake_timeout", cfg.HandshakeTimeout.String(),
		"idle_timeout", cfg.IdleTimeout.String())
	return srv, nil
}

// Healthcheck GETs /healthz on cfg.Addr (localhost) over cfg.Transport.
func Healthcheck(ctx context.Context, cfg transport.Config) error {
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
