// Command ledger serves LedgerService over h2 or h3, backed by PostgreSQL.
// "ledger healthcheck" probes a running ledger over the same transport.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1/paymentv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/ledger"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/service"
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
	f := service.NewFlags("ledger")
	c := service.RegisterCommon(f, transport.RoleServer|transport.RoleClient)
	var dbURL string
	var maxConns int
	f.String(&dbURL, "db-url", "DATABASE_URL", "", "PostgreSQL URL")
	f.Int(&maxConns, "db-max-conns", "DB_MAX_CONNS", 8, "max pool connections")
	if err := f.Parse(args); err != nil {
		return err
	}
	if probe {
		return service.Healthcheck(ctx, c.Transport)
	}
	errs := []error{c.Transport.Validate(transport.RoleServer)}
	if dbURL == "" {
		errs = append(errs, errors.New("db-url/DATABASE_URL is required"))
	}
	if maxConns < 1 {
		errs = append(errs, errors.New("db-max-conns/DB_MAX_CONNS must be > 0"))
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}

	log := service.NewLogger(c.Level)
	store, err := ledger.NewPgStore(ctx, dbURL, int32(maxConns))
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer store.Close()
	path, h := paymentv1connect.NewLedgerServiceHandler(ledger.New(store, log), service.HandlerOptions(log)...)
	srv, err := service.Listen("ledger", version, c.Transport, log, path, h)
	if err != nil {
		return err
	}
	err = srv.Serve(ctx)
	log.Info("ledger stopped", "error", err)
	return errors.Join(err, service.WriteReport(c.RepDir, service.Report{
		Service: "ledger", Version: version, Transport: c.Transport.Transport.String(), Dials: map[string]int64{}}))
}
