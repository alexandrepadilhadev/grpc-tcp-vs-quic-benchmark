// Command gateway serves PaymentService over h2 or h3, the entry point of
// the payment system; it hands each payment to the Core.
// "gateway healthcheck" probes a running gateway over the same transport.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1/paymentv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/gateway"
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
	f := service.NewFlags("gateway")
	c := service.RegisterCommon(f, transport.RoleServer|transport.RoleClient)
	var coreAddr string
	f.String(&coreAddr, "core-addr", "CORE_ADDR", "core:8443", "core host:port")
	if err := f.Parse(args); err != nil {
		return err
	}
	if probe {
		return service.Healthcheck(ctx, c.Transport)
	}
	if err := c.Transport.Validate(transport.RoleServer | transport.RoleClient); err != nil {
		return err
	}

	log := service.NewLogger(c.Level)
	cli, opts, err := service.NewClient(c.Transport, log)
	if err != nil {
		return err
	}
	defer cli.Close()
	core := paymentv1connect.NewCoreServiceClient(cli, "https://"+coreAddr, opts...)
	gw := gateway.New(core, log)
	path, h := paymentv1connect.NewPaymentServiceHandler(gw, service.HandlerOptions(log)...)
	srv, err := service.Listen("gateway", version, c.Transport, log, path, h)
	if err != nil {
		return err
	}
	err = srv.Serve(ctx)
	log.Info("gateway stopped", "error", err)
	return errors.Join(err, service.WriteReport(c.RepDir, service.Report{
		Service: "gateway", Version: version, Transport: c.Transport.Transport.String(),
		Dials: map[string]int64{coreAddr: cli.Dials()}, Peers: gw.Peers()}))
}
