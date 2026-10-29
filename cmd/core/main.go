// Command core serves CoreService over h2 or h3: it settles payments in the
// Ledger and exchanges events with the Risk service over AMQP.
// "core healthcheck" probes a running core over the same transport.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1/paymentv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/broker"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/core"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/service"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
)

var version = "dev" // set with -ldflags "-X main.version=..."

// eventQueue bounds the in-memory queue between the RPC path and the publisher.
const eventQueue = 10000

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
	f := service.NewFlags("core")
	c := service.RegisterCommon(f, transport.RoleServer|transport.RoleClient)
	var ledgerAddr, amqpURL string
	var acquirerDelay time.Duration
	f.String(&ledgerAddr, "ledger-addr", "LEDGER_ADDR", "ledger:8443", "ledger host:port")
	f.String(&amqpURL, "amqp-url", "AMQP_URL", "", "RabbitMQ URL")
	f.Duration(&acquirerDelay, "acquirer-delay", "ACQUIRER_DELAY", 0, "stub acquirer latency")
	if err := f.Parse(args); err != nil {
		return err
	}
	if probe {
		return service.Healthcheck(ctx, c.Transport)
	}
	errs := []error{c.Transport.Validate(transport.RoleServer | transport.RoleClient)}
	if amqpURL == "" {
		errs = append(errs, errors.New("amqp-url/AMQP_URL is required"))
	}
	if acquirerDelay < 0 {
		errs = append(errs, errors.New("acquirer-delay/ACQUIRER_DELAY must be >= 0"))
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}

	log := service.NewLogger(c.Level)
	b, err := broker.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("broker: %w", err)
	}
	defer b.Close()
	results, err := b.Consume(broker.ResultsQueue, 100)
	if err != nil {
		return fmt.Errorf("broker: %w", err)
	}
	cli, opts, err := service.NewClient(c.Transport, log)
	if err != nil {
		return err
	}
	defer cli.Close()
	events := core.NewEvents(eventQueue)
	ledger := paymentv1connect.NewLedgerServiceClient(cli, "https://"+ledgerAddr, opts...)
	path, h := paymentv1connect.NewCoreServiceHandler(core.New(ledger, events, acquirerDelay, log), service.HandlerOptions(log)...)
	srv, err := service.Listen("core", version, c.Transport, log, path, h)
	if err != nil {
		return err
	}

	// the server, the publisher and the results consumer stop together:
	// the first error (e.g. a lost broker) cancels the others
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var published, received atomic.Int64
	var wg sync.WaitGroup
	errc := make(chan error, 3)
	for _, task := range []func() error{
		func() error { return srv.Serve(ctx) },
		func() error {
			return core.RunPublisher(ctx, events, func(ctx context.Context, e *paymentv1.PaymentEvent) error {
				return b.Publish(ctx, broker.EventsQueue, e)
			}, &published, log)
		},
		func() error { return core.RunResults(ctx, results, &received, log) },
	} {
		wg.Go(func() {
			if err := task(); err != nil {
				errc <- err
				cancel()
			}
		})
	}
	wg.Wait()
	close(errc)
	for e := range errc {
		err = errors.Join(err, e)
	}
	log.Info("core stopped", "error", err)
	return errors.Join(err, service.WriteReport(c.RepDir, service.Report{
		Service: "core", Version: version, Transport: c.Transport.Transport.String(),
		Dials: map[string]int64{ledgerAddr: cli.Dials()},
		Counters: map[string]int64{
			"events_published": published.Load(),
			"events_dropped":   events.Dropped(),
			"risk_results":     received.Load(),
		},
	}))
}
