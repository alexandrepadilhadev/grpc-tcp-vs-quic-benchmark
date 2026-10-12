// Command risk scores payment events from RabbitMQ and publishes the results.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/broker"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/risk"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/service"
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
	f := service.NewFlags("risk")
	c := service.RegisterCommon(f, 0)
	var amqpURL string
	f.String(&amqpURL, "amqp-url", "AMQP_URL", "", "RabbitMQ URL")
	if err := f.Parse(args); err != nil {
		return err
	}
	if amqpURL == "" {
		return errors.New("amqp-url/AMQP_URL is required")
	}

	log := service.NewLogger(c.Level)
	b, err := broker.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("broker: %w", err)
	}
	defer b.Close()
	in, err := b.Consume(broker.EventsQueue, 100)
	if err != nil {
		return fmt.Errorf("broker: %w", err)
	}
	log.Info("risk started", "version", version)
	var consumed, published atomic.Int64
	err = risk.Run(ctx, in, func(ctx context.Context, r *paymentv1.RiskResult) error {
		return b.Publish(ctx, broker.ResultsQueue, r)
	}, &consumed, &published, log)
	log.Info("risk stopped", "error", err)
	return errors.Join(err, service.WriteReport(c.RepDir, service.Report{
		Service: "risk", Version: version,
		Counters: map[string]int64{"events_consumed": consumed.Load(), "results_published": published.Load()},
	}))
}
