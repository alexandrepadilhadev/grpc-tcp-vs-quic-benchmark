package core

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
)

// RunPublisher drains ev into publish until ctx is done; it returns the first
// publish error, so a lost broker stops the service instead of counting zeros.
func RunPublisher(ctx context.Context, ev *Events, publish func(context.Context, *paymentv1.PaymentEvent) error, published *atomic.Int64, log *slog.Logger) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-ev.C():
			if err := publish(ctx, e); err != nil {
				if ctx.Err() != nil { // shutting down
					return nil
				}
				return err
			}
			published.Add(1)
			if log.Enabled(ctx, slog.LevelDebug) {
				log.DebugContext(ctx, "event published", "payment_id", e.GetPaymentId())
			}
		}
	}
}

// RunResults counts RiskResults until ctx is done; a closed channel before
// that means the broker connection was lost.
func RunResults(ctx context.Context, in <-chan []byte, results *atomic.Int64, log *slog.Logger) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case b, ok := <-in:
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return errors.New("risk results consumer closed")
			}
			var r paymentv1.RiskResult
			if err := proto.Unmarshal(b, &r); err != nil {
				log.Error("bad risk result", "error", err.Error())
				continue
			}
			results.Add(1)
			if log.Enabled(ctx, slog.LevelDebug) {
				log.DebugContext(ctx, "risk result received", "payment_id", r.GetPaymentId(), "score", r.GetScore())
			}
		}
	}
}
