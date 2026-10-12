// Package risk scores payment events off the critical path and publishes
// the result back to the Core.
package risk

import (
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
)

// Score is a deterministic risk score in 0..99: FNV-1a 32 of paymentID mod 100.
func Score(paymentID string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(paymentID))
	return h.Sum32() % 100
}

// Run consumes PaymentEvents, publishes one RiskResult each and stops when ctx
// is done; a closed channel before that means the broker connection was lost.
func Run(ctx context.Context, in <-chan []byte, publish func(context.Context, *paymentv1.RiskResult) error, consumed, published *atomic.Int64, log *slog.Logger) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case b, ok := <-in:
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return errors.New("payment events consumer closed")
			}
			var e paymentv1.PaymentEvent
			if err := proto.Unmarshal(b, &e); err != nil {
				log.Error("bad payment event", "error", err.Error())
				continue
			}
			consumed.Add(1)
			id := e.GetPaymentId()
			debug := log.Enabled(ctx, slog.LevelDebug) // args are built only when debug is on
			score := Score(id)
			if debug {
				log.DebugContext(ctx, "event received", "payment_id", id)
				log.DebugContext(ctx, "risk scored", "payment_id", id, "score", score)
			}
			if err := publish(ctx, &paymentv1.RiskResult{PaymentId: id, Score: score}); err != nil {
				if ctx.Err() != nil { // shutting down
					return nil
				}
				return err
			}
			published.Add(1)
			if debug {
				log.DebugContext(ctx, "result published", "payment_id", id)
			}
		}
	}
}
