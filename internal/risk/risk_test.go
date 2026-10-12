package risk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
)

var discard = slog.New(slog.DiscardHandler)

func TestScore(t *testing.T) {
	if Score("p1") != Score("p1") {
		t.Fatal("Score is not deterministic")
	}
	b := make([]byte, 16)
	for range 1000 {
		_, _ = rand.Read(b)
		if s := Score(hex.EncodeToString(b)); s >= 100 {
			t.Fatalf("score %d >= 100", s)
		}
	}
}

func eventMsg(t *testing.T, id string) []byte {
	t.Helper()
	b, err := proto.Marshal(&paymentv1.PaymentEvent{PaymentId: id, AccountId: 1, AmountCents: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRiskRun(t *testing.T) {
	in := make(chan []byte, 2)
	in <- eventMsg(t, "p1")
	in <- eventMsg(t, "p2")
	got := make(chan *paymentv1.RiskResult, 2)
	var consumed, published atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, in, func(_ context.Context, r *paymentv1.RiskResult) error {
			got <- r
			return nil
		}, &consumed, &published, discard)
	}()
	for _, id := range []string{"p1", "p2"} {
		r := <-got
		if r.GetPaymentId() != id || r.GetScore() != Score(id) {
			t.Fatalf("result = %v; want %s with Score %d", r, id, Score(id))
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("err = %v; want nil on cancel", err)
	}
	if consumed.Load() != 2 || published.Load() != 2 {
		t.Fatalf("consumed %d published %d", consumed.Load(), published.Load())
	}
}

func TestRiskStopsWhenClosed(t *testing.T) {
	in := make(chan []byte)
	close(in)
	var consumed, published atomic.Int64
	err := Run(context.Background(), in, func(context.Context, *paymentv1.RiskResult) error { return nil }, &consumed, &published, discard)
	if err == nil {
		t.Fatal("closed channel with a live context must be an error")
	}
}
