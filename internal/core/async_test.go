package core

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
)

var discard = slog.New(slog.DiscardHandler)

func TestPublisherDrains(t *testing.T) {
	ev := NewEvents(10)
	for _, id := range []string{"p1", "p2", "p3"} {
		ev.Offer(&paymentv1.PaymentEvent{PaymentId: id})
	}
	got := make(chan string, 3)
	var published atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunPublisher(ctx, ev, func(_ context.Context, e *paymentv1.PaymentEvent) error {
			got <- e.GetPaymentId()
			return nil
		}, &published, discard)
	}()
	for _, want := range []string{"p1", "p2", "p3"} {
		if id := <-got; id != want {
			t.Fatalf("published %q; want %q", id, want)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("err = %v; want nil on cancel", err)
	}
	if published.Load() != 3 {
		t.Fatalf("published = %d", published.Load())
	}
}

func TestPublisherStopsOnError(t *testing.T) {
	ev := NewEvents(10)
	ev.Offer(&paymentv1.PaymentEvent{PaymentId: "p1"})
	boom := errors.New("channel closed")
	var published atomic.Int64
	err := RunPublisher(context.Background(), ev, func(context.Context, *paymentv1.PaymentEvent) error { return boom }, &published, discard)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v; want the publish error", err)
	}
}

func resultMsg(t *testing.T, id string, score uint32) []byte {
	t.Helper()
	b, err := proto.Marshal(&paymentv1.RiskResult{PaymentId: id, Score: score})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestResultsCounts(t *testing.T) {
	in := make(chan []byte, 2)
	in <- resultMsg(t, "p1", 12)
	in <- resultMsg(t, "p2", 99)
	var results atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunResults(ctx, in, &results, discard) }()
	for deadline := time.Now().Add(2 * time.Second); results.Load() < 2; {
		if time.Now().After(deadline) {
			t.Fatalf("results = %d; want 2", results.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("err = %v; want nil on cancel", err)
	}
}

func TestResultsStopsWhenClosed(t *testing.T) {
	in := make(chan []byte)
	close(in)
	var results atomic.Int64
	if err := RunResults(context.Background(), in, &results, discard); err == nil {
		t.Fatal("closed channel with a live context must be an error")
	}
}
