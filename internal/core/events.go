package core

import (
	"sync/atomic"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
)

// Events is the bounded in-memory queue between the RPC path and the AMQP
// publisher, so a slow broker never blocks a payment.
type Events struct {
	ch      chan *paymentv1.PaymentEvent
	dropped atomic.Int64
}

func NewEvents(size int) *Events {
	return &Events{ch: make(chan *paymentv1.PaymentEvent, size)}
}

// Offer never blocks; it returns false (and counts a drop) when the queue is full.
func (e *Events) Offer(ev *paymentv1.PaymentEvent) bool {
	select {
	case e.ch <- ev:
		return true
	default:
		e.dropped.Add(1)
		return false
	}
}

func (e *Events) C() <-chan *paymentv1.PaymentEvent { return e.ch }

func (e *Events) Dropped() int64 { return e.dropped.Load() }
