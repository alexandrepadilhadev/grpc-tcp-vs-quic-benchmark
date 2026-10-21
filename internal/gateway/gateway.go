// Package gateway is the entry point of the payment system: it validates
// a payment and hands it to the Core.
package gateway

import (
	"context"
	"log/slog"
	"maps"
	"net"
	"sync"

	"connectrpc.com/connect"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1/paymentv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/payment"
)

// Handler implements paymentv1connect.PaymentServiceHandler.
type Handler struct {
	core paymentv1connect.CoreServiceClient
	log  *slog.Logger

	mu    sync.Mutex
	peers map[string]int64 // RPCs per client IP
}

func New(core paymentv1connect.CoreServiceClient, log *slog.Logger) *Handler {
	return &Handler{core: core, log: log, peers: map[string]int64{}}
}

// Peers returns a copy of the RPC count per client IP.
func (h *Handler) Peers() map[string]int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return maps.Clone(h.peers)
}

func (h *Handler) countPeer(addr string) {
	if addr == "" { // unit-test requests carry no address; skipping avoids SplitHostPort's error alloc
		return
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return
	}
	h.mu.Lock()
	h.peers[host]++
	h.mu.Unlock()
}

func (h *Handler) CreatePayment(ctx context.Context, req *connect.Request[paymentv1.CreatePaymentRequest]) (*connect.Response[paymentv1.CreatePaymentResponse], error) {
	h.countPeer(req.Peer().Addr)
	m := req.Msg
	if err := payment.Validate(m.GetPaymentId(), m.GetAccountId(), m.GetAmountCents()); err != nil {
		return nil, err
	}
	if h.log.Enabled(ctx, slog.LevelDebug) { // args are built only when debug is on
		h.log.DebugContext(ctx, "payment received", "payment_id", m.GetPaymentId(),
			"account_id", m.GetAccountId(), "amount_cents", m.GetAmountCents())
	}
	resp, err := h.core.ProcessPayment(ctx, connect.NewRequest(&paymentv1.ProcessPaymentRequest{
		PaymentId: m.GetPaymentId(), AccountId: m.GetAccountId(), AmountCents: m.GetAmountCents()}))
	if err != nil {
		return nil, connect.NewError(connect.CodeOf(err), err)
	}
	return connect.NewResponse(&paymentv1.CreatePaymentResponse{
		PaymentId: m.GetPaymentId(), Status: resp.Msg.GetStatus(), AuthCode: resp.Msg.GetAuthCode()}), nil
}
