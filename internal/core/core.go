// Package core processes payments: business rules, the stub acquirer,
// the payment event and the settlement in the Ledger.
package core

import (
	"context"
	"encoding/base32"
	"encoding/binary"
	"hash/fnv"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1/paymentv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/payment"
)

// AuthCode is the stub acquirer's authorization code: the first 6 chars of
// the unpadded base32 of FNV-1a 64 over paymentID.
func AuthCode(paymentID string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(paymentID))
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(binary.BigEndian.AppendUint64(nil, h.Sum64()))[:6]
}

// Handler implements paymentv1connect.CoreServiceHandler.
type Handler struct {
	ledger        paymentv1connect.LedgerServiceClient
	events        *Events
	acquirerDelay time.Duration
	log           *slog.Logger
}

func New(ledger paymentv1connect.LedgerServiceClient, ev *Events, acquirerDelay time.Duration, log *slog.Logger) *Handler {
	return &Handler{ledger: ledger, events: ev, acquirerDelay: acquirerDelay, log: log}
}

func (h *Handler) ProcessPayment(ctx context.Context, req *connect.Request[paymentv1.ProcessPaymentRequest]) (*connect.Response[paymentv1.ProcessPaymentResponse], error) {
	m := req.Msg
	id := m.GetPaymentId()
	if err := payment.Validate(id, m.GetAccountId(), m.GetAmountCents()); err != nil {
		return nil, err
	}
	debug := h.log.Enabled(ctx, slog.LevelDebug) // args are built only when debug is on
	if debug {
		h.log.DebugContext(ctx, "payment received", "payment_id", id, "account_id", m.GetAccountId(), "amount_cents", m.GetAmountCents())
	}

	if h.acquirerDelay > 0 {
		t := time.NewTimer(h.acquirerDelay)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			if ctx.Err() == context.DeadlineExceeded {
				return nil, connect.NewError(connect.CodeDeadlineExceeded, ctx.Err())
			}
			return nil, connect.NewError(connect.CodeCanceled, ctx.Err())
		}
	}
	code := AuthCode(id)
	if debug {
		h.log.DebugContext(ctx, "acquirer approved", "payment_id", id, "auth_code", code)
	}

	queued := h.events.Offer(&paymentv1.PaymentEvent{PaymentId: id, AccountId: m.GetAccountId(),
		AmountCents: m.GetAmountCents(), CreatedNs: time.Now().UnixNano()})
	if debug {
		msg := "event queued"
		if !queued {
			msg = "event dropped"
		}
		h.log.DebugContext(ctx, msg, "payment_id", id)
	}

	resp, err := h.ledger.Settle(ctx, connect.NewRequest(&paymentv1.SettleRequest{
		PaymentId: id, AccountId: m.GetAccountId(), AmountCents: m.GetAmountCents(), AuthCode: code}))
	if err != nil {
		return nil, connect.NewError(connect.CodeOf(err), err)
	}
	return connect.NewResponse(&paymentv1.ProcessPaymentResponse{Status: resp.Msg.GetStatus(), AuthCode: code}), nil
}
