// Package ledger settles payments: one atomic debit per payment_id.
package ledger

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"

	"connectrpc.com/connect"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/payment"
)

// Entry is one settlement.
type Entry struct {
	PaymentID   string
	AccountID   uint32
	AmountCents int64
	AuthCode    string
}

// Store applies an entry atomically: an already-settled PaymentID returns SETTLED
// without debiting again; a missing or short account returns DECLINED and changes nothing.
type Store interface {
	Settle(ctx context.Context, e Entry) (paymentv1.PaymentStatus, error)
}

// Handler implements paymentv1connect.LedgerServiceHandler.
type Handler struct {
	store             Store
	log               *slog.Logger
	settled, declined atomic.Int64 // committed outcomes, for the shutdown report
}

func New(s Store, log *slog.Logger) *Handler { return &Handler{store: s, log: log} }

// Counters are the committed outcomes so far: settled and declined payments.
// With no retries in the system, settled counts distinct payments.
func (h *Handler) Counters() map[string]int64 {
	return map[string]int64{"settled": h.settled.Load(), "declined": h.declined.Load()}
}

func (h *Handler) Settle(ctx context.Context, req *connect.Request[paymentv1.SettleRequest]) (*connect.Response[paymentv1.SettleResponse], error) {
	m := req.Msg
	if err := payment.Validate(m.GetPaymentId(), m.GetAccountId(), m.GetAmountCents()); err != nil {
		return nil, err
	}
	if m.GetAuthCode() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("auth_code is required"))
	}
	debug := h.log.Enabled(ctx, slog.LevelDebug) // args are built only when debug is on
	if debug {
		h.log.DebugContext(ctx, "settle received", "payment_id", m.GetPaymentId(),
			"account_id", m.GetAccountId(), "amount_cents", m.GetAmountCents())
	}
	st, err := h.store.Settle(ctx, Entry{m.GetPaymentId(), m.GetAccountId(), m.GetAmountCents(), m.GetAuthCode()})
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, connect.NewError(connect.CodeDeadlineExceeded, err)
		}
		if ctx.Err() != nil {
			return nil, connect.NewError(connect.CodeCanceled, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if st == paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED {
		h.settled.Add(1)
	} else {
		h.declined.Add(1)
	}
	if debug {
		msg := "debit applied"
		if st != paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED {
			msg = "declined"
		}
		h.log.DebugContext(ctx, msg, "payment_id", m.GetPaymentId())
	}
	return connect.NewResponse(&paymentv1.SettleResponse{Status: st}), nil
}
