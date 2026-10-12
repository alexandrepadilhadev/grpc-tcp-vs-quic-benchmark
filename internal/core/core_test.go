package core

import (
	"context"
	"log/slog"
	"testing"

	"connectrpc.com/connect"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
)

// ledger records calls and answers SETTLED, or fails with code when set.
type ledger struct {
	code  connect.Code
	calls []*paymentv1.SettleRequest
}

func (l *ledger) Settle(_ context.Context, req *connect.Request[paymentv1.SettleRequest]) (*connect.Response[paymentv1.SettleResponse], error) {
	l.calls = append(l.calls, req.Msg)
	if l.code != 0 {
		return nil, connect.NewError(l.code, nil)
	}
	return connect.NewResponse(&paymentv1.SettleResponse{Status: paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED}), nil
}

func process(h *Handler, id string) (*paymentv1.ProcessPaymentResponse, error) {
	resp, err := h.ProcessPayment(context.Background(), connect.NewRequest(&paymentv1.ProcessPaymentRequest{
		PaymentId: id, AccountId: 4821, AmountCents: 1000}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func TestProcessPayment(t *testing.T) {
	l, ev := &ledger{}, NewEvents(10)
	resp, err := process(New(l, ev, 0, slog.New(slog.DiscardHandler)), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED || resp.GetAuthCode() != AuthCode("p1") || len(resp.GetAuthCode()) != 6 {
		t.Fatalf("resp = %v", resp)
	}
	if len(l.calls) != 1 || l.calls[0].GetAuthCode() != AuthCode("p1") || l.calls[0].GetAccountId() != 4821 {
		t.Fatalf("ledger calls = %v", l.calls)
	}
	if len(ev.C()) != 1 {
		t.Fatalf("events = %d; want 1", len(ev.C()))
	}
	e := <-ev.C()
	if e.GetPaymentId() != "p1" || e.GetAccountId() != 4821 || e.GetAmountCents() != 1000 || e.GetCreatedNs() == 0 {
		t.Fatalf("event = %v", e)
	}
}

func TestAuthCodeDeterministic(t *testing.T) {
	if AuthCode("p1") != AuthCode("p1") || AuthCode("p1") == AuthCode("p2") {
		t.Fatalf("AuthCode p1=%q p2=%q", AuthCode("p1"), AuthCode("p2"))
	}
}

func TestEventsFull(t *testing.T) {
	l, ev := &ledger{}, NewEvents(1)
	h := New(l, ev, 0, slog.New(slog.DiscardHandler))
	for _, id := range []string{"p1", "p2"} {
		if resp, err := process(h, id); err != nil || resp.GetStatus() != paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED {
			t.Fatalf("%s: resp %v err %v; a full queue must not fail the payment", id, resp, err)
		}
	}
	if ev.Dropped() != 1 || len(ev.C()) != 1 {
		t.Fatalf("dropped %d queued %d", ev.Dropped(), len(ev.C()))
	}
}

func TestLedgerErrorPropagates(t *testing.T) {
	_, err := process(New(&ledger{code: connect.CodeUnavailable}, NewEvents(10), 0, slog.New(slog.DiscardHandler)), "p1")
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("err = %v; want unavailable", err)
	}
}

func TestProcessInvalid(t *testing.T) {
	l := &ledger{}
	if _, err := process(New(l, NewEvents(10), 0, slog.New(slog.DiscardHandler)), ""); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("err = %v", err)
	}
	if len(l.calls) != 0 {
		t.Fatalf("ledger called %d times", len(l.calls))
	}
}
