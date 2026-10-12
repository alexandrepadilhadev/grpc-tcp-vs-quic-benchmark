package gateway

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"connectrpc.com/connect"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
)

// core answers SETTLED/ABC123, or fails with code when set.
type core struct {
	code  connect.Code
	calls int
}

func (c *core) ProcessPayment(_ context.Context, req *connect.Request[paymentv1.ProcessPaymentRequest]) (*connect.Response[paymentv1.ProcessPaymentResponse], error) {
	c.calls++
	if c.code != 0 {
		return nil, connect.NewError(c.code, nil)
	}
	return connect.NewResponse(&paymentv1.ProcessPaymentResponse{
		Status: paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED, AuthCode: "ABC123"}), nil
}

func create(h *Handler, id string) (*paymentv1.CreatePaymentResponse, error) {
	resp, err := h.CreatePayment(context.Background(), connect.NewRequest(&paymentv1.CreatePaymentRequest{
		PaymentId: id, AccountId: 4821, AmountCents: 1000}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func TestCreatePayment(t *testing.T) {
	resp, err := create(New(&core{}, slog.New(slog.DiscardHandler)), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPaymentId() != "p1" || resp.GetStatus() != paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED || resp.GetAuthCode() != "ABC123" {
		t.Fatalf("resp = %v", resp)
	}
}

func TestCreateInvalid(t *testing.T) {
	c := &core{}
	if _, err := create(New(c, slog.New(slog.DiscardHandler)), strings.Repeat("a", 65)); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("err = %v", err)
	}
	if c.calls != 0 {
		t.Fatalf("core called %d times", c.calls)
	}
}

func TestCoreErrorPropagates(t *testing.T) {
	_, err := create(New(&core{code: connect.CodeDeadlineExceeded}, slog.New(slog.DiscardHandler)), "p1")
	if connect.CodeOf(err) != connect.CodeDeadlineExceeded {
		t.Fatalf("err = %v; want deadline_exceeded", err)
	}
}
