package gateway

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1/paymentv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/payment"
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

func TestPeers(t *testing.T) {
	h := New(&core{}, slog.New(slog.DiscardHandler))
	path, handler := paymentv1connect.NewPaymentServiceHandler(h)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	cli := paymentv1connect.NewPaymentServiceClient(srv.Client(), srv.URL)
	for _, id := range []string{"p1", "p2"} {
		if _, err := cli.CreatePayment(context.Background(), connect.NewRequest(&paymentv1.CreatePaymentRequest{
			PaymentId: id, AccountId: 4821, AmountCents: 1000})); err != nil {
			t.Fatal(err)
		}
	}
	if got := h.Peers(); len(got) != 1 || got["127.0.0.1"] != 2 {
		t.Fatalf("peers = %v; want {127.0.0.1: 2}", got)
	}
}

func TestPeersSkipsUnknownAddr(t *testing.T) {
	h := New(&core{}, slog.New(slog.DiscardHandler))
	if _, err := create(h, "p1"); err != nil { // no Peer on a unit-test request
		t.Fatal(err)
	}
	if got := h.Peers(); len(got) != 0 {
		t.Fatalf("peers = %v; want empty", got)
	}
}

var sink any

// createNoLogs is CreatePayment's work without logs; noinline keeps the
// core call behind the interface, as in the handler.
//
//go:noinline
func createNoLogs(ctx context.Context, cc paymentv1connect.CoreServiceClient, req *connect.Request[paymentv1.CreatePaymentRequest]) any {
	m := req.Msg
	_ = payment.Validate(m.GetPaymentId(), m.GetAccountId(), m.GetAmountCents())
	resp, _ := cc.ProcessPayment(ctx, connect.NewRequest(&paymentv1.ProcessPaymentRequest{
		PaymentId: m.GetPaymentId(), AccountId: m.GetAccountId(), AmountCents: m.GetAmountCents()}))
	return connect.NewResponse(&paymentv1.CreatePaymentResponse{
		PaymentId: m.GetPaymentId(), Status: resp.Msg.GetStatus(), AuthCode: resp.Msg.GetAuthCode()})
}

// TestNoDebugCostAtInfo pins spec §11: with debug off, the handler's step logs
// cost one level check, so it allocates no more than the same work without logs.
func TestNoDebugCostAtInfo(t *testing.T) {
	ctx := context.Background()
	c := &core{}
	h := New(c, slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo})))
	req := connect.NewRequest(&paymentv1.CreatePaymentRequest{PaymentId: "p1", AccountId: 4821, AmountCents: 1000})
	got := testing.AllocsPerRun(200, func() { sink, _ = h.CreatePayment(ctx, req) })
	want := testing.AllocsPerRun(200, func() { sink = createNoLogs(ctx, c, req) })
	if got > want {
		t.Fatalf("allocs at info = %v; want <= %v (debug args must not be built)", got, want)
	}
}
