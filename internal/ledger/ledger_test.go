package ledger

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"connectrpc.com/connect"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
)

const (
	settled  = paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED
	declined = paymentv1.PaymentStatus_PAYMENT_STATUS_DECLINED
)

func settle(t *testing.T, h *Handler, id string, account uint32, amount int64) (paymentv1.PaymentStatus, error) {
	t.Helper()
	resp, err := h.Settle(context.Background(), connect.NewRequest(&paymentv1.SettleRequest{
		PaymentId: id, AccountId: account, AmountCents: amount, AuthCode: "ABC123"}))
	if err != nil {
		return 0, err
	}
	return resp.Msg.GetStatus(), nil
}

func newHandler() (*Handler, *MemStore) {
	s := NewMemStore(10, 10_000)
	return New(s, slog.New(slog.DiscardHandler)), s
}

func TestSettle(t *testing.T) {
	h, s := newHandler()
	if st, err := settle(t, h, "p1", 3, 1000); err != nil || st != settled {
		t.Fatalf("status %v err %v", st, err)
	}
	if s.Balance(3) != 9000 || s.Entries() != 1 {
		t.Fatalf("balance %d entries %d", s.Balance(3), s.Entries())
	}
}

func TestSettleIdempotent(t *testing.T) {
	h, s := newHandler()
	for range 2 {
		if st, err := settle(t, h, "p1", 3, 1000); err != nil || st != settled {
			t.Fatalf("status %v err %v", st, err)
		}
	}
	if s.Balance(3) != 9000 || s.Entries() != 1 {
		t.Fatalf("balance %d entries %d; want one debit", s.Balance(3), s.Entries())
	}
}

func TestSettleDeclined(t *testing.T) {
	h, s := newHandler()
	if st, err := settle(t, h, "p1", 3, 20_000); err != nil || st != declined {
		t.Fatalf("short balance: status %v err %v", st, err)
	}
	if st, err := settle(t, h, "p2", 99, 1000); err != nil || st != declined {
		t.Fatalf("unknown account: status %v err %v", st, err)
	}
	if s.Balance(3) != 10_000 || s.Entries() != 0 {
		t.Fatalf("balance %d entries %d; want unchanged", s.Balance(3), s.Entries())
	}
}

func TestSettleInvalid(t *testing.T) {
	h, _ := newHandler()
	if _, err := settle(t, h, "p1", 3, 0); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("amount 0: %v", err)
	}
	_, err := h.Settle(context.Background(), connect.NewRequest(&paymentv1.SettleRequest{
		PaymentId: "p1", AccountId: 3, AmountCents: 1000}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("empty auth_code: %v", err)
	}
}

func TestMemStoreCanceledContext(t *testing.T) {
	h, s := newHandler()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := h.Settle(ctx, connect.NewRequest(&paymentv1.SettleRequest{
		PaymentId: "p1", AccountId: 3, AmountCents: 1000, AuthCode: "ABC123"}))
	if connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("err = %v; want canceled", err)
	}
	if s.Balance(3) != 10_000 || s.Entries() != 0 {
		t.Fatalf("balance %d entries %d; want unchanged", s.Balance(3), s.Entries())
	}
}

func TestMemStoreConcurrentSameAccount(t *testing.T) {
	s := NewMemStore(10, 10_000)
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			st, err := s.Settle(context.Background(), Entry{PaymentID: fmt.Sprint("p", i), AccountID: 7, AmountCents: 10, AuthCode: "A"})
			if err != nil || st != settled {
				t.Errorf("status %v err %v", st, err)
			}
		})
	}
	wg.Wait()
	if s.Balance(7) != 10_000-1000 || s.Entries() != 100 {
		t.Fatalf("balance %d entries %d", s.Balance(7), s.Entries())
	}
}

// TestCounters: the ledger reports what it committed, so the analysis can find
// payments the system settled while the client saw an error (orphans).
func TestCounters(t *testing.T) {
	h, _ := newHandler()
	for _, id := range []string{"p1", "p2"} {
		if st, err := settle(t, h, id, 3, 1000); err != nil || st != settled {
			t.Fatalf("%s: status %v err %v", id, st, err)
		}
	}
	if st, err := settle(t, h, "p3", 3, 1_000_000); err != nil || st != declined {
		t.Fatalf("p3: status %v err %v", st, err)
	}
	if _, err := settle(t, h, "p4", 3, 0); err == nil {
		t.Fatal("invalid payment accepted")
	}
	if got := h.Counters(); got["settled"] != 2 || got["declined"] != 1 || len(got) != 2 {
		t.Fatalf("counters = %v; want settled 2, declined 1", got)
	}
}
