package ledger

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"testing"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
)

// newPgStore connects to TEST_DATABASE_URL (seeded with deploy/postgres/init.sql);
// without it the test is skipped. ids are unique per test so runs never collide.
func newPgStore(t *testing.T) (*PgStore, func(string) string) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set (make test-db)")
	}
	s, err := NewPgStore(context.Background(), url, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	prefix := hex.EncodeToString(b)
	return s, func(id string) string { return prefix + "-" + id }
}

func pgBalance(t *testing.T, s *PgStore, id uint32) int64 {
	t.Helper()
	var b int64
	if err := s.pool.QueryRow(context.Background(), "SELECT balance_cents FROM accounts WHERE id = $1", id).Scan(&b); err != nil {
		t.Fatal(err)
	}
	return b
}

func pgEntries(t *testing.T, s *PgStore, paymentID string) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(), "SELECT count(*) FROM entries WHERE payment_id = $1", paymentID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPgStoreSettle(t *testing.T) {
	s, id := newPgStore(t)
	before := pgBalance(t, s, 3)
	st, err := s.Settle(context.Background(), Entry{id("p1"), 3, 1000, "ABC123"})
	if err != nil || st != settled {
		t.Fatalf("status %v err %v", st, err)
	}
	if got := pgBalance(t, s, 3); got != before-1000 || pgEntries(t, s, id("p1")) != 1 {
		t.Fatalf("balance %d (before %d) entries %d", got, before, pgEntries(t, s, id("p1")))
	}
}

func TestPgStoreIdempotent(t *testing.T) {
	s, id := newPgStore(t)
	before := pgBalance(t, s, 3)
	for range 2 {
		if st, err := s.Settle(context.Background(), Entry{id("p1"), 3, 1000, "ABC123"}); err != nil || st != settled {
			t.Fatalf("status %v err %v", st, err)
		}
	}
	if got := pgBalance(t, s, 3); got != before-1000 {
		t.Fatalf("balance %d (before %d); want one debit", got, before)
	}
}

func TestPgStoreDeclined(t *testing.T) {
	s, id := newPgStore(t)
	st, err := s.Settle(context.Background(), Entry{id("p1"), 99999, 1000, "ABC123"})
	if err != nil || st != declined {
		t.Fatalf("status %v err %v", st, err)
	}
	if n := pgEntries(t, s, id("p1")); n != 0 {
		t.Fatalf("entries = %d; want the insert rolled back", n)
	}
}

func TestPgStoreCanceledContext(t *testing.T) {
	s, id := newPgStore(t)
	before := pgBalance(t, s, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Settle(ctx, Entry{id("p1"), 3, 1000, "ABC123"}); err == nil {
		t.Fatal("settled with a canceled context")
	}
	if got := pgBalance(t, s, 3); got != before || pgEntries(t, s, id("p1")) != 0 {
		t.Fatalf("balance %d (before %d) entries %d; want unchanged", got, before, pgEntries(t, s, id("p1")))
	}
}

func TestPgStoreConcurrentSameAccount(t *testing.T) {
	s, id := newPgStore(t)
	before := pgBalance(t, s, 7)
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			st, err := s.Settle(context.Background(), Entry{id(fmt.Sprint("p", i)), 7, 1000, "A"})
			if err != nil || st != settled {
				t.Errorf("status %v err %v", st, err)
			}
		})
	}
	wg.Wait()
	if got := pgBalance(t, s, 7); got != before-100*1000 {
		t.Fatalf("balance %d (before %d); want exactly -100000", got, before)
	}
}

// TestPgStoreConcurrentBoundary: concurrent debits racing for the last funds
// settle exactly as many as the balance covers (READ COMMITTED re-checks
// balance_cents >= amount after each lock wait).
func TestPgStoreConcurrentBoundary(t *testing.T) {
	s, id := newPgStore(t)
	const account = 9
	if _, err := s.pool.Exec(context.Background(), "UPDATE accounts SET balance_cents = 50000 WHERE id = $1", account); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	n := map[paymentv1.PaymentStatus]int{}
	for i := range 100 {
		wg.Go(func() {
			st, err := s.Settle(context.Background(), Entry{id(fmt.Sprint("b", i)), account, 1000, "A"})
			if err != nil {
				t.Errorf("err %v", err)
			}
			mu.Lock()
			n[st]++
			mu.Unlock()
		})
	}
	wg.Wait()
	if n[settled] != 50 || n[declined] != 50 || pgBalance(t, s, account) != 0 {
		t.Fatalf("settled %d declined %d balance %d; want 50/50/0", n[settled], n[declined], pgBalance(t, s, account))
	}
}
