package ledger

import (
	"context"
	"sync"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
)

// MemStore is an in-memory Store for tests and runs without PostgreSQL.
type MemStore struct {
	mu       sync.Mutex
	balances map[uint32]int64
	entries  map[string]Entry
}

// NewMemStore creates accounts 1..accounts with balanceCents each.
func NewMemStore(accounts int, balanceCents int64) *MemStore {
	m := &MemStore{balances: make(map[uint32]int64, accounts), entries: map[string]Entry{}}
	for id := 1; id <= accounts; id++ {
		m.balances[uint32(id)] = balanceCents
	}
	return m
}

func (m *MemStore) Settle(ctx context.Context, e Entry) (paymentv1.PaymentStatus, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.entries[e.PaymentID]; ok {
		return paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED, nil
	}
	b, ok := m.balances[e.AccountID]
	if !ok || b < e.AmountCents {
		return paymentv1.PaymentStatus_PAYMENT_STATUS_DECLINED, nil
	}
	m.balances[e.AccountID] = b - e.AmountCents
	m.entries[e.PaymentID] = e
	return paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED, nil
}

func (m *MemStore) Balance(id uint32) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.balances[id]
}

func (m *MemStore) Entries() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}
