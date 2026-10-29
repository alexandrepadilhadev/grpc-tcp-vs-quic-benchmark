package ledger

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
)

// PgStore is the PostgreSQL Store (schema in deploy/postgres/init.sql).
type PgStore struct{ pool *pgxpool.Pool }

// NewPgStore opens a pool of at most maxConns connections and pings it.
func NewPgStore(ctx context.Context, url string, maxConns int32) (*PgStore, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &PgStore{pool: pool}, nil
}

func (s *PgStore) Close() { s.pool.Close() }

// errDeclined rolls the transaction back when the debit does not apply.
var errDeclined = errors.New("declined")

// Settle inserts the entry first (idempotency on payment_id), then debits the
// account only if the balance covers it; the row lock lasts one statement.
func (s *PgStore) Settle(ctx context.Context, e Entry) (paymentv1.PaymentStatus, error) {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO entries (payment_id, account_id, amount_cents, auth_code)
			VALUES ($1, $2, $3, $4) ON CONFLICT (payment_id) DO NOTHING`,
			e.PaymentID, e.AccountID, e.AmountCents, e.AuthCode)
		if err != nil || tag.RowsAffected() == 0 { // 0 rows: already settled
			return err
		}
		tag, err = tx.Exec(ctx, `UPDATE accounts SET balance_cents = balance_cents - $2
			WHERE id = $1 AND balance_cents >= $2`, e.AccountID, e.AmountCents)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return errDeclined
		}
		return nil
	})
	switch {
	case errors.Is(err, errDeclined):
		return paymentv1.PaymentStatus_PAYMENT_STATUS_DECLINED, nil
	case err != nil:
		return 0, err
	}
	return paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED, nil
}
