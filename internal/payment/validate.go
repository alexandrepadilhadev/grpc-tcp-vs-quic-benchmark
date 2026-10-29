// Package payment holds the rules shared by every payment service.
package payment

import (
	"errors"

	"connectrpc.com/connect"
)

// MaxIDBytes bounds payment_id, the correlation and idempotency key.
const MaxIDBytes = 64

// Validate returns connect.CodeInvalidArgument unless the payment has an id
// of 1..MaxIDBytes bytes, an account and a positive amount.
func Validate(paymentID string, accountID uint32, amountCents int64) error {
	switch {
	case paymentID == "" || len(paymentID) > MaxIDBytes:
		return connect.NewError(connect.CodeInvalidArgument, errors.New("payment_id must have 1..64 bytes"))
	case accountID == 0:
		return connect.NewError(connect.CodeInvalidArgument, errors.New("account_id must be > 0"))
	case amountCents <= 0:
		return connect.NewError(connect.CodeInvalidArgument, errors.New("amount_cents must be > 0"))
	}
	return nil
}
