package payment

import (
	"strings"
	"testing"

	"connectrpc.com/connect"
)

func TestValidate(t *testing.T) {
	if err := Validate("p", 1, 1); err != nil {
		t.Fatalf("valid payment: %v", err)
	}
	for _, c := range []struct {
		id      string
		account uint32
		amount  int64
	}{
		{"", 1, 1},
		{strings.Repeat("a", 65), 1, 1},
		{"p", 0, 1},
		{"p", 1, 0},
		{"p", 1, -5},
	} {
		if err := Validate(c.id, c.account, c.amount); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("Validate(%q, %d, %d) = %v; want invalid_argument", c.id, c.account, c.amount, err)
		}
	}
	if err := Validate(strings.Repeat("a", 64), 1, 1); err != nil {
		t.Fatalf("64-byte id rejected: %v", err)
	}
}
