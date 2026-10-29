package main

import (
	"context"
	"strings"
	"testing"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport/transporttest"
)

func TestMissingDatabaseURL(t *testing.T) {
	cert, key, ca := transporttest.NewCertFiles(t)
	t.Setenv("DATABASE_URL", "")
	err := run(context.Background(), []string{"--addr=127.0.0.1:0", "--tls-cert=" + cert, "--tls-key=" + key, "--tls-ca=" + ca})
	if err == nil || !strings.Contains(err.Error(), "db-url") {
		t.Fatalf("err = %v; want a db-url error before opening any socket", err)
	}
}
