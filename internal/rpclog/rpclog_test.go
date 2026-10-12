package rpclog_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1/paymentv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/rpclog"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
)

// ledger answers SETTLED, or fails with code when it is set.
type ledger struct{ code connect.Code }

func (l ledger) Settle(context.Context, *connect.Request[paymentv1.SettleRequest]) (*connect.Response[paymentv1.SettleResponse], error) {
	if l.code != 0 {
		return nil, connect.NewError(l.code, nil)
	}
	return connect.NewResponse(&paymentv1.SettleResponse{Status: paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED}), nil
}

// call sends one Settle with the interceptor on both ends; it returns the
// server and client log lines.
func call(t *testing.T, level slog.Level, code connect.Code) (srvLines, cliLines []map[string]any, err error) {
	t.Helper()
	var srvBuf, cliBuf bytes.Buffer
	opts := &slog.HandlerOptions{Level: level}
	mux := http.NewServeMux()
	mux.Handle(paymentv1connect.NewLedgerServiceHandler(ledger{code},
		connect.WithInterceptors(rpclog.Interceptor(slog.New(slog.NewJSONHandler(&srvBuf, opts))))))
	// as transport.Server does: every response names the HTTP version served
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(transport.HeaderServedProto, r.Proto)
		mux.ServeHTTP(w, r)
	}))
	defer srv.Close()

	cli := paymentv1connect.NewLedgerServiceClient(srv.Client(), srv.URL,
		connect.WithInterceptors(rpclog.Interceptor(slog.New(slog.NewJSONHandler(&cliBuf, opts)))))
	_, err = cli.Settle(context.Background(), connect.NewRequest(&paymentv1.SettleRequest{
		PaymentId: "p1", AccountId: 1, AmountCents: 1000, AuthCode: "ABC123"}))
	return parse(t, &srvBuf), parse(t, &cliBuf), err
}

func parse(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("bad log line %q: %v", l, err)
		}
		lines = append(lines, m)
	}
	return lines
}

func TestLogsServerAndClient(t *testing.T) {
	srv, cli, err := call(t, slog.LevelDebug, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(srv) != 1 || len(cli) != 1 {
		t.Fatalf("server %v client %v; want 1 line each", srv, cli)
	}
	s := srv[0]
	if s["msg"] != "rpc" || s["level"] != "DEBUG" || s["code"] != "ok" || s["side"] != "server" ||
		s["payment_id"] != "p1" || s["procedure"] != paymentv1connect.LedgerServiceSettleProcedure {
		t.Fatalf("server line = %v", s)
	}
	if d, ok := s["duration_us"].(float64); !ok || d < 0 {
		t.Fatalf("duration_us = %v", s["duration_us"])
	}
	if s["peer"] == "" || s["protocol"] != connect.ProtocolConnect {
		t.Fatalf("peer/protocol = %v %v", s["peer"], s["protocol"])
	}
	if c := cli[0]; c["side"] != "client" || c["payment_id"] != "p1" || c["code"] != "ok" || c["http"] != "HTTP/1.1" {
		t.Fatalf("client line = %v; want http = the served HTTP version", c)
	}
	if _, ok := s["http"]; ok {
		t.Fatalf("server line = %v; the server side cannot see the HTTP version", s)
	}
}

func TestLogsError(t *testing.T) {
	srv, cli, err := call(t, slog.LevelDebug, connect.CodeUnavailable)
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("client err = %v; want unavailable unchanged", err)
	}
	if len(srv) != 1 || srv[0]["code"] != "unavailable" || len(cli) != 1 || cli[0]["code"] != "unavailable" {
		t.Fatalf("server %v client %v", srv, cli)
	}
}

func TestSilentAtInfo(t *testing.T) {
	for _, code := range []connect.Code{0, connect.CodeUnavailable} {
		srv, cli, _ := call(t, slog.LevelInfo, code)
		if len(srv)+len(cli) != 0 {
			t.Fatalf("code %v: server %v client %v; want none at info", code, srv, cli)
		}
	}
}
