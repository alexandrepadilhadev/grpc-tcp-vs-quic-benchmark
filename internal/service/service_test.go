package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1/paymentv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport/transporttest"
)

var discard = slog.New(slog.DiscardHandler)

type ledger struct{}

func (ledger) Settle(context.Context, *connect.Request[paymentv1.SettleRequest]) (*connect.Response[paymentv1.SettleResponse], error) {
	return connect.NewResponse(&paymentv1.SettleResponse{Status: paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED}), nil
}

func testConfig(t *testing.T, tr transport.Transport) transport.Config {
	cert, key, ca := transporttest.NewCertFiles(t)
	cfg := transport.DefaultConfig()
	cfg.Transport, cfg.Addr, cfg.CertFile, cfg.KeyFile, cfg.CAFile = tr, "127.0.0.1:0", cert, key, ca
	return cfg
}

func TestHealthcheck(t *testing.T) {
	for _, tr := range []transport.Transport{transport.H2, transport.H3} {
		t.Run(tr.String(), func(t *testing.T) {
			cfg := testConfig(t, tr)
			path, h := paymentv1connect.NewLedgerServiceHandler(ledger{}, HandlerOptions(discard)...)
			srv, err := Listen("ledger", "test", cfg, discard, path, h)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- srv.Serve(ctx) }()
			defer func() { cancel(); <-done }()

			cfg.Addr = srv.Addr().String()
			if err := Healthcheck(context.Background(), cfg); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHealthcheckDown(t *testing.T) {
	cfg := testConfig(t, transport.H3)
	cfg.Addr, cfg.HandshakeTimeout = "127.0.0.1:1", 300*time.Millisecond
	start := time.Now()
	if err := Healthcheck(context.Background(), cfg); err == nil {
		t.Fatal("healthcheck succeeded with no server")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("took %v", d)
	}
}

func TestFlagPrecedence(t *testing.T) {
	parse := func(args ...string) string {
		t.Helper()
		f := NewFlags("x")
		var addr string
		f.String(&addr, "core-addr", "CORE_ADDR", "core:8443", "core address")
		if err := f.Parse(args); err != nil {
			t.Fatal(err)
		}
		return addr
	}
	if got := parse(); got != "core:8443" {
		t.Fatalf("default = %q", got)
	}
	t.Setenv("CORE_ADDR", "a:1")
	if got := parse(); got != "a:1" {
		t.Fatalf("env = %q", got)
	}
	if got := parse("--core-addr=b:2"); got != "b:2" {
		t.Fatalf("flag = %q", got)
	}
}

func TestInvalidEnv(t *testing.T) {
	t.Setenv("LOG_LEVEL", "nope")
	t.Setenv("ACQUIRER_DELAY", "soon")
	if err := NewFlagsWithCommon(t).Parse(nil); err == nil {
		t.Fatal("invalid env accepted")
	}
	f := NewFlagsWithCommon(t)
	if err := f.Parse([]string{"--log-level=debug", "--acquirer-delay=1ms"}); err != nil {
		t.Fatalf("flags must override invalid env: %v", err)
	}
}

// NewFlagsWithCommon registers the common flags (no transport) plus a duration.
func NewFlagsWithCommon(t *testing.T) *Flags {
	t.Helper()
	f := NewFlags("x")
	c := RegisterCommon(f, 0)
	var d time.Duration
	f.Duration(&d, "acquirer-delay", "ACQUIRER_DELAY", 0, "stub delay")
	t.Cleanup(func() {
		if c.Level != slog.LevelInfo && c.Level != slog.LevelDebug {
			t.Errorf("level = %v", c.Level)
		}
	})
	return f
}

func TestCommonDefaults(t *testing.T) {
	t.Setenv("REP_DIR", "/results/r")
	f := NewFlags("x")
	c := RegisterCommon(f, transport.RoleServer|transport.RoleClient)
	if err := f.Parse([]string{"--transport=h3"}); err != nil {
		t.Fatal(err)
	}
	if c.Level != slog.LevelInfo || c.RepDir != "/results/r" || c.Transport.Transport != transport.H3 {
		t.Fatalf("common = %+v", c)
	}
}

func TestVerifyProto(t *testing.T) {
	path, h := paymentv1connect.NewLedgerServiceHandler(ledger{})
	mux := http.NewServeMux()
	mux.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(transport.HeaderServedProto, "HTTP/1.1")
		h.ServeHTTP(w, r)
	}))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	cli := paymentv1connect.NewLedgerServiceClient(srv.Client(), srv.URL,
		connect.WithGRPC(), connect.WithInterceptors(VerifyProto(transport.H2)))
	_, err := cli.Settle(context.Background(), connect.NewRequest(&paymentv1.SettleRequest{PaymentId: "p1"}))
	if connect.CodeOf(err) != connect.CodeInternal || !errors.Is(err, transport.ErrProtoMismatch) {
		t.Fatalf("err = %v; want internal wrapping ErrProtoMismatch", err)
	}
}

func TestClientServesOverTransport(t *testing.T) {
	for _, tr := range []transport.Transport{transport.H2, transport.H3} {
		t.Run(tr.String(), func(t *testing.T) {
			cfg := testConfig(t, tr)
			mux := http.NewServeMux()
			mux.Handle(paymentv1connect.NewLedgerServiceHandler(ledger{}, HandlerOptions(discard)...))
			srv, err := transport.Listen(cfg, mux, discard)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- srv.Serve(ctx) }()
			defer func() { cancel(); <-done }()

			hc, opts, err := NewClient(cfg, discard)
			if err != nil {
				t.Fatal(err)
			}
			defer hc.Close()
			cli := paymentv1connect.NewLedgerServiceClient(hc, "https://"+srv.Addr().String(), opts...)
			for range 3 {
				if _, err := cli.Settle(context.Background(), connect.NewRequest(&paymentv1.SettleRequest{PaymentId: "p1"})); err != nil {
					t.Fatal(err)
				}
			}
			if hc.Dials() != 1 {
				t.Fatalf("dials = %d", hc.Dials())
			}
		})
	}
}

func TestWriteReport(t *testing.T) {
	dir := t.TempDir()
	r := Report{Service: "core", Version: "v1", Transport: "h3",
		Dials: map[string]int64{"ledger:8443": 1}, Counters: map[string]int64{"events_dropped": 0}}
	if err := WriteReport(dir, r); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "svc-core.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got Report
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Service != "core" || got.Dials["ledger:8443"] != 1 || got.Transport != "h3" {
		t.Fatalf("report = %+v", got)
	}
	if err := WriteReport("", r); err != nil {
		t.Fatalf(`WriteReport("") = %v; want no-op`, err)
	}
}

// TestWriteReportEmptyMaps: every svc-*.json has the same shape, so the
// analysis can read dials/counters without special cases.
func TestWriteReportEmptyMaps(t *testing.T) {
	dir := t.TempDir()
	if err := WriteReport(dir, Report{Service: "risk"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "svc-risk.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"dials", "counters"} {
		if _, ok := m[k].(map[string]any); !ok {
			t.Fatalf("%s = %v; want an object, not null: %s", k, m[k], b)
		}
	}
}
