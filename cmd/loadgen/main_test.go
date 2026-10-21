package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1/paymentv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/core"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/gateway"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/ledger"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/loadgen"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/service"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport/transporttest"
)

var discard = slog.New(slog.DiscardHandler)

// chain is gateway -> core -> ledger served in-process over one transport.
type chain struct {
	gatewayAddr, ca string
	gwCli, coreCli  *transport.Client // gateway's client of core, core's client of ledger
	events          *core.Events
}

func serve(t *testing.T, cfg transport.Config, path string, h http.Handler) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(path, h)
	srv, err := transport.Listen(cfg, mux, discard)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return srv.Addr().String()
}

// startChain serves the ledger (on store), the core and the gateway; coreAddr,
// when set, replaces the real core as the gateway's upstream.
func startChain(t *testing.T, tr transport.Transport, store ledger.Store, coreAddr string) *chain {
	t.Helper()
	cert, key, ca := transporttest.NewCertFiles(t)
	cfg := transport.DefaultConfig()
	cfg.Transport, cfg.Addr, cfg.CertFile, cfg.KeyFile, cfg.CAFile = tr, "127.0.0.1:0", cert, key, ca
	if coreAddr != "" {
		cfg.HandshakeTimeout = 300 * time.Millisecond
	}
	newClient := func() (*transport.Client, []connect.ClientOption) {
		cli, opts, err := service.NewClient(cfg, discard)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cli.Close() })
		return cli, opts
	}
	c := &chain{ca: ca, events: core.NewEvents(10000)}

	if coreAddr == "" {
		path, h := paymentv1connect.NewLedgerServiceHandler(ledger.New(store, discard), service.HandlerOptions(discard)...)
		ledgerAddr := serve(t, cfg, path, h)
		cli, opts := newClient()
		c.coreCli = cli
		lc := paymentv1connect.NewLedgerServiceClient(cli, "https://"+ledgerAddr, opts...)
		path, h = paymentv1connect.NewCoreServiceHandler(core.New(lc, c.events, 0, discard), service.HandlerOptions(discard)...)
		coreAddr = serve(t, cfg, path, h)
	}
	cli, opts := newClient()
	c.gwCli = cli
	cc := paymentv1connect.NewCoreServiceClient(cli, "https://"+coreAddr, opts...)
	path, h := paymentv1connect.NewPaymentServiceHandler(gateway.New(cc, discard), service.HandlerOptions(discard)...)
	c.gatewayAddr = serve(t, cfg, path, h)
	return c
}

// gatewayClient is a client of the chain's gateway outside the loadgen.
func gatewayClient(t *testing.T, tr transport.Transport, c *chain) paymentv1connect.PaymentServiceClient {
	t.Helper()
	cfg := transport.DefaultConfig()
	cfg.Transport, cfg.CAFile = tr, c.ca
	cli, opts, err := service.NewClient(cfg, discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cli.Close() })
	return paymentv1connect.NewPaymentServiceClient(cli, "https://"+c.gatewayAddr, opts...)
}

var rowRe = regexp.MustCompile(`^\d+,\d+,\d+,\d+,ok,\d+,\d+,measure$`)

func TestSmoke(t *testing.T) {
	for _, tr := range []transport.Transport{transport.H2, transport.H3} {
		t.Run(tr.String(), func(t *testing.T) {
			store := ledger.NewMemStore(10000, 1e15)
			c := startChain(t, tr, store, "")
			out := filepath.Join(t.TempDir(), "run", "requests.csv")
			err := run(context.Background(), []string{
				"--transport=" + tr.String(), "--target=" + c.gatewayAddr, "--tls-ca=" + c.ca,
				"--requests=1000", "--warmup=0", "--concurrency=50", "--strict", "--out=" + out,
			}, discard)
			if err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(out)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(string(b)), "\n")
			if lines[0] != loadgen.Header || len(lines) != 1001 {
				t.Fatalf("header=%q rows=%d", lines[0], len(lines)-1)
			}
			for _, l := range lines[1:] {
				if !rowRe.MatchString(l) || strings.Contains(l, ",0,0,measure") {
					t.Fatalf("bad row %q", l)
				}
			}
			var m loadgen.Meta
			b, err = os.ReadFile(filepath.Join(filepath.Dir(out), "meta.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatal(err)
			}
			if m.Dials != 1 || m.OK != 1000 || m.Transport != tr.String() || m.HandshakeUs <= 0 {
				t.Fatalf("meta = %+v", m)
			}
			if c.gwCli.Dials() != 1 || c.coreCli.Dials() != 1 {
				t.Fatalf("hop dials gateway->core=%d core->ledger=%d; want 1", c.gwCli.Dials(), c.coreCli.Dials())
			}
			// fan-out 1: one settlement and one event per payment (1000 + probe)
			if store.Entries() != 1001 || len(c.events.C()) != 1001 || c.events.Dropped() != 0 {
				t.Fatalf("entries=%d events=%d dropped=%d", store.Entries(), len(c.events.C()), c.events.Dropped())
			}
		})
	}
}

// blockStore settles at once, except payment "slow": that one waits for its
// ctx to end and reports the ctx's deadline.
type blockStore struct{ deadline chan time.Time }

func (b blockStore) Settle(ctx context.Context, e ledger.Entry) (paymentv1.PaymentStatus, error) {
	if e.PaymentID != "slow" {
		return paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED, nil
	}
	dl, _ := ctx.Deadline() // zero when no deadline reached the ledger
	b.deadline <- dl
	<-ctx.Done()
	return 0, ctx.Err()
}

func TestDeadlinePropagates(t *testing.T) {
	store := blockStore{deadline: make(chan time.Time, 1)}
	c := startChain(t, transport.H3, store, "")
	cli := gatewayClient(t, transport.H3, c)
	// warm-up: opens the three connections, so the 300ms below is all ledger time
	if _, err := cli.CreatePayment(context.Background(), connect.NewRequest(newPayment(10))); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := cli.CreatePayment(ctx, connect.NewRequest(&paymentv1.CreatePaymentRequest{
		PaymentId: "slow", AccountId: 1, AmountCents: amountCents}))
	if connect.CodeOf(err) != connect.CodeDeadlineExceeded {
		t.Fatalf("err = %v; want deadline_exceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("took %v", d)
	}
	select {
	case dl := <-store.deadline:
		if dl.IsZero() || dl.After(start.Add(400*time.Millisecond)) {
			t.Fatalf("ledger deadline = %v (start %v); want the client's 300ms propagated", dl, start)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ledger never received the payment")
	}
}

func TestCoreDown(t *testing.T) {
	for _, tr := range []transport.Transport{transport.H2, transport.H3} {
		t.Run(tr.String(), func(t *testing.T) {
			c := startChain(t, tr, nil, "127.0.0.1:1")
			cli := gatewayClient(t, tr, c)
			start := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := cli.CreatePayment(ctx, connect.NewRequest(newPayment(10)))
			if connect.CodeOf(err) != connect.CodeUnavailable {
				t.Fatalf("err = %v; want unavailable", err)
			}
			// the 300ms handshake timeout, not the 2s deadline, ends the call
			if d := time.Since(start); d > time.Second {
				t.Fatalf("took %v; want a fast failure well before the deadline", d)
			}
		})
	}
}

func TestNewPayment(t *testing.T) {
	hexID := regexp.MustCompile(`^[0-9a-f]{32}$`)
	seen := map[string]bool{}
	for range 1000 {
		p := newPayment(10)
		if !hexID.MatchString(p.GetPaymentId()) || seen[p.GetPaymentId()] {
			t.Fatalf("payment_id %q (dup=%v)", p.GetPaymentId(), seen[p.GetPaymentId()])
		}
		seen[p.GetPaymentId()] = true
		if p.GetAccountId() < 1 || p.GetAccountId() > 10 || p.GetAmountCents() != 1000 {
			t.Fatalf("payment = %v", p)
		}
	}
}

func TestTransportMismatch(t *testing.T) {
	c := startChain(t, transport.H2, ledger.NewMemStore(10, 1e15), "")
	out := filepath.Join(t.TempDir(), "requests.csv")
	start := time.Now()
	err := run(context.Background(), []string{
		"--transport=h3", "--target=" + c.gatewayAddr, "--tls-ca=" + c.ca, "--handshake-timeout=500ms",
		"--requests=10", "--out=" + out,
	}, discard)
	if err == nil {
		t.Fatal("run succeeded against the wrong transport")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("took %v", d)
	}
	if b, _ := os.ReadFile(out); strings.Count(string(b), "\n") > 1 {
		t.Fatalf("CSV has data rows: %q", b)
	}
}

func TestBadOut(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	err := run(context.Background(), []string{
		"--target=127.0.0.1:1", "--tls-ca=ca.crt", "--requests=10", "--out=" + filepath.Join(file, "requests.csv"),
	}, discard)
	if err == nil || !strings.Contains(err.Error(), "out") {
		t.Fatalf("err = %v; want out error before any RPC", err)
	}
}

func TestInvalidLogLevel(t *testing.T) {
	t.Setenv("LOG_LEVEL", "nope")
	err := run(context.Background(), []string{"--target=127.0.0.1:1", "--tls-ca=ca.crt", "--requests=1",
		"--out=" + filepath.Join(t.TempDir(), "requests.csv")}, discard)
	if err == nil || !strings.Contains(err.Error(), "LOG_LEVEL") {
		t.Fatalf("err = %v; want LOG_LEVEL error before any RPC", err)
	}
}

func TestInterrupted(t *testing.T) {
	c := startChain(t, transport.H2, ledger.NewMemStore(10000, 1e15), "")
	out := filepath.Join(t.TempDir(), "requests.csv")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := run(ctx, []string{
		"--target=" + c.gatewayAddr, "--tls-ca=" + c.ca, "--duration=10s", "--warmup=0", "--out=" + out,
	}, discard)
	if err == nil || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("err = %v; want interrupted", err)
	}
	var m loadgen.Meta
	b, err := os.ReadFile(filepath.Join(filepath.Dir(out), "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &m); err != nil || !m.Interrupted {
		t.Fatalf("meta interrupted=%v err=%v", m.Interrupted, err)
	}
}

func TestMigrateFlags(t *testing.T) {
	cases := map[string][]string{
		"at without to":         {"--migrate-at=1s"},
		"to without at":         {"--migrate-to=10.10.0.250"},
		"ipv6":                  {"--migrate-at=1s", "--migrate-to=::1"},
		"not an address":        {"--migrate-at=1s", "--migrate-to=nope"},
		"at after the load":     {"--migrate-at=80s", "--migrate-to=10.10.0.250", "--warmup=10s", "--duration=60s"},
		"incompatible requests": {"--migrate-at=1s", "--migrate-to=10.10.0.250", "--requests=10"},
	}
	for name, extra := range cases {
		t.Run(name, func(t *testing.T) {
			args := append([]string{"--target=127.0.0.1:1", "--tls-ca=ca.crt",
				"--out=" + filepath.Join(t.TempDir(), "requests.csv")}, extra...)
			if err := run(context.Background(), args, discard); err == nil || !strings.Contains(err.Error(), "migrate") {
				t.Fatalf("err = %v; want a migrate flag error", err)
			}
		})
	}
}

// fakeIP replaces the ip command and the interface lookup for the test.
func fakeIP(t *testing.T) *[]string {
	t.Helper()
	var calls []string
	var mu sync.Mutex
	oldL, oldI := localPrefix, runIP
	localPrefix = func(netip.Addr) (string, netip.Prefix, error) {
		return "lo", netip.MustParsePrefix("127.0.0.1/8"), nil
	}
	runIP = func(_ context.Context, args ...string) error {
		mu.Lock()
		calls = append(calls, strings.Join(args, " "))
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() { localPrefix, runIP = oldL, oldI })
	return &calls
}

func TestMigrateRun(t *testing.T) {
	transporttest.NeedLoopback2(t)
	for _, tr := range []transport.Transport{transport.H2, transport.H3} {
		t.Run(tr.String(), func(t *testing.T) {
			calls := fakeIP(t)
			store := ledger.NewMemStore(10000, 1e15)
			c := startChain(t, tr, store, "")
			out := filepath.Join(t.TempDir(), "run", "requests.csv")
			args := []string{
				"--transport=" + tr.String(), "--target=" + c.gatewayAddr, "--tls-ca=" + c.ca,
				"--rps=200", "--duration=2s", "--warmup=0", "--concurrency=50",
				"--migrate-at=1s", "--migrate-to=127.0.0.2", "--out=" + out,
			}
			if tr == transport.H3 { // h2 closes the old connection: in-flight RPCs may fail
				args = append(args, "--strict")
			}
			if err := run(context.Background(), args, discard); err != nil {
				t.Fatal(err)
			}
			want := []string{"addr add 127.0.0.2/8 dev lo", "addr del 127.0.0.1/8 dev lo"}
			if !slices.Equal(*calls, want) {
				t.Fatalf("ip calls = %q; want %q", *calls, want)
			}
			b, err := os.ReadFile(filepath.Join(filepath.Dir(out), "events.csv"))
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n")[1:] {
				names = append(names, strings.Split(l, ",")[0])
			}
			if !slices.Equal(names, []string{"ip_added", "migrate_start", "migrated", "ip_removed"}) {
				t.Fatalf("events = %q", names)
			}
			var m loadgen.Meta
			b, err = os.ReadFile(filepath.Join(filepath.Dir(out), "meta.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatal(err)
			}
			wantDials := int64(1)
			if tr == transport.H2 {
				wantDials = 2
			}
			if m.Migrations != 1 || m.MigrateTo != "127.0.0.2" || m.Dials != wantDials || !m.TransportCfg.Migratable || m.OK == 0 {
				t.Fatalf("meta = %+v", m)
			}
		})
	}
}

func TestMigrateInterruptedBeforeAt(t *testing.T) {
	calls := fakeIP(t)
	c := startChain(t, transport.H2, ledger.NewMemStore(10000, 1e15), "")
	out := filepath.Join(t.TempDir(), "requests.csv")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := run(ctx, []string{
		"--target=" + c.gatewayAddr, "--tls-ca=" + c.ca, "--duration=10s", "--warmup=0",
		"--migrate-at=1s", "--migrate-to=127.0.0.2", "--out=" + out,
	}, discard)
	if err == nil || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("err = %v; want interrupted", err)
	}
	if len(*calls) != 0 {
		t.Fatalf("ip calls = %q; want none", *calls)
	}
	var m loadgen.Meta
	b, err := os.ReadFile(filepath.Join(filepath.Dir(out), "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &m); err != nil || m.Migrations != 0 || !m.Interrupted {
		t.Fatalf("meta migrations=%d interrupted=%v err=%v", m.Migrations, m.Interrupted, err)
	}
}
