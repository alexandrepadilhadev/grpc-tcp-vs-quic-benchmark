package transport_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport/transporttest"
)

var loopback2 = netip.MustParseAddr("127.0.0.2")

// peerHandler records the client host of each request.
type peerHandler struct {
	mu    sync.Mutex
	hosts []string
	delay time.Duration
}

func (p *peerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	p.mu.Lock()
	p.hosts = append(p.hosts, host)
	p.mu.Unlock()
	time.Sleep(p.delay)
	okHandler(w, r)
}

func (p *peerHandler) last() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.hosts) == 0 {
		return ""
	}
	return p.hosts[len(p.hosts)-1]
}

func migratable(cfg transport.Config) transport.Config {
	cfg.Migratable = true
	return cfg
}

// waitPeer repeats the request until the server sees host: it switches its
// peer only after validating the new path, so early requests may still show
// the old address.
func waitPeer(ctx context.Context, t *testing.T, cli *transport.Client, url string, ph *peerHandler, host string) {
	t.Helper()
	for range 50 {
		if _, err := get(ctx, cli, url); err != nil {
			t.Fatal(err)
		}
		if ph.last() == host {
			return
		}
	}
	t.Fatalf("host after = %q; want %s", ph.last(), host)
}

func TestMigrateH3(t *testing.T) {
	transporttest.SkipH3SwitchUnderRace(t)
	transporttest.NeedLoopback2(t)
	ph := &peerHandler{}
	url, cfg, _ := startServer(t, transport.H3, ph, nil)
	cli := newClient(t, migratable(cfg))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := get(ctx, cli, url); err != nil {
		t.Fatal(err)
	}
	if h := ph.last(); h != "127.0.0.1" {
		t.Fatalf("host before = %q; want 127.0.0.1", h)
	}
	if err := cli.Migrate(ctx, loopback2); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	waitPeer(ctx, t, cli, url, ph, "127.0.0.2")
	if n := cli.Dials(); n != 1 {
		t.Fatalf("Dials = %d; want 1", n)
	}
}

func TestMigrateH2(t *testing.T) {
	transporttest.NeedLoopback2(t)
	ph := &peerHandler{}
	url, cfg, _ := startServer(t, transport.H2, ph, nil)
	cli := newClient(t, migratable(cfg))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := get(ctx, cli, url); err != nil {
		t.Fatal(err)
	}
	if h := ph.last(); h != "127.0.0.1" {
		t.Fatalf("host before = %q; want 127.0.0.1", h)
	}
	if err := cli.Migrate(ctx, loopback2); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	// A request racing the close may fail once, before http2 sees the conn dead.
	failures := 0
	for {
		_, err := get(ctx, cli, url)
		if err == nil {
			break
		}
		if failures++; failures > 1 || ctx.Err() != nil {
			t.Fatalf("get after Migrate (failure %d): %v", failures, err)
		}
	}
	if h := ph.last(); h != "127.0.0.2" {
		t.Fatalf("host after = %q; want 127.0.0.2", h)
	}
	if n := cli.Dials(); n != 2 {
		t.Fatalf("Dials = %d; want 2", n)
	}
}

func TestMigrateH3InFlight(t *testing.T) {
	transporttest.SkipH3SwitchUnderRace(t)
	transporttest.NeedLoopback2(t)
	ph := &peerHandler{delay: 50 * time.Millisecond}
	url, cfg, _ := startServer(t, transport.H3, ph, nil)
	cli := newClient(t, migratable(cfg))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := get(ctx, cli, url); err != nil {
		t.Fatalf("warm-up: %v", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for range 200 {
		wg.Go(func() {
			resp, err := get(ctx, cli, url)
			if err == nil && resp.StatusCode != 200 {
				err = errors.New(resp.Status)
			}
			errs <- err
		})
	}
	time.Sleep(10 * time.Millisecond)
	if err := cli.Migrate(ctx, loopback2); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := cli.Dials(); n != 1 {
		t.Fatalf("Dials = %d; want 1", n)
	}
	waitPeer(ctx, t, cli, url, ph, "127.0.0.2")
}

// dropProxy relays UDP between a 127.0.0.1 client and the server at url,
// dropping packets from other hosts, so a new path is never validated.
func dropProxy(t *testing.T, url string) string {
	t.Helper()
	srv, err := net.ResolveUDPAddr("udp", strings.TrimPrefix(url, "https://"))
	if err != nil {
		t.Fatal(err)
	}
	in, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	up, err := net.DialUDP("udp", nil, srv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { in.Close(); up.Close() })
	var client atomic.Pointer[net.UDPAddr]
	go func() {
		buf := make([]byte, 65536)
		for {
			n, from, err := in.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if from.IP.Equal(net.IPv4(127, 0, 0, 1)) {
				client.Store(from)
				_, _ = up.Write(buf[:n])
			}
		}
	}()
	go func() {
		buf := make([]byte, 65536)
		for {
			n, err := up.Read(buf)
			if err != nil {
				return
			}
			if a := client.Load(); a != nil {
				_, _ = in.WriteToUDP(buf[:n], a)
			}
		}
	}()
	return "https://" + in.LocalAddr().String()
}

// "unvalidated" fails after the probe left, when the new path's transport
// already holds the connection.
func TestMigrateH3FailureKeepsConnection(t *testing.T) {
	transporttest.NeedLoopback2(t)
	cases := []struct {
		name    string
		timeout time.Duration // 0: canceled before Migrate
		want    error
	}{
		{"canceled", 0, context.Canceled},
		{"unvalidated", 300 * time.Millisecond, context.DeadlineExceeded},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			url, cfg, _ := startServer(t, transport.H3, &peerHandler{}, nil)
			url = dropProxy(t, url)
			cli := newClient(t, migratable(cfg))
			if _, err := get(context.Background(), cli, url); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if c.timeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, c.timeout)
				defer cancel()
			} else {
				cancel()
			}
			if err := cli.Migrate(ctx, loopback2); !errors.Is(err, c.want) {
				t.Fatalf("Migrate = %v; want %v", err, c.want)
			}
			if _, err := get(context.Background(), cli, url); err != nil {
				t.Fatal(err)
			}
			if n := cli.Dials(); n != 1 {
				t.Fatalf("Dials = %d; want 1", n)
			}
		})
	}
}

func TestMigrateNotMigratable(t *testing.T) {
	for _, tr := range both {
		t.Run(tr.String(), func(t *testing.T) {
			url, cfg, _ := startServer(t, tr, &peerHandler{}, nil)
			cli := newClient(t, cfg)
			if _, err := get(context.Background(), cli, url); err != nil {
				t.Fatal(err)
			}
			if err := cli.Migrate(context.Background(), loopback2); !errors.Is(err, transport.ErrNotMigratable) {
				t.Fatalf("Migrate = %v; want ErrNotMigratable", err)
			}
		})
	}
}

func TestMigrateBeforeDial(t *testing.T) {
	t.Run("h2", func(t *testing.T) {
		transporttest.NeedLoopback2(t)
		ph := &peerHandler{}
		url, cfg, _ := startServer(t, transport.H2, ph, nil)
		cli := newClient(t, migratable(cfg))
		if err := cli.Migrate(context.Background(), loopback2); err != nil {
			t.Fatalf("Migrate: %v", err)
		}
		if _, err := get(context.Background(), cli, url); err != nil {
			t.Fatal(err)
		}
		if h := ph.last(); h != "127.0.0.2" {
			t.Fatalf("host = %q; want 127.0.0.2", h)
		}
		if n := cli.Dials(); n != 1 {
			t.Fatalf("Dials = %d; want 1", n)
		}
	})
	t.Run("h3", func(t *testing.T) {
		_, cfg, _ := startServer(t, transport.H3, &peerHandler{}, nil)
		cli := newClient(t, migratable(cfg))
		if err := cli.Migrate(context.Background(), loopback2); !errors.Is(err, transport.ErrNoConnection) {
			t.Fatalf("Migrate = %v; want ErrNoConnection", err)
		}
	})
}
