package transport_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport/transporttest"
)

var both = []transport.Transport{transport.H2, transport.H3}

var discard = slog.New(slog.DiscardHandler)

func okHandler(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }

// startServer serves h on 127.0.0.1:0 and returns the base URL, a config
// usable by clients, and a stop func returning Serve's result.
func startServer(t *testing.T, tr transport.Transport, h http.Handler, mutate func(*transport.Config)) (string, transport.Config, func() error) {
	t.Helper()
	cert, key, ca := transporttest.NewCertFiles(t)
	cfg := transport.DefaultConfig()
	cfg.Transport, cfg.Addr, cfg.CertFile, cfg.KeyFile, cfg.CAFile = tr, "127.0.0.1:0", cert, key, ca
	if mutate != nil {
		mutate(&cfg)
	}
	srv, err := transport.Listen(cfg, h, discard)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	var once sync.Once
	var result error
	stop := func() error {
		once.Do(func() { cancel(); result = <-done })
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return "https://" + srv.Addr().String(), cfg, stop
}

func newClient(t *testing.T, cfg transport.Config) *transport.Client {
	t.Helper()
	cli, err := transport.NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func get(ctx context.Context, cli *transport.Client, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp, nil
}

func TestRoundTrip(t *testing.T) {
	for _, tr := range both {
		t.Run(tr.String(), func(t *testing.T) {
			url, cfg, _ := startServer(t, tr, http.HandlerFunc(okHandler), nil)
			cli := newClient(t, cfg)
			resp, err := get(context.Background(), cli, url)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 200 || resp.Proto != tr.Proto() || resp.Header.Get(transport.HeaderServedProto) != tr.Proto() {
				t.Fatalf("status=%d proto=%s served=%q", resp.StatusCode, resp.Proto, resp.Header.Get(transport.HeaderServedProto))
			}
			if err := transport.VerifyServedProto(tr, resp.Header); err != nil {
				t.Fatal(err)
			}
			if n := cli.Dials(); n != 1 {
				t.Fatalf("Dials = %d; want 1", n)
			}
		})
	}
}

// In-flight requests stay below the server's stream limit, as in the
// benchmark (concurrency 50, limit 1000): Go's HTTP/2 client with
// StrictMaxConcurrentRequests stalls when requests exceed the limit
// (reproduced on Go 1.26 and 1.27). QUIC is also checked above its limit.
func TestSingleConnection(t *testing.T) {
	cases := []struct {
		tr    transport.Transport
		limit uint32
	}{
		{transport.H2, 1000},
		{transport.H3, 1000},
		{transport.H3, 10},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s-limit-%d", c.tr, c.limit), func(t *testing.T) {
			slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(20 * time.Millisecond)
				okHandler(w, r)
			})
			url, cfg, _ := startServer(t, c.tr, slow, func(cfg *transport.Config) { cfg.MaxConcurrentStreams = c.limit })
			cli := newClient(t, cfg)
			if _, err := get(context.Background(), cli, url); err != nil {
				t.Fatalf("warm-up: %v", err)
			}
			var wg sync.WaitGroup
			errs := make(chan error, 200)
			for range 200 {
				wg.Go(func() {
					resp, err := get(context.Background(), cli, url)
					if err == nil && resp.StatusCode != 200 {
						err = errors.New(resp.Status)
					}
					errs <- err
				})
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
		})
	}
}

func TestGracefulShutdown(t *testing.T) {
	for _, tr := range both {
		t.Run(tr.String(), func(t *testing.T) {
			started := make(chan struct{})
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(started)
				time.Sleep(300 * time.Millisecond)
				okHandler(w, r)
			})
			url, cfg, stop := startServer(t, tr, h, func(c *transport.Config) { c.ShutdownTimeout = 2 * time.Second })
			cli := newClient(t, cfg)
			type result struct {
				resp *http.Response
				err  error
			}
			res := make(chan result, 1)
			go func() {
				resp, err := get(context.Background(), cli, url)
				res <- result{resp, err}
			}()
			<-started
			stopped := make(chan error, 1)
			go func() { stopped <- stop() }()
			r := <-res
			if r.err != nil || r.resp.StatusCode != 200 {
				t.Fatalf("in-flight request: %v %v", r.resp, r.err)
			}
			_ = cli.Close()
			select {
			case err := <-stopped:
				if err != nil {
					t.Fatalf("Serve = %v; want nil", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Serve did not return")
			}
		})
	}
}

func TestListenErrors(t *testing.T) {
	cert, key, _ := transporttest.NewCertFiles(t)
	cfg := transport.DefaultConfig()
	cfg.Addr = ""
	if _, err := transport.Listen(cfg, http.NotFoundHandler(), discard); !errors.Is(err, transport.ErrInvalidConfig) {
		t.Errorf("invalid config: %v", err)
	}
	cfg = transport.DefaultConfig()
	cfg.Addr, cfg.CertFile, cfg.KeyFile = "127.0.0.1:0", cert+".missing", key
	if _, err := transport.Listen(cfg, http.NotFoundHandler(), discard); !errors.Is(err, transport.ErrTLSConfig) {
		t.Errorf("missing cert: %v", err)
	}
	for _, tr := range both {
		url, _, _ := startServer(t, tr, http.NotFoundHandler(), nil)
		cfg := transport.DefaultConfig()
		cfg.Transport, cfg.Addr, cfg.CertFile, cfg.KeyFile = tr, url[len("https://"):], cert, key
		if _, err := transport.Listen(cfg, http.NotFoundHandler(), discard); err == nil {
			t.Errorf("%s: second Listen on %s succeeded", tr, cfg.Addr)
		}
	}
}

func TestNewClientErrors(t *testing.T) {
	cfg := transport.DefaultConfig()
	if _, err := transport.NewClient(cfg); !errors.Is(err, transport.ErrInvalidConfig) {
		t.Errorf("missing CA flag: %v", err)
	}
	cfg.CAFile = "does-not-exist.pem"
	if _, err := transport.NewClient(cfg); !errors.Is(err, transport.ErrTLSConfig) {
		t.Errorf("missing CA file: %v", err)
	}
}

func TestProtocolMismatch(t *testing.T) {
	cases := []struct{ server, client transport.Transport }{
		{transport.H3, transport.H2},
		{transport.H2, transport.H3},
	}
	for _, c := range cases {
		t.Run(c.client.String()+"-to-"+c.server.String(), func(t *testing.T) {
			url, cfg, _ := startServer(t, c.server, http.HandlerFunc(okHandler), nil)
			cfg.Transport, cfg.HandshakeTimeout = c.client, 500*time.Millisecond
			cli := newClient(t, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			start := time.Now()
			if _, err := get(ctx, cli, url); err == nil {
				t.Fatal("request succeeded; want error")
			}
			if d := time.Since(start); d >= 2*time.Second {
				t.Fatalf("took %v; want < 2s", d)
			}
		})
	}
}

func TestH2RejectsServerWithoutALPN(t *testing.T) {
	certFile, keyFile, ca := transporttest.NewCertFiles(t)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(okHandler)}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	cfg := transport.DefaultConfig()
	cfg.CAFile, cfg.HandshakeTimeout = ca, time.Second
	cli := newClient(t, cfg)
	_, err = get(context.Background(), cli, "https://"+ln.Addr().(*net.TCPAddr).String())
	if !errors.Is(err, transport.ErrProtoMismatch) {
		t.Fatalf("err = %v; want ErrProtoMismatch", err)
	}
}

func TestVerifyServedProto(t *testing.T) {
	h := http.Header{}
	h.Set(transport.HeaderServedProto, "HTTP/2.0")
	if err := transport.VerifyServedProto(transport.H2, h); err != nil {
		t.Errorf("match: %v", err)
	}
	if err := transport.VerifyServedProto(transport.H3, h); !errors.Is(err, transport.ErrProtoMismatch) {
		t.Errorf("mismatch: %v", err)
	}
	if err := transport.VerifyServedProto(transport.H2, http.Header{}); !errors.Is(err, transport.ErrProtoMismatch) {
		t.Errorf("missing: %v", err)
	}
}

// A burst on a fresh client must not open extra connections.
func TestBurstSingleDial(t *testing.T) {
	for _, tr := range both {
		t.Run(tr.String(), func(t *testing.T) {
			slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(20 * time.Millisecond)
				okHandler(w, r)
			})
			url, cfg, _ := startServer(t, tr, slow, nil)
			cli := newClient(t, cfg)
			var wg sync.WaitGroup
			var failed atomic.Int64
			for range 200 {
				wg.Go(func() {
					if resp, err := get(context.Background(), cli, url); err != nil || resp.StatusCode != 200 {
						failed.Add(1)
					}
				})
			}
			wg.Wait()
			if failed.Load() != 0 || cli.Dials() != 1 {
				t.Fatalf("failed=%d dials=%d; want 0 and 1", failed.Load(), cli.Dials())
			}
		})
	}
}
