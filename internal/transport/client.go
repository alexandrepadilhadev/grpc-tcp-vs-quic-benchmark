package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// Client uses one multiplexed connection per host; it satisfies connect.HTTPClient.
type Client struct {
	hc    *http.Client
	close func() error
	dials atomic.Int64

	mu      sync.Mutex
	migrate func(context.Context, netip.Addr) error // nil without Config.Migratable
	local   netip.Addr                              // h2: source address of the next dial
	tcp     net.Conn                                // h2: current connection
	qconn   *quic.Conn                              // h3: current connection
	trs     []*quic.Transport                       // h3: closed only by Close
	socks   []*net.UDPConn
}

// NewClient validates cfg and builds an h2 or h3 client.
func NewClient(cfg Config) (*Client, error) {
	if err := cfg.Validate(RoleClient); err != nil {
		return nil, err
	}
	tlsCfg, err := clientTLSConfig(cfg.Transport, cfg.CAFile)
	if err != nil {
		return nil, err
	}
	c := &Client{}

	switch cfg.Transport {
	case H2:
		var p http.Protocols
		p.SetHTTP2(true)
		dial := (&tls.Dialer{Config: tlsCfg}).DialContext
		if cfg.Migratable {
			dial, c.migrate = c.dialH2(tlsCfg), c.migrateH2
		}
		tr := &http.Transport{
			Protocols:          &p,
			HTTP2:              &http.HTTP2Config{StrictMaxConcurrentRequests: true},
			MaxConnsPerHost:    1, // serializes dials so a burst shares one connection (as http3.Transport does)
			IdleConnTimeout:    cfg.IdleTimeout,
			DisableCompression: true,
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				ctx, cancel := context.WithTimeout(ctx, cfg.HandshakeTimeout)
				defer cancel()
				conn, err := dial(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				c.dials.Add(1)
				if got := conn.(*tls.Conn).ConnectionState().NegotiatedProtocol; got != "h2" {
					conn.Close()
					return nil, fmt.Errorf("%w: negotiated %q, want h2", ErrProtoMismatch, got)
				}
				return conn, nil
			},
		}
		c.hc = &http.Client{Transport: tr}
		c.close = func() error { tr.CloseIdleConnections(); return nil }
	case H3:
		// QUIC requires ALPN, so a mismatch cannot pass the handshake; only count dials.
		dial := quic.DialAddrEarly
		if cfg.Migratable {
			dial, c.migrate = c.dialH3, c.migrateH3
		}
		tr := &http3.Transport{
			TLSClientConfig:    tlsCfg,
			QUICConfig:         &quic.Config{HandshakeIdleTimeout: cfg.HandshakeTimeout, MaxIdleTimeout: cfg.IdleTimeout, Tracer: quicTracer()},
			DisableCompression: true,
			Dial: func(ctx context.Context, addr string, tc *tls.Config, qc *quic.Config) (*quic.Conn, error) {
				conn, err := dial(ctx, addr, tc, qc)
				if err == nil {
					c.dials.Add(1)
				}
				return conn, err
			},
		}
		c.hc = &http.Client{Transport: tr}
		c.close = tr.Close
	}
	return c, nil
}

// Do sends req over the configured transport.
func (c *Client) Do(req *http.Request) (*http.Response, error) { return c.hc.Do(req) }

// Dials returns the connections opened so far; a valid run has exactly 1.
func (c *Client) Dials() int64 { return c.dials.Load() }

// Close releases the client's connections.
func (c *Client) Close() error {
	err := c.close()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, tr := range c.trs {
		tr.Close()
	}
	for _, s := range c.socks {
		s.Close()
	}
	c.trs, c.socks = nil, nil
	return err
}

// Migrate moves the client's connection to the local address local.
// h3: the same QUIC connection moves to a new path (AddPath, Probe, Switch).
// h2: the TCP connection is closed and the next request dials from local.
func (c *Client) Migrate(ctx context.Context, local netip.Addr) error {
	if c.migrate == nil {
		return ErrNotMigratable
	}
	return c.migrate(ctx, local)
}

// dialH2 dials from c.local (any address while unset) and keeps the connection.
func (c *Client) dialH2(tlsCfg *tls.Config) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		c.mu.Lock()
		local := c.local
		c.mu.Unlock()
		nd := &net.Dialer{}
		if local.IsValid() {
			nd.LocalAddr = &net.TCPAddr{IP: local.AsSlice()}
		}
		conn, err := (&tls.Dialer{NetDialer: nd, Config: tlsCfg}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		c.tcp = conn
		c.mu.Unlock()
		return conn, nil
	}
}

// migrateH2 sets the source address and closes the connection; the next
// request redials.
func (c *Client) migrateH2(_ context.Context, local netip.Addr) error {
	c.mu.Lock()
	conn := c.tcp
	c.local, c.tcp = local, nil
	c.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	return nil
}

// dialH3 dials from its own Transport: DialAddrEarly uses zero-length
// connection IDs, which leave no ID for a new path.
func (c *Client) dialH3(ctx context.Context, addr string, tc *tls.Config, qc *quic.Config) (*quic.Conn, error) {
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		return nil, err
	}
	tr := &quic.Transport{Conn: udp}
	conn, err := tr.DialEarly(ctx, raddr, tc, qc)
	if err != nil {
		tr.Close()
		udp.Close()
		return nil, err
	}
	c.mu.Lock()
	c.qconn = conn
	c.trs, c.socks = append(c.trs, tr), append(c.socks, udp)
	c.mu.Unlock()
	return conn, nil
}

// migrateH3 probes a path from local and switches to it; on failure the
// connection stays on its current path.
func (c *Client) migrateH3(ctx context.Context, local netip.Addr) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.qconn == nil {
		return ErrNoConnection
	}
	udp, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.AddrPortFrom(local, 0)))
	if err != nil {
		return fmt.Errorf("migrate to %s: %w", local, err)
	}
	tr := &quic.Transport{Conn: udp}
	path, err := c.qconn.AddPath(tr)
	if err != nil {
		tr.Close()
		udp.Close()
		return fmt.Errorf("migrate to %s: %w", local, err)
	}
	// Once probed, tr holds the connection (quic-go has no way to detach it),
	// so closing tr would kill it: tr is kept until Close even on failure.
	c.trs, c.socks = append(c.trs, tr), append(c.socks, udp)
	if err = path.Probe(ctx); err == nil {
		err = path.Switch()
	}
	if err != nil {
		path.Close()
		return fmt.Errorf("migrate to %s: %w", local, err)
	}
	return nil
}
