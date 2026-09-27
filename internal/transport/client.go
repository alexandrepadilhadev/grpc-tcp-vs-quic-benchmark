package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// Client uses one multiplexed connection per host; it satisfies connect.HTTPClient.
type Client struct {
	hc    *http.Client
	close func() error
	dials atomic.Int64
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
		d := &tls.Dialer{Config: tlsCfg}
		tr := &http.Transport{
			Protocols:          &p,
			HTTP2:              &http.HTTP2Config{StrictMaxConcurrentRequests: true},
			MaxConnsPerHost:    1, // serializes dials so a burst shares one connection (as http3.Transport does)
			IdleConnTimeout:    cfg.IdleTimeout,
			DisableCompression: true,
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				ctx, cancel := context.WithTimeout(ctx, cfg.HandshakeTimeout)
				defer cancel()
				conn, err := d.DialContext(ctx, network, addr)
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
		tr := &http3.Transport{
			TLSClientConfig:    tlsCfg,
			QUICConfig:         &quic.Config{HandshakeIdleTimeout: cfg.HandshakeTimeout, MaxIdleTimeout: cfg.IdleTimeout},
			DisableCompression: true,
			Dial: func(ctx context.Context, addr string, tc *tls.Config, qc *quic.Config) (*quic.Conn, error) {
				conn, err := quic.DialAddrEarly(ctx, addr, tc, qc)
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
func (c *Client) Close() error { return c.close() }
