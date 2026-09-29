// Package transport serves and calls HTTP handlers over HTTP/2 (TLS/TCP)
// or HTTP/3 (QUIC/UDP), selected by configuration.
package transport

import (
	"errors"
	"fmt"
)

// Transport selects the HTTP version and its underlying protocol.
type Transport string

const (
	H2 Transport = "h2" // HTTP/2 over TLS 1.3 / TCP
	H3 Transport = "h3" // HTTP/3 over QUIC / UDP
)

var (
	ErrInvalidTransport = errors.New("transport: invalid transport")
	ErrInvalidConfig    = errors.New("transport: invalid config")
	ErrTLSConfig        = errors.New("transport: tls config")
	ErrProtoMismatch    = errors.New("transport: protocol mismatch")
)

// Parse validates s and returns the matching Transport.
func Parse(s string) (Transport, error) {
	switch t := Transport(s); t {
	case H2, H3:
		return t, nil
	}
	return "", fmt.Errorf("%w %q (valid: h2, h3)", ErrInvalidTransport, s)
}

// Network is the socket network: "tcp" or "udp".
func (t Transport) Network() string {
	if t == H3 {
		return "udp"
	}
	return "tcp"
}

// ALPN is the TLS application protocol: "h2" or "h3".
func (t Transport) ALPN() string { return string(t) }

// Proto is the expected http.Request.Proto: "HTTP/2.0" or "HTTP/3.0".
func (t Transport) Proto() string {
	if t == H3 {
		return "HTTP/3.0"
	}
	return "HTTP/2.0"
}

func (t Transport) String() string { return string(t) }

// Set implements flag.Value.
func (t *Transport) Set(s string) error {
	v, err := Parse(s)
	if err != nil {
		return err
	}
	*t = v
	return nil
}
