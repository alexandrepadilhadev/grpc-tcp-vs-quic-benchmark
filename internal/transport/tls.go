package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func serverTLSConfig(t Transport, certFile, keyFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("%w: load key pair: %w", ErrTLSConfig, err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{t.ALPN()},
	}, nil
}

func clientTLSConfig(t Transport, caFile string) (*tls.Config, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("%w: read CA: %w", ErrTLSConfig, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%w: no certificates in %s", ErrTLSConfig, caFile)
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    pool,
		NextProtos: []string{t.ALPN()},
	}, nil
}
