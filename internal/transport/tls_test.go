package transport

import (
	"crypto/tls"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport/transporttest"
)

func TestServerTLS(t *testing.T) {
	cert, key, _ := transporttest.NewCertFiles(t)
	for _, tr := range []Transport{H2, H3} {
		cfg, err := serverTLSConfig(tr, cert, key)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MinVersion != tls.VersionTLS13 || !slices.Equal(cfg.NextProtos, []string{tr.ALPN()}) || len(cfg.Certificates) != 1 {
			t.Errorf("%s: MinVersion=%x NextProtos=%v certs=%d", tr, cfg.MinVersion, cfg.NextProtos, len(cfg.Certificates))
		}
	}
}

func TestClientTLS(t *testing.T) {
	_, _, ca := transporttest.NewCertFiles(t)
	for _, tr := range []Transport{H2, H3} {
		cfg, err := clientTLSConfig(tr, ca)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MinVersion != tls.VersionTLS13 || cfg.RootCAs == nil || !slices.Equal(cfg.NextProtos, []string{tr.ALPN()}) {
			t.Errorf("%s: MinVersion=%x RootCAs=%v NextProtos=%v", tr, cfg.MinVersion, cfg.RootCAs, cfg.NextProtos)
		}
	}
}

func TestTLSErrors(t *testing.T) {
	cert, _, _ := transporttest.NewCertFiles(t)
	_, otherKey, _ := transporttest.NewCertFiles(t)
	garbage := filepath.Join(t.TempDir(), "garbage.pem")
	if err := os.WriteFile(garbage, []byte("not a pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing.pem")

	cases := map[string]error{}
	_, cases["missing cert"] = serverTLSConfig(H2, missing, otherKey)
	_, cases["mismatched key"] = serverTLSConfig(H2, cert, otherKey)
	_, cases["garbage CA"] = clientTLSConfig(H2, garbage)
	_, cases["missing CA"] = clientTLSConfig(H2, missing)
	for name, err := range cases {
		if !errors.Is(err, ErrTLSConfig) {
			t.Errorf("%s: err = %v; want ErrTLSConfig", name, err)
		}
	}
}

func TestRejectsTLS12(t *testing.T) {
	cert, key, ca := transporttest.NewCertFiles(t)
	srvCfg, err := serverTLSConfig(H2, cert, key)
	if err != nil {
		t.Fatal(err)
	}
	cliCfg, err := clientTLSConfig(H2, ca)
	if err != nil {
		t.Fatal(err)
	}
	cliCfg.MinVersion, cliCfg.MaxVersion, cliCfg.ServerName = tls.VersionTLS12, tls.VersionTLS12, "localhost"

	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go func() { _ = tls.Server(a, srvCfg).Handshake(); a.Close() }()
	if err := tls.Client(b, cliCfg).Handshake(); err == nil {
		t.Fatal("TLS 1.2 handshake succeeded; want failure")
	}
}
