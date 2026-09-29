package transport

import (
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	for in, want := range map[string]Transport{"h2": H2, "h3": H3} {
		got, err := Parse(in)
		if err != nil || got != want {
			t.Errorf("Parse(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"", "H2", "h1", "quic"} {
		_, err := Parse(in)
		if !errors.Is(err, ErrInvalidTransport) {
			t.Errorf("Parse(%q) err = %v; want ErrInvalidTransport", in, err)
		}
		if err != nil && !strings.Contains(err.Error(), "h2, h3") {
			t.Errorf("Parse(%q) err = %q; want valid values listed", in, err)
		}
	}
}

func TestTransportProps(t *testing.T) {
	cases := []struct {
		t                    Transport
		network, alpn, proto string
	}{
		{H2, "tcp", "h2", "HTTP/2.0"},
		{H3, "udp", "h3", "HTTP/3.0"},
	}
	for _, c := range cases {
		if c.t.Network() != c.network || c.t.ALPN() != c.alpn || c.t.Proto() != c.proto || c.t.String() != c.alpn {
			t.Errorf("%s: got %s/%s/%s/%s", c.t, c.t.Network(), c.t.ALPN(), c.t.Proto(), c.t.String())
		}
	}
}

func TestFlagValue(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var tr Transport
	fs.Var(&tr, "transport", "")
	err := fs.Parse([]string{"--transport=h4"})
	if err == nil || !strings.Contains(err.Error(), "invalid transport") {
		t.Fatalf("err = %v; want invalid transport", err)
	}
}
