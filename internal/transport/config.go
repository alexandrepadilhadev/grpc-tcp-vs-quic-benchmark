package transport

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"time"
)

// Role selects which Config fields are registered and validated.
type Role uint8

const (
	RoleServer Role = 1 << iota
	RoleClient
)

// Config is shared by a service's server and all of its clients,
// so a single TRANSPORT switches the whole stack.
type Config struct {
	Transport            Transport     // both
	HandshakeTimeout     time.Duration // both
	IdleTimeout          time.Duration // both
	Addr                 string        // server: listen address
	CertFile             string        // server
	KeyFile              string        // server
	MaxConcurrentStreams uint32        // server
	ShutdownTimeout      time.Duration // server
	CAFile               string        // client

	envErr error // invalid env values, reported by Validate
}

// DefaultConfig returns the defaults used when neither flag nor env is set.
func DefaultConfig() Config {
	return Config{
		Transport:            H2,
		HandshakeTimeout:     5 * time.Second,
		IdleTimeout:          30 * time.Second,
		Addr:                 ":8443",
		MaxConcurrentStreams: 1000,
		ShutdownTimeout:      10 * time.Second,
	}
}

// RegisterFlags binds the fields of the given roles to fs.
// Precedence: flag > env > current value (default).
func (c *Config) RegisterFlags(fs *flag.FlagSet, r Role) {
	var errs []error
	env := func(name string, parse func(string) error) {
		if v, ok := os.LookupEnv(name); ok {
			if err := parse(v); err != nil {
				errs = append(errs, fmt.Errorf("%w: %s=%q: %v", ErrInvalidConfig, name, v, err))
			}
		}
	}
	str := func(p *string) func(string) error { return func(v string) error { *p = v; return nil } }
	dur := func(p *time.Duration) func(string) error {
		return func(v string) (err error) { *p, err = time.ParseDuration(v); return err }
	}

	if r&(RoleServer|RoleClient) != 0 {
		env("TRANSPORT", c.Transport.Set)
		env("HANDSHAKE_TIMEOUT", dur(&c.HandshakeTimeout))
		env("IDLE_TIMEOUT", dur(&c.IdleTimeout))
		fs.Var(&c.Transport, "transport", "h2 or h3 [TRANSPORT]")
		fs.DurationVar(&c.HandshakeTimeout, "handshake-timeout", c.HandshakeTimeout, "TCP+TLS or QUIC handshake timeout [HANDSHAKE_TIMEOUT]")
		fs.DurationVar(&c.IdleTimeout, "idle-timeout", c.IdleTimeout, "idle connection timeout [IDLE_TIMEOUT]")
	}
	if r&RoleServer != 0 {
		streams := func(v string) error {
			n, err := strconv.ParseUint(v, 10, 32)
			c.MaxConcurrentStreams = uint32(n)
			return err
		}
		env("ADDR", str(&c.Addr))
		env("TLS_CERT", str(&c.CertFile))
		env("TLS_KEY", str(&c.KeyFile))
		env("MAX_CONCURRENT_STREAMS", streams)
		env("SHUTDOWN_TIMEOUT", dur(&c.ShutdownTimeout))
		fs.StringVar(&c.Addr, "addr", c.Addr, "listen address [ADDR]")
		fs.StringVar(&c.CertFile, "tls-cert", c.CertFile, "server certificate PEM [TLS_CERT]")
		fs.StringVar(&c.KeyFile, "tls-key", c.KeyFile, "server key PEM [TLS_KEY]")
		fs.Func("max-streams", fmt.Sprintf("max concurrent streams per connection (default %d) [MAX_CONCURRENT_STREAMS]", c.MaxConcurrentStreams), streams)
		fs.DurationVar(&c.ShutdownTimeout, "shutdown-timeout", c.ShutdownTimeout, "graceful shutdown timeout [SHUTDOWN_TIMEOUT]")
	}
	if r&RoleClient != 0 {
		env("TLS_CA", str(&c.CAFile))
		fs.StringVar(&c.CAFile, "tls-ca", c.CAFile, "CA certificate PEM [TLS_CA]")
	}
	c.envErr = errors.Join(errs...)
}

// Validate checks the fields of the given roles. All problems are joined;
// each wraps ErrInvalidConfig.
func (c Config) Validate(r Role) error {
	if r&(RoleServer|RoleClient) == 0 {
		return fmt.Errorf("%w: no role", ErrInvalidConfig)
	}
	errs := []error{c.envErr}
	bad := func(format string, a ...any) {
		errs = append(errs, fmt.Errorf("%w: "+format, append([]any{ErrInvalidConfig}, a...)...))
	}
	if _, err := Parse(string(c.Transport)); err != nil {
		bad("transport/TRANSPORT: %v", err)
	}
	if c.HandshakeTimeout <= 0 {
		bad("handshake-timeout/HANDSHAKE_TIMEOUT must be > 0")
	}
	if c.IdleTimeout <= 0 {
		bad("idle-timeout/IDLE_TIMEOUT must be > 0")
	}
	if r&RoleServer != 0 {
		if _, port, err := net.SplitHostPort(c.Addr); err != nil {
			bad("addr/ADDR %q: %v", c.Addr, err)
		} else if _, err := strconv.ParseUint(port, 10, 16); err != nil {
			bad("addr/ADDR %q: invalid port", c.Addr)
		}
		if c.CertFile == "" {
			bad("tls-cert/TLS_CERT is required")
		}
		if c.KeyFile == "" {
			bad("tls-key/TLS_KEY is required")
		}
		if c.MaxConcurrentStreams == 0 || c.MaxConcurrentStreams > math.MaxInt32 {
			bad("max-streams/MAX_CONCURRENT_STREAMS must be in 1..%d", math.MaxInt32)
		}
		if c.ShutdownTimeout <= 0 {
			bad("shutdown-timeout/SHUTDOWN_TIMEOUT must be > 0")
		}
	}
	if r&RoleClient != 0 && c.CAFile == "" {
		bad("tls-ca/TLS_CA is required")
	}
	return errors.Join(errs...)
}
