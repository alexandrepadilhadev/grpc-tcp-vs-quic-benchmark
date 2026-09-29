package transport

import (
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
	"time"
)

func parse(t *testing.T, r Role, args ...string) (Config, *flag.FlagSet) {
	t.Helper()
	cfg := DefaultConfig()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cfg.RegisterFlags(fs, r)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse: %v", err)
	}
	return cfg, fs
}

func TestDefaults(t *testing.T) {
	cfg, _ := parse(t, RoleServer|RoleClient)
	if cfg.Transport != H2 || cfg.Addr != ":8443" || cfg.MaxConcurrentStreams != 1000 ||
		cfg.HandshakeTimeout != 5*time.Second || cfg.IdleTimeout != 30*time.Second ||
		cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("defaults = %+v", cfg)
	}
}

func TestPrecedence(t *testing.T) {
	t.Setenv("TRANSPORT", "h3")
	t.Setenv("IDLE_TIMEOUT", "45s")
	t.Setenv("MAX_CONCURRENT_STREAMS", "10")
	cfg, _ := parse(t, RoleServer)
	if cfg.Transport != H3 || cfg.IdleTimeout != 45*time.Second || cfg.MaxConcurrentStreams != 10 {
		t.Fatalf("env not applied: %+v", cfg)
	}
	cfg, _ = parse(t, RoleServer, "--transport=h2", "--max-streams=20")
	if cfg.Transport != H2 || cfg.MaxConcurrentStreams != 20 {
		t.Fatalf("flag must beat env: %+v", cfg)
	}
}

func TestRoleFlags(t *testing.T) {
	_, fs := parse(t, RoleClient)
	if fs.Lookup("tls-ca") == nil || fs.Lookup("transport") == nil {
		t.Fatal("client flags missing")
	}
	for _, name := range []string{"addr", "tls-cert", "tls-key", "max-streams", "shutdown-timeout"} {
		if fs.Lookup(name) != nil {
			t.Errorf("server flag %q registered for client", name)
		}
	}
}

func TestInvalidEnv(t *testing.T) {
	for env, val := range map[string]string{"TRANSPORT": "h4", "IDLE_TIMEOUT": "abc"} {
		t.Run(env, func(t *testing.T) {
			t.Setenv(env, val)
			cfg, _ := parse(t, RoleClient)
			cfg.CAFile = "ca.crt"
			err := cfg.Validate(RoleClient)
			if !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), env) {
				t.Fatalf("err = %v; want ErrInvalidConfig naming %s", err, env)
			}
		})
	}
}

func validServer() Config {
	cfg := DefaultConfig()
	cfg.CertFile, cfg.KeyFile, cfg.CAFile = "s.crt", "s.key", "ca.crt"
	return cfg
}

func TestValidateServer(t *testing.T) {
	if err := validServer().Validate(RoleServer); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	cfg := validServer()
	cfg.Addr = "127.0.0.1:0"
	if err := cfg.Validate(RoleServer); err != nil {
		t.Fatalf("port 0 must be accepted: %v", err)
	}
	cases := map[string]func(*Config){
		"tls-cert":         func(c *Config) { c.CertFile = "" },
		"tls-key":          func(c *Config) { c.KeyFile = "" },
		"addr":             func(c *Config) { c.Addr = "x" },
		"max-streams":      func(c *Config) { c.MaxConcurrentStreams = 0 },
		"shutdown-timeout": func(c *Config) { c.ShutdownTimeout = 0 },
		"transport":        func(c *Config) { c.Transport = "h4" },
		"idle-timeout":     func(c *Config) { c.IdleTimeout = 0 },
	}
	for name, mutate := range cases {
		cfg := validServer()
		mutate(&cfg)
		err := cfg.Validate(RoleServer)
		if !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), name) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	cfg = validServer()
	cfg.CertFile, cfg.Addr = "", ""
	err := cfg.Validate(RoleServer)
	if !strings.Contains(err.Error(), "tls-cert") || !strings.Contains(err.Error(), "addr") {
		t.Fatalf("errors must be joined: %v", err)
	}
}

func TestValidateClient(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(RoleClient); !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), "tls-ca") {
		t.Fatalf("missing CA: %v", err)
	}
	cfg.CAFile = "ca.crt"
	if err := cfg.Validate(RoleClient); err != nil {
		t.Fatalf("client without cert/key must pass: %v", err)
	}
}

func TestValidateNoRole(t *testing.T) {
	if err := validServer().Validate(0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("err = %v", err)
	}
}
