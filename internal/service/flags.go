// Package service holds what the payment service binaries share: flags,
// logging, the gRPC server and clients, health checks and the shutdown report.
package service

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
)

// Flags binds flags whose defaults come from the environment.
// Precedence: flag > env > default.
type Flags struct {
	fs      *flag.FlagSet
	envErrs map[string]error // by flag name; reported unless the flag is set
}

func NewFlags(name string) *Flags {
	return &Flags{fs: flag.NewFlagSet(name, flag.ContinueOnError), envErrs: map[string]error{}}
}

func (f *Flags) FlagSet() *flag.FlagSet { return f.fs }

// env applies env to the default through parse, remembering a parse error.
func (f *Flags) env(name, env string, parse func(string) error) {
	if v, ok := os.LookupEnv(env); ok {
		if err := parse(v); err != nil {
			f.envErrs[name] = fmt.Errorf("env %s=%q: %w", env, v, err)
		}
	}
}

func (f *Flags) String(p *string, name, env, def, usage string) {
	*p = def
	f.env(name, env, func(v string) error { *p = v; return nil })
	f.fs.StringVar(p, name, *p, usage+" ["+env+"]")
}

func (f *Flags) Duration(p *time.Duration, name, env string, def time.Duration, usage string) {
	*p = def
	f.env(name, env, func(v string) (err error) { *p, err = time.ParseDuration(v); return err })
	f.fs.DurationVar(p, name, *p, usage+" ["+env+"]")
}

func (f *Flags) Int(p *int, name, env string, def int, usage string) {
	*p = def
	f.env(name, env, func(v string) (err error) { *p, err = strconv.Atoi(v); return err })
	f.fs.IntVar(p, name, *p, usage+" ["+env+"]")
}

func (f *Flags) Level(p *slog.Level, name, env string, def slog.Level, usage string) {
	*p = def
	f.env(name, env, func(v string) error { return p.UnmarshalText([]byte(v)) })
	f.fs.TextVar(p, name, *p, usage+" ["+env+"]")
}

// Parse parses args; an invalid env value is an error unless its flag was set.
func (f *Flags) Parse(args []string) error {
	if err := f.fs.Parse(args); err != nil {
		return err
	}
	set := map[string]bool{}
	f.fs.Visit(func(fl *flag.Flag) { set[fl.Name] = true })
	var errs []error
	for name, err := range f.envErrs {
		if !set[name] {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Common are the flags every service binary has.
type Common struct {
	Transport transport.Config // RoleServer|RoleClient; zero value when r == 0
	Level     slog.Level       // --log-level / LOG_LEVEL, default info
	RepDir    string           // --rep-dir / REP_DIR, "" = no report
}

// RegisterCommon binds the shared flags; r == 0 skips the transport flags (risk).
func RegisterCommon(f *Flags, r transport.Role) *Common {
	c := &Common{}
	if r != 0 {
		c.Transport = transport.DefaultConfig()
		c.Transport.RegisterFlags(f.fs, r)
	}
	f.Level(&c.Level, "log-level", "LOG_LEVEL", slog.LevelInfo, "debug|info|warn|error; debug logs every step")
	f.String(&c.RepDir, "rep-dir", "REP_DIR", "", "directory for svc-<name>.json at shutdown; empty = none")
	return c
}
