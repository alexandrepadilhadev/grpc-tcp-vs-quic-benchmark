package loadgen

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
)

// Event names written to events.csv by the migration sequence.
const (
	EventIPAdded       = "ip_added"
	EventMigrateStart  = "migrate_start"
	EventMigrated      = "migrated"
	EventMigrateFailed = "migrate_failed"
	EventIPRemoved     = "ip_removed"
)

// delTimeout bounds the `ip addr del` that runs after ctx is canceled.
const delTimeout = 5 * time.Second

type eventRow struct {
	name string
	ts   int64
}

// Events collects timestamped events for events.csv.
type Events struct {
	mu   sync.Mutex
	rows []eventRow
}

// Add records name at the current Unix time in ns.
func (e *Events) Add(name string) {
	e.mu.Lock()
	e.rows = append(e.rows, eventRow{name, time.Now().UnixNano()})
	e.mu.Unlock()
}

// Has reports whether name was recorded.
func (e *Events) Has(name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range e.rows {
		if r.name == name {
			return true
		}
	}
	return false
}

// WriteCSV writes "event,ts_ns" rows; with no events it writes no file.
func (e *Events) WriteCSV(path string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.rows) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("event,ts_ns\n")
	for _, r := range e.rows {
		b.WriteString(r.name + "," + strconv.FormatInt(r.ts, 10) + "\n")
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// Migration is one client IP change: add the new IP, migrate, drop the old one.
type Migration struct {
	To      netip.Addr
	Timeout time.Duration // Migrate's ctx (= --deadline)
	Local   func(to netip.Addr) (iface string, old netip.Prefix, err error)
	IP      func(ctx context.Context, args ...string) error // runs `ip args...`
	Migrate func(ctx context.Context, local netip.Addr) error
	Log     *slog.Logger
}

// Run performs one migration; migrated reports Migrate's success.
// err is a setup failure (Local or ip); a Migrate failure is an event, not an error.
func (m Migration) Run(ctx context.Context, ev *Events) (migrated bool, err error) {
	log := m.Log
	if log == nil {
		log = slog.Default()
	}
	iface, old, err := m.Local(m.To)
	if err != nil {
		return false, err
	}
	add := netip.PrefixFrom(m.To, old.Bits())
	if err := m.IP(ctx, "addr", "add", add.String(), "dev", iface); err != nil {
		return false, fmt.Errorf("ip addr add: %w", err)
	}
	ev.Add(EventIPAdded)

	ev.Add(EventMigrateStart)
	mctx, cancel := context.WithTimeout(ctx, m.Timeout)
	merr := m.Migrate(mctx, m.To)
	cancel()
	migrated = merr == nil
	if migrated {
		ev.Add(EventMigrated)
	} else {
		ev.Add(EventMigrateFailed)
	}
	log.Info("migration", "to", m.To, "iface", iface, "old", old, "migrated", migrated, "error", merr)

	// The old IP goes away even when Migrate failed or ctx was canceled; bounded so a hung ip cannot stall the run.
	dctx, dcancel := context.WithTimeout(context.WithoutCancel(ctx), delTimeout)
	defer dcancel()
	if err := m.IP(dctx, "addr", "del", old.String(), "dev", iface); err != nil {
		return migrated, fmt.Errorf("ip addr del: %w", err)
	}
	ev.Add(EventIPRemoved)
	return migrated, nil
}

// LocalPrefix finds the interface and IPv4 prefix whose subnet holds to.
func LocalPrefix(to netip.Addr) (string, netip.Prefix, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return "", netip.Prefix{}, err
	}
	addrs := make(map[string][]netip.Prefix)
	for _, ifc := range ifs {
		as, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range as {
			ipn, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip, ok := netip.AddrFromSlice(ipn.IP)
			if !ok || !ip.Unmap().Is4() {
				continue
			}
			ones, _ := ipn.Mask.Size()
			addrs[ifc.Name] = append(addrs[ifc.Name], netip.PrefixFrom(ip.Unmap(), ones))
		}
	}
	return pickPrefix(addrs, to)
}

// pickPrefix returns the interface address in the same subnet as to, skipping to itself.
func pickPrefix(addrs map[string][]netip.Prefix, to netip.Addr) (string, netip.Prefix, error) {
	for name, ps := range addrs {
		for _, p := range ps {
			if p.Addr() != to && p.Masked().Contains(to) {
				return name, p, nil
			}
		}
	}
	return "", netip.Prefix{}, fmt.Errorf("no local interface in the subnet of %s", to)
}

// ExecIP runs `ip args...`; the error carries the command output.
func ExecIP(ctx context.Context, args ...string) error {
	out, err := exec.CommandContext(ctx, "ip", args...).CombinedOutput()
	if err != nil {
		msg := string(bytes.TrimSpace(out))
		if msg == "" {
			return err
		}
		return errors.New(err.Error() + ": " + msg)
	}
	return nil
}

// DialsValid reports whether the meta.json dials value is valid for a run (spec 7.1).
// h3 after a failed Probe may keep its connection (passive server migration): 1 or 1 + migrations.
func DialsValid(t transport.Transport, migrations int, migrated bool, dials int64) bool {
	redial := 1 + int64(migrations)
	switch {
	case migrations == 0:
		return dials == 1
	case t != transport.H3:
		return dials == redial
	case migrated:
		return dials == 1
	default:
		return dials == 1 || dials == redial
	}
}
