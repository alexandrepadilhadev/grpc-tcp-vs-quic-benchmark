package loadgen

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
)

var (
	testTo  = netip.MustParseAddr("10.10.0.250")
	testOld = netip.MustParsePrefix("10.10.0.5/24")
)

type migFake struct {
	m        Migration
	ipCalls  [][]string
	migrated []netip.Addr
	deadline time.Time
}

func newMigFake(migrateErr error, failOn string) *migFake {
	f := &migFake{}
	f.m = Migration{
		To:      testTo,
		Timeout: 2 * time.Second,
		Local: func(netip.Addr) (string, netip.Prefix, error) {
			return "eth0", testOld, nil
		},
		IP: func(_ context.Context, args ...string) error {
			f.ipCalls = append(f.ipCalls, args)
			if failOn != "" && args[1] == failOn {
				return errors.New("ip failed")
			}
			return nil
		},
		Migrate: func(ctx context.Context, local netip.Addr) error {
			f.migrated = append(f.migrated, local)
			f.deadline, _ = ctx.Deadline()
			return migrateErr
		},
	}
	return f
}

func eventNames(ev *Events) []string {
	var names []string
	for _, r := range ev.rows {
		names = append(names, r.name)
	}
	return names
}

func TestMigrationOrder(t *testing.T) {
	f := newMigFake(nil, "")
	ev := &Events{}
	start := time.Now()
	migrated, err := f.m.Run(context.Background(), ev)
	if err != nil || !migrated {
		t.Fatalf("migrated=%v err=%v", migrated, err)
	}
	want := []string{EventIPAdded, EventMigrateStart, EventMigrated, EventIPRemoved}
	if got := eventNames(ev); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := 1; i < len(ev.rows); i++ {
		if ev.rows[i].ts < ev.rows[i-1].ts {
			t.Fatalf("ts decreasing at %d", i)
		}
	}
	wantIP := [][]string{
		{"addr", "add", "10.10.0.250/24", "dev", "eth0"},
		{"addr", "del", "10.10.0.5/24", "dev", "eth0"},
	}
	if !reflect.DeepEqual(f.ipCalls, wantIP) {
		t.Fatalf("ip calls = %v, want %v", f.ipCalls, wantIP)
	}
	if len(f.migrated) != 1 || f.migrated[0] != testTo {
		t.Fatalf("Migrate got %v", f.migrated)
	}
	if f.deadline.IsZero() || f.deadline.After(start.Add(f.m.Timeout+time.Second)) {
		t.Fatalf("ctx deadline = %v", f.deadline)
	}
}

func TestMigrationFailedStillRemovesIP(t *testing.T) {
	f := newMigFake(errors.New("probe timeout"), "")
	ev := &Events{}
	migrated, err := f.m.Run(context.Background(), ev)
	if err != nil || migrated {
		t.Fatalf("migrated=%v err=%v", migrated, err)
	}
	want := []string{EventIPAdded, EventMigrateStart, EventMigrateFailed, EventIPRemoved}
	if got := eventNames(ev); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	if len(f.ipCalls) != 2 || f.ipCalls[1][1] != "del" {
		t.Fatalf("ip calls = %v", f.ipCalls)
	}
}

func TestMigrationCanceledStillRemovesIP(t *testing.T) {
	f := newMigFake(nil, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.m.Migrate = func(c context.Context, _ netip.Addr) error {
		cancel()
		return c.Err()
	}
	var delErr error
	var delDeadline time.Time
	f.m.IP = func(c context.Context, args ...string) error {
		f.ipCalls = append(f.ipCalls, args)
		if args[1] == "del" {
			delErr = c.Err()
			delDeadline, _ = c.Deadline()
		}
		return nil
	}
	ev := &Events{}
	migrated, err := f.m.Run(ctx, ev)
	end := time.Now()
	if err != nil || migrated {
		t.Fatalf("migrated=%v err=%v", migrated, err)
	}
	if len(f.ipCalls) != 2 || f.ipCalls[1][1] != "del" || delErr != nil {
		t.Fatalf("ip calls = %v, del ctx err = %v", f.ipCalls, delErr)
	}
	if delDeadline.IsZero() || delDeadline.After(end.Add(5*time.Second)) {
		t.Fatalf("del ctx deadline = %v, want within 5 s", delDeadline)
	}
	names := eventNames(ev)
	if names[len(names)-1] != EventIPRemoved {
		t.Fatalf("events = %v", names)
	}
}

func TestMigrationIPAddFails(t *testing.T) {
	f := newMigFake(nil, "add")
	ev := &Events{}
	if _, err := f.m.Run(context.Background(), ev); err == nil {
		t.Fatal("want error")
	}
	if len(f.migrated) != 0 {
		t.Fatal("Migrate must not be called")
	}
	if len(ev.rows) != 0 {
		t.Fatalf("events = %v", eventNames(ev))
	}
}

func TestMigrationIPDelFails(t *testing.T) {
	f := newMigFake(nil, "del")
	ev := &Events{}
	if _, err := f.m.Run(context.Background(), ev); err == nil {
		t.Fatal("want error")
	}
	names := eventNames(ev)
	if len(names) == 0 || names[len(names)-1] != EventMigrated {
		t.Fatalf("events = %v, want migrated recorded", names)
	}
}

func TestPickPrefix(t *testing.T) {
	addrs := map[string][]netip.Prefix{
		"lo":   {netip.MustParsePrefix("127.0.0.1/8")},
		"eth1": {netip.MustParsePrefix("10.20.0.3/24")},
		"eth0": {testOld},
	}
	iface, p, err := pickPrefix(addrs, testTo)
	if err != nil || iface != "eth0" || p != testOld {
		t.Fatalf("got %q %v %v", iface, p, err)
	}
	if _, _, err := pickPrefix(addrs, netip.MustParseAddr("192.168.1.1")); err == nil {
		t.Fatal("want error without a matching subnet")
	}
	same := map[string][]netip.Prefix{
		"eth0": {netip.MustParsePrefix("10.10.0.250/24")},
	}
	if _, _, err := pickPrefix(same, testTo); err == nil {
		t.Fatal("want error: only address equals to")
	}
}

func TestEventsCSV(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.csv")
	ev := &Events{}
	if err := ev.WriteCSV(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("file must not exist, err=%v", err)
	}
	ev.Add(EventIPAdded)
	ev.Add(EventMigrateStart)
	if err := ev.WriteCSV(path); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("event,ts_ns\nip_added,%d\nmigrate_start,%d\n", ev.rows[0].ts, ev.rows[1].ts)
	if string(b) != want {
		t.Fatalf("csv = %q, want %q", b, want)
	}
	if !strings.HasPrefix(string(b), "event,ts_ns\n") {
		t.Fatal("bad header")
	}
}

func TestDialsValid(t *testing.T) {
	cases := []struct {
		t          transport.Transport
		migrations int
		migrated   bool
		dials      int64
		want       bool
	}{
		{transport.H2, 0, false, 1, true},
		{transport.H2, 0, false, 2, false},
		{transport.H2, 1, false, 2, true},
		{transport.H2, 1, true, 2, true},
		{transport.H2, 1, false, 1, false},
		{transport.H3, 0, false, 1, true},
		{transport.H3, 0, false, 2, false},
		{transport.H3, 1, true, 1, true},
		{transport.H3, 1, true, 2, false},
		{transport.H3, 1, false, 1, true}, // failed Probe, passive server migration
		{transport.H3, 1, false, 2, true},
		{transport.H3, 1, false, 3, false},
	}
	for _, c := range cases {
		if got := DialsValid(c.t, c.migrations, c.migrated, c.dials); got != c.want {
			t.Errorf("(%s,%d,%v,%d) = %v, want %v", c.t, c.migrations, c.migrated, c.dials, got, c.want)
		}
	}
}

func TestEventsHas(t *testing.T) {
	var ev Events
	if ev.Has(EventIPAdded) {
		t.Fatal("empty Events has ip_added")
	}
	ev.Add(EventIPAdded)
	if !ev.Has(EventIPAdded) || ev.Has(EventMigrateStart) {
		t.Fatal("Has wrong after Add(ip_added)")
	}
}
