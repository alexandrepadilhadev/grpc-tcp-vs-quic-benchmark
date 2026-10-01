package loadgen

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
)

type row struct {
	seq, worker, startNs, latencyUs int64
	status, phase                   string
}

// runRows executes Run with an in-memory recorder and parses the CSV rows.
func runRows(t *testing.T, ctx context.Context, cfg Config, call Call) ([]row, Result) {
	t.Helper()
	var buf bytes.Buffer
	rec := NewRecorder(&buf, 1024)
	res := Run(ctx, cfg, call, rec)
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
	var rows []row
	for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n")[1:] {
		f := strings.Split(l, ",")
		n := func(i int) int64 { v, _ := strconv.ParseInt(f[i], 10, 64); return v }
		rows = append(rows, row{n(0), n(1), n(2), n(3), f[4], f[7]})
	}
	return rows, res
}

func instant(context.Context) (int, error) { return 10, nil }

func base() Config {
	return Config{Concurrency: 4, Duration: time.Second, Deadline: time.Second, ReqBytes: 8}
}

func TestRequestsExact(t *testing.T) {
	cfg := base()
	cfg.Requests = 100
	rows, res := runRows(t, context.Background(), cfg, instant)
	if len(rows) != 100 || res.OK != 100 || res.Total != 100 {
		t.Fatalf("rows=%d ok=%d total=%d", len(rows), res.OK, res.Total)
	}
	seen := map[int64]bool{}
	for _, r := range rows {
		if seen[r.seq] || r.worker < 0 || r.worker >= 4 || r.phase != "measure" || r.status != "ok" {
			t.Fatalf("bad row %+v", r)
		}
		seen[r.seq] = true
	}
}

func TestWarmupPhases(t *testing.T) {
	cfg := base()
	cfg.Warmup, cfg.Duration = 50*time.Millisecond, 100*time.Millisecond
	rows, res := runRows(t, context.Background(), cfg, func(context.Context) (int, error) {
		time.Sleep(2 * time.Millisecond)
		return 10, nil
	})
	var warm, meas int64
	firstMeasure := int64(1 << 62)
	lastWarm := int64(0)
	for _, r := range rows {
		if r.phase == "warmup" {
			warm++
			lastWarm = max(lastWarm, r.startNs)
		} else {
			meas++
			firstMeasure = min(firstMeasure, r.startNs)
		}
	}
	if warm == 0 || meas == 0 || lastWarm >= firstMeasure || res.Total != meas {
		t.Fatalf("warm=%d meas=%d lastWarm=%d firstMeasure=%d total=%d", warm, meas, lastWarm, firstMeasure, res.Total)
	}
}

func TestStatusMapping(t *testing.T) {
	cfg := base()
	cfg.Requests, cfg.Deadline = 20, 20*time.Millisecond
	var n atomic.Int64
	rows, res := runRows(t, context.Background(), cfg, func(ctx context.Context) (int, error) {
		if n.Add(1)%2 == 0 {
			return 0, connect.NewError(connect.CodeUnavailable, errors.New("down"))
		}
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if res.Errors["unavailable"] != 10 || res.Errors["deadline_exceeded"] != 10 || res.OK != 0 {
		t.Fatalf("errors = %v ok = %d", res.Errors, res.OK)
	}
	for _, r := range rows {
		if r.status == "deadline_exceeded" && r.latencyUs < 20_000 {
			t.Fatalf("deadline row latency %dus < 20ms", r.latencyUs)
		}
	}
}

func TestRPSSchedule(t *testing.T) {
	cfg := base()
	cfg.RPS, cfg.Duration = 200, 300*time.Millisecond
	rows, res := runRows(t, context.Background(), cfg, instant)
	if int64(len(rows))+res.Unsent != 60 {
		t.Fatalf("rows=%d unsent=%d; want sum 60", len(rows), res.Unsent)
	}
	first := rows[0].startNs
	for _, r := range rows {
		first = min(first, r.startNs)
	}
	for _, r := range rows {
		if (r.startNs-first)%int64(5*time.Millisecond) != 0 {
			t.Fatalf("start_ns %d not on the 5ms schedule", r.startNs-first)
		}
	}
}

func TestCOCorrection(t *testing.T) {
	cfg := base()
	cfg.RPS, cfg.Concurrency, cfg.Requests = 100, 1, 10
	var n atomic.Int64
	rows, res := runRows(t, context.Background(), cfg, func(context.Context) (int, error) {
		if n.Add(1) == 1 {
			time.Sleep(60 * time.Millisecond)
		}
		return 10, nil
	})
	for _, r := range rows {
		if r.seq == 1 && r.latencyUs < 50_000 {
			t.Fatalf("seq 1 latency %dus; want >= 50ms (measured from schedule)", r.latencyUs)
		}
	}
	if res.LateStarts < 4 {
		t.Fatalf("LateStarts = %d; want >= 4", res.LateStarts)
	}
}

func TestUnsentCountsMeasureSlotsOnly(t *testing.T) {
	cfg := base()
	cfg.RPS, cfg.Concurrency = 100, 1
	cfg.Warmup, cfg.Duration = 100*time.Millisecond, 100*time.Millisecond
	// The only worker is stuck past the window, so the pacer never leaves the warm-up.
	_, res := runRows(t, context.Background(), cfg, func(context.Context) (int, error) {
		time.Sleep(300 * time.Millisecond)
		return 10, nil
	})
	if res.Total != 0 || res.Unsent != 10 {
		t.Fatalf("total=%d unsent=%d; want 0 and the 10 measure slots", res.Total, res.Unsent)
	}
}

func TestLateStartsCountMeasureSlotsOnly(t *testing.T) {
	cfg := base()
	cfg.RPS, cfg.Concurrency = 100, 1
	cfg.Warmup, cfg.Duration = 60*time.Millisecond, 50*time.Millisecond
	var n atomic.Int64
	// The first call makes warm-up slots 1-3 late; the pacer catches up before the measure phase.
	_, res := runRows(t, context.Background(), cfg, func(context.Context) (int, error) {
		if n.Add(1) == 1 {
			time.Sleep(35 * time.Millisecond)
		}
		return 10, nil
	})
	if res.LateStarts != 0 || res.Total == 0 {
		t.Fatalf("late=%d total=%d; want no late measure slot", res.LateStarts, res.Total)
	}
}

func TestCancel(t *testing.T) {
	cfg := base()
	cfg.Duration = 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	rows, _ := runRows(t, ctx, cfg, func(context.Context) (int, error) {
		time.Sleep(time.Millisecond)
		return 10, nil
	})
	if d := time.Since(start); d > 300*time.Millisecond || len(rows) == 0 {
		t.Fatalf("took %v with %d rows", d, len(rows))
	}
}

func TestConfigValidate(t *testing.T) {
	if err := base().Validate(); err != nil {
		t.Fatalf("valid: %v", err)
	}
	cases := map[string]func(*Config){
		"concurrency": func(c *Config) { c.Concurrency = 0 },
		"rps":         func(c *Config) { c.RPS = -1 },
		"warmup":      func(c *Config) { c.Warmup = -1 },
		"deadline":    func(c *Config) { c.Deadline = 0 },
		"duration":    func(c *Config) { c.Duration = 0 },
		"requests":    func(c *Config) { c.Requests = -1 },
	}
	for name, mutate := range cases {
		cfg := base()
		mutate(&cfg)
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	cfg := base()
	cfg.Duration, cfg.Requests = 0, 10
	if err := cfg.Validate(); err != nil {
		t.Errorf("duration may be 0 when requests > 0: %v", err)
	}
}
