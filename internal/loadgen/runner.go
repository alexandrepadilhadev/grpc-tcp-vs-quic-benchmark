package loadgen

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
)

// lateThreshold marks an RPS-mode request as sent late.
const lateThreshold = time.Millisecond

// Config is the load shape of one run.
type Config struct {
	Concurrency int           `json:"concurrency"`
	RPS         int           `json:"rps"`         // 0 = closed loop
	Duration    time.Duration `json:"duration_ns"` // ignored when Requests > 0
	Warmup      time.Duration `json:"warmup_ns"`
	Requests    int           `json:"requests"` // measured RPCs; 0 = use Duration
	Deadline    time.Duration `json:"deadline_ns"`
	ReqBytes    int           `json:"req_bytes"`
}

// Validate reports every invalid field.
func (c Config) Validate() error {
	var errs []error
	check := func(ok bool, msg string) {
		if !ok {
			errs = append(errs, errors.New(msg))
		}
	}
	check(c.Concurrency > 0, "concurrency must be > 0")
	check(c.RPS >= 0, "rps must be >= 0")
	check(c.Warmup >= 0, "warmup must be >= 0")
	check(c.Deadline > 0, "deadline must be > 0")
	check(c.Requests >= 0, "requests must be >= 0")
	check(c.Requests > 0 || c.Duration > 0, "duration must be > 0 when requests is 0")
	check(c.ReqBytes >= 0, "payload bytes must be >= 0")
	return errors.Join(errs...)
}

// Call performs one RPC and returns the response payload size.
type Call func(ctx context.Context) (respBytes int, err error)

// Result summarizes the measure phase.
type Result struct {
	Start, End time.Time
	Total, OK  int64
	Errors     map[string]int64 // status -> count
	LateStarts int64            // RPS mode: measure slots sent > 1ms after schedule
	Unsent     int64            // RPS mode: measure slots due before end but never sent
}

type runner struct {
	cfg      Config
	call     Call
	rec      *Recorder
	t0       time.Time
	warmEnd  time.Time
	seq      atomic.Uint64
	measured atomic.Int64 // closed loop: measure slots claimed
	late     atomic.Int64

	mu  sync.Mutex
	res Result
}

// Run drives load until the window ends, Requests measured RPCs are done,
// or ctx is canceled. Rows go to rec; the caller closes rec.
func Run(ctx context.Context, cfg Config, call Call, rec *Recorder) Result {
	r := &runner{cfg: cfg, call: call, rec: rec, t0: time.Now()}
	r.warmEnd = r.t0.Add(cfg.Warmup)
	r.res.Errors = map[string]int64{}
	end := r.warmEnd.Add(cfg.Duration)

	var wg sync.WaitGroup
	if cfg.RPS == 0 {
		for w := range cfg.Concurrency {
			wg.Go(func() { r.closedLoop(ctx, w, end) })
		}
	} else {
		jobs := make(chan time.Time)
		for w := range cfg.Concurrency {
			wg.Go(func() {
				for tk := range jobs {
					if r.phase(tk) == PhaseMeasure && time.Since(tk) > lateThreshold {
						r.late.Add(1)
					}
					r.do(ctx, w, tk)
				}
			})
		}
		r.res.Unsent = r.pace(ctx, jobs, end)
		close(jobs)
	}
	wg.Wait()

	r.res.Start, r.res.End, r.res.LateStarts = r.t0, time.Now(), r.late.Load()
	return r.res
}

func (r *runner) closedLoop(ctx context.Context, w int, end time.Time) {
	for ctx.Err() == nil {
		s := time.Now()
		if r.cfg.Requests == 0 && !s.Before(end) {
			return
		}
		if r.cfg.Requests > 0 && r.phase(s) == PhaseMeasure && r.measured.Add(1) > int64(r.cfg.Requests) {
			return
		}
		r.do(ctx, w, s)
	}
}

// pace sends t_k = t0 + k/RPS to workers. When workers are busy it blocks,
// then sends overdue slots back to back. Returns the slots never sent.
func (r *runner) pace(ctx context.Context, jobs chan<- time.Time, end time.Time) int64 {
	interval := time.Second / time.Duration(r.cfg.RPS)
	var endC <-chan time.Time
	if r.cfg.Requests == 0 {
		endC = time.After(time.Until(end))
	}
	measured := 0
	k := 0
	for ; ; k++ {
		tk := r.t0.Add(time.Duration(k) * interval)
		if r.cfg.Requests == 0 && !tk.Before(end) {
			return 0
		}
		if r.cfg.Requests > 0 && r.phase(tk) == PhaseMeasure {
			if measured++; measured > r.cfg.Requests {
				return 0
			}
		}
		timer := time.NewTimer(time.Until(tk))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return 0
		case <-endC:
			timer.Stop()
			return r.unsent(k, end, interval)
		}
		select {
		case jobs <- tk:
		case <-ctx.Done():
			return 0
		case <-endC:
			return r.unsent(k, end, interval)
		}
	}
}

// unsent is the number of measure slots from k on scheduled before end;
// warm-up slots left when the window ends are not counted.
func (r *runner) unsent(k int, end time.Time, interval time.Duration) int64 {
	return r.due(end, interval) - max(int64(k), r.due(r.warmEnd, interval))
}

// due is the number of slots scheduled before end.
func (r *runner) due(end time.Time, interval time.Duration) int64 {
	return int64((end.Sub(r.t0) + interval - 1) / interval)
}

func (r *runner) phase(t time.Time) Phase {
	if t.Before(r.warmEnd) {
		return PhaseWarmup
	}
	return PhaseMeasure
}

// do runs one RPC; latency counts from sched (the planned start).
func (r *runner) do(ctx context.Context, w int, sched time.Time) {
	seq := r.seq.Add(1) - 1
	cctx, cancel := context.WithTimeout(ctx, r.cfg.Deadline)
	n, err := r.call(cctx)
	cancel()
	lat := time.Since(sched)
	status := statusOf(err)
	phase := r.phase(sched)
	r.rec.Record(Record{
		Seq:       seq,
		Worker:    w,
		StartNs:   r.t0.UnixNano() + int64(sched.Sub(r.t0)),
		LatencyUs: lat.Microseconds(),
		Status:    status,
		ReqBytes:  r.cfg.ReqBytes,
		RespBytes: n,
		Phase:     phase,
	})
	if phase != PhaseMeasure {
		return
	}
	r.mu.Lock()
	r.res.Total++
	if err == nil {
		r.res.OK++
	} else {
		r.res.Errors[status]++
	}
	r.mu.Unlock()
}

// statusOf maps an RPC error to its gRPC code name ("ok" for nil).
func statusOf(err error) string {
	var ce *connect.Error
	switch {
	case err == nil:
		return "ok"
	case errors.As(err, &ce):
		return ce.Code().String()
	case errors.Is(err, context.DeadlineExceeded):
		return connect.CodeDeadlineExceeded.String()
	case errors.Is(err, context.Canceled):
		return connect.CodeCanceled.String()
	}
	return fmt.Sprint(connect.CodeOf(err))
}
