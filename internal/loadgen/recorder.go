// Package loadgen drives RPC load and records one CSV row per request.
package loadgen

import (
	"bufio"
	"io"
	"strconv"
	"sync/atomic"
)

// Phase marks whether a request belongs to the warm-up or the measured window.
type Phase string

const (
	PhaseWarmup  Phase = "warmup"
	PhaseMeasure Phase = "measure"
)

// Header is the stable requests.csv schema the analysis depends on.
const Header = "seq,worker,start_ns,latency_us,status,req_bytes,resp_bytes,phase"

// Record is one requests.csv row.
type Record struct {
	Seq       uint64
	Worker    int
	StartNs   int64
	LatencyUs int64
	Status    string
	ReqBytes  int
	RespBytes int
	Phase     Phase
}

// Recorder writes records asynchronously through a single writer goroutine.
// Record must not be called after Close.
type Recorder struct {
	ch     chan Record
	done   chan error
	stalls atomic.Int64
}

// NewRecorder starts the writer; buffer is the channel capacity.
func NewRecorder(w io.Writer, buffer int) *Recorder {
	r := &Recorder{ch: make(chan Record, buffer), done: make(chan error, 1)}
	go r.loop(w)
	return r
}

// Record queues rec. It blocks when the buffer is full and never drops rows.
func (r *Recorder) Record(rec Record) {
	select {
	case r.ch <- rec:
	default:
		r.stalls.Add(1)
		r.ch <- rec
	}
}

// Close drains the queue, flushes and returns the first write error.
func (r *Recorder) Close() error {
	close(r.ch)
	return <-r.done
}

// Stalls counts Record calls that found the buffer full.
func (r *Recorder) Stalls() int64 { return r.stalls.Load() }

func (r *Recorder) loop(w io.Writer) {
	bw := bufio.NewWriterSize(w, 64<<10)
	_, err := bw.WriteString(Header + "\n")
	line := make([]byte, 0, 128)
	for rec := range r.ch {
		if err != nil {
			continue // keep draining so producers never block
		}
		line = appendRecord(line[:0], rec)
		_, err = bw.Write(line)
	}
	if err == nil {
		err = bw.Flush()
	}
	r.done <- err
}

func appendRecord(b []byte, r Record) []byte {
	b = strconv.AppendUint(b, r.Seq, 10)
	b = append(b, ',')
	b = strconv.AppendInt(b, int64(r.Worker), 10)
	b = append(b, ',')
	b = strconv.AppendInt(b, r.StartNs, 10)
	b = append(b, ',')
	b = strconv.AppendInt(b, r.LatencyUs, 10)
	b = append(b, ',')
	b = append(b, r.Status...)
	b = append(b, ',')
	b = strconv.AppendInt(b, int64(r.ReqBytes), 10)
	b = append(b, ',')
	b = strconv.AppendInt(b, int64(r.RespBytes), 10)
	b = append(b, ',')
	b = append(b, r.Phase...)
	return append(b, '\n')
}
