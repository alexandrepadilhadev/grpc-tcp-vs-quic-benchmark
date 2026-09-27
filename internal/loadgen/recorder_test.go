package loadgen

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestRecorderFormat(t *testing.T) {
	var buf bytes.Buffer
	r := NewRecorder(&buf, 4)
	r.Record(Record{Seq: 1, Worker: 0, StartNs: 100, LatencyUs: 250, Status: "ok", ReqBytes: 1024, RespBytes: 1024, Phase: PhaseMeasure})
	r.Record(Record{Seq: 2, Worker: 1, StartNs: 200, LatencyUs: 300, Status: "unavailable", ReqBytes: 1024, RespBytes: 0, Phase: PhaseWarmup})
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	want := Header + "\n1,0,100,250,ok,1024,1024,measure\n2,1,200,300,unavailable,1024,0,warmup\n"
	if buf.String() != want {
		t.Fatalf("got:\n%s\nwant:\n%s", buf.String(), want)
	}
}

func TestRecorderConcurrent(t *testing.T) {
	var buf bytes.Buffer
	r := NewRecorder(&buf, 16)
	var wg sync.WaitGroup
	for w := range 50 {
		wg.Go(func() {
			for i := range 1000 {
				r.Record(Record{Seq: uint64(w*1000 + i), Worker: w, Status: "ok", Phase: PhaseMeasure})
			}
		})
	}
	wg.Wait()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if lines[0] != Header || len(lines) != 50_001 {
		t.Fatalf("header=%q lines=%d", lines[0], len(lines))
	}
	seen := make(map[string]bool, 50_000)
	for _, l := range lines[1:] {
		f := strings.Split(l, ",")
		if len(f) != 8 || seen[f[0]] {
			t.Fatalf("bad or duplicate line %q", l)
		}
		seen[f[0]] = true
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestRecorderWriteError(t *testing.T) {
	r := NewRecorder(failWriter{}, 1)
	for i := range 10_000 {
		r.Record(Record{Seq: uint64(i), Status: "ok", Phase: PhaseMeasure})
	}
	if err := r.Close(); err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("Close = %v; want write error", err)
	}
}
