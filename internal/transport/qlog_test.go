package transport

import "testing"

func TestQuicTracerOffByDefault(t *testing.T) {
	t.Setenv("QLOGDIR", "")
	if quicTracer() != nil {
		t.Fatal("quicTracer() != nil without QLOGDIR; benchmark runs must not trace")
	}
	t.Setenv("QLOGDIR", t.TempDir())
	if quicTracer() == nil {
		t.Fatal("quicTracer() == nil with QLOGDIR set")
	}
}
