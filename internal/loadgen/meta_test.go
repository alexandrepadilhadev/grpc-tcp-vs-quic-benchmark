package loadgen

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteMeta(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.json")
	if err := WriteMeta(path, Meta{Transport: "h3", Dials: 1, Errors: map[string]int64{"unavailable": 2}}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"dials", "errors", "transport", "handshake_us", "load", "ok", "total"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing key %q", k)
		}
	}
	if m["transport"] != "h3" || m["dials"] != float64(1) {
		t.Errorf("values: %v", m)
	}
}

func TestReadSysctls(t *testing.T) {
	_ = ReadSysctls() // must not panic without /proc (Windows)
}
