package loadgen

import (
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
)

// Meta is written to meta.json next to requests.csv.
type Meta struct {
	Version        string            `json:"version"`
	Transport      string            `json:"transport"`
	Target         string            `json:"target"`
	TransportCfg   transport.Config  `json:"transport_config"`
	Load           Config            `json:"load"`
	ResponseBytes  int               `json:"response_bytes"`
	Start          time.Time         `json:"start"`
	End            time.Time         `json:"end"`
	Interrupted    bool              `json:"interrupted"` // stopped by a signal: not a valid repetition
	HandshakeUs    int64             `json:"handshake_us"`
	Dials          int64             `json:"dials"`
	Total          int64             `json:"total"`
	OK             int64             `json:"ok"`
	Errors         map[string]int64  `json:"errors"`
	LateStarts     int64             `json:"late_starts"`
	Unsent         int64             `json:"unsent"`
	RecorderStalls int64             `json:"recorder_stalls"`
	Sysctl         map[string]string `json:"sysctl,omitempty"`
}

// WriteMeta writes m as indented JSON.
func WriteMeta(path string, m Meta) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// ReadSysctls returns the UDP buffer limits; missing /proc entries are skipped.
func ReadSysctls() map[string]string {
	out := map[string]string{}
	for _, name := range []string{"net.core.rmem_max", "net.core.wmem_max"} {
		b, err := os.ReadFile("/proc/sys/" + strings.ReplaceAll(name, ".", "/"))
		if err == nil {
			out[name] = strings.TrimSpace(string(b))
		}
	}
	return out
}
