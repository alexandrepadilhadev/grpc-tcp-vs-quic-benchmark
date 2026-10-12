package service

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Report is written at shutdown so the analysis can check every hop.
type Report struct {
	Service   string           `json:"service"`
	Version   string           `json:"version"`
	Transport string           `json:"transport"`
	Dials     map[string]int64 `json:"dials"`
	Counters  map[string]int64 `json:"counters"`
}

// WriteReport writes dir/svc-<Service>.json; dir == "" is a no-op.
func WriteReport(dir string, r Report) error {
	if dir == "" {
		return nil
	}
	if r.Dials == nil { // same shape in every file: {} rather than null
		r.Dials = map[string]int64{}
	}
	if r.Counters == nil {
		r.Counters = map[string]int64{}
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "svc-"+r.Service+".json"), append(b, '\n'), 0o644)
}
