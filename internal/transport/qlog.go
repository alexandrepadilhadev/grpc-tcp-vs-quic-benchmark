package transport

import (
	"context"
	"os"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

// quicTracer writes a qlog trace per QUIC connection to $QLOGDIR (diagnostics
// only: tracing costs CPU). Without QLOGDIR it returns nil and nothing is traced.
func quicTracer() func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace {
	if os.Getenv("QLOGDIR") == "" {
		return nil
	}
	return qlog.DefaultConnectionTracer
}
