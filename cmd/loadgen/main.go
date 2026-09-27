// Command loadgen drives BenchService.Unary over h2 or h3 and records
// one CSV row per request plus a meta.json.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"connectrpc.com/connect"

	benchv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/bench/v1"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/bench/v1/benchv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/benchsvc"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/loadgen"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
)

var version = "dev" // set with -ldflags "-X main.version=..."

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(ctx, os.Args[1:], log); err != nil {
		log.Error("loadgen failed", "error", err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, log *slog.Logger) error {
	tcfg := transport.DefaultConfig()
	lcfg := loadgen.Config{}
	var target, out string
	var respBytes int
	var strict bool

	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	tcfg.RegisterFlags(fs, transport.RoleClient)
	fs.StringVar(&target, "target", "server:8443", "server host:port")
	fs.IntVar(&lcfg.Concurrency, "concurrency", 50, "requests in flight (workers)")
	fs.IntVar(&lcfg.RPS, "rps", 0, "0 = closed loop; >0 = fixed global rate")
	fs.DurationVar(&lcfg.Duration, "duration", 60*time.Second, "measured window")
	fs.IntVar(&lcfg.Requests, "requests", 0, ">0 = stop after N measured RPCs (ignores --duration)")
	fs.DurationVar(&lcfg.Warmup, "warmup", 10*time.Second, "warm-up (phase=warmup in the CSV)")
	fs.IntVar(&lcfg.ReqBytes, "payload-bytes", 1024, "request payload size")
	fs.IntVar(&respBytes, "response-bytes", 1024, "response payload size")
	fs.DurationVar(&lcfg.Deadline, "deadline", 2*time.Second, "per-RPC timeout")
	fs.BoolVar(&strict, "strict", false, "exit 1 on any non-ok RPC or more than one connection")
	fs.StringVar(&out, "out", "/results/requests.csv", "requests.csv path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	errs := []error{tcfg.Validate(transport.RoleClient), lcfg.Validate()}
	if _, _, err := net.SplitHostPort(target); err != nil {
		errs = append(errs, fmt.Errorf("target %q: %w", target, err))
	}
	if respBytes < 0 || respBytes > benchsvc.MaxResponseBytes {
		errs = append(errs, fmt.Errorf("response-bytes must be in 0..%d", benchsvc.MaxResponseBytes))
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return fmt.Errorf("out: %w", err)
	}
	f, err := os.Create(out)
	if err != nil {
		return fmt.Errorf("out: %w", err)
	}
	defer f.Close()

	cli, err := transport.NewClient(tcfg)
	if err != nil {
		return err
	}
	defer cli.Close()
	bench := benchv1connect.NewBenchServiceClient(cli, "https://"+target,
		connect.WithGRPC(), connect.WithAcceptCompression("gzip", nil, nil))

	payload := make([]byte, lcfg.ReqBytes)
	_, _ = rand.Read(payload)
	msg := &benchv1.UnaryRequest{Payload: payload, ResponseSize: uint32(respBytes)}

	// Probe: establishes the single connection and checks the served protocol.
	pctx, cancel := context.WithTimeout(ctx, tcfg.HandshakeTimeout+lcfg.Deadline)
	start := time.Now()
	resp, err := bench.Unary(pctx, connect.NewRequest(msg))
	handshake := time.Since(start)
	cancel()
	if err != nil {
		return fmt.Errorf("probe: %w", err)
	}
	if err := transport.VerifyServedProto(tcfg.Transport, resp.Header()); err != nil {
		return fmt.Errorf("probe: %w", err)
	}
	log.Info("probe ok", "transport", tcfg.Transport, "target", target, "handshake_us", handshake.Microseconds())

	rec := loadgen.NewRecorder(f, 1<<16)
	res := loadgen.Run(ctx, lcfg, func(ctx context.Context) (int, error) {
		r, err := bench.Unary(ctx, connect.NewRequest(msg))
		if err != nil {
			return 0, err
		}
		return len(r.Msg.GetPayload()), nil
	}, rec)
	if err := rec.Close(); err != nil {
		return fmt.Errorf("write %s: %w", out, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("write %s: %w", out, err)
	}

	meta := loadgen.Meta{
		Version: version, Transport: tcfg.Transport.String(), Target: target,
		TransportCfg: tcfg, Load: lcfg, ResponseBytes: respBytes,
		Start: res.Start, End: res.End, Interrupted: ctx.Err() != nil, HandshakeUs: handshake.Microseconds(),
		Dials: cli.Dials(), Total: res.Total, OK: res.OK, Errors: res.Errors,
		LateStarts: res.LateStarts, Unsent: res.Unsent, RecorderStalls: rec.Stalls(),
		Sysctl: loadgen.ReadSysctls(),
	}
	if err := loadgen.WriteMeta(filepath.Join(filepath.Dir(out), "meta.json"), meta); err != nil {
		return fmt.Errorf("meta: %w", err)
	}
	log.Info("summary", "total", res.Total, "ok", res.OK, "errors", res.Errors,
		"dials", meta.Dials, "late_starts", res.LateStarts, "unsent", res.Unsent)

	if meta.Interrupted {
		return errors.New("interrupted: results are partial")
	}
	if strict && (res.OK != res.Total || meta.Dials != 1) {
		return fmt.Errorf("strict: ok=%d total=%d dials=%d", res.OK, res.Total, meta.Dials)
	}
	return nil
}
