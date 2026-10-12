// Command loadgen drives PaymentService.CreatePayment over h2 or h3 and records
// one CSV row per request plus a meta.json.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	randv2 "math/rand/v2"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	paymentv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1/paymentv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/loadgen"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
)

var version = "dev" // set with -ldflags "-X main.version=..."

// amountCents is the fixed amount of every payment (spec §6.4).
const amountCents = 1000

// logLevel is the level of main's logger; run sets it from --log-level.
var logLevel = new(slog.LevelVar)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	if err := run(ctx, os.Args[1:], log); err != nil {
		log.Error("loadgen failed", "error", err.Error())
		os.Exit(1)
	}
}

// newPayment is a new payment from a random account in 1..accounts.
func newPayment(accounts int) *paymentv1.CreatePaymentRequest {
	id := make([]byte, 16)
	_, _ = rand.Read(id)
	return &paymentv1.CreatePaymentRequest{
		PaymentId:   hex.EncodeToString(id),
		AccountId:   uint32(1 + randv2.IntN(accounts)),
		AmountCents: amountCents,
	}
}

func run(ctx context.Context, args []string, log *slog.Logger) error {
	tcfg := transport.DefaultConfig()
	lcfg := loadgen.Config{}
	var target, out string
	var accounts int
	var strict bool

	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	tcfg.RegisterFlags(fs, transport.RoleClient)
	fs.StringVar(&target, "target", "gateway:8443", "gateway host:port")
	fs.IntVar(&lcfg.Concurrency, "concurrency", 50, "requests in flight (workers)")
	fs.IntVar(&lcfg.RPS, "rps", 0, "0 = closed loop; >0 = fixed global rate")
	fs.DurationVar(&lcfg.Duration, "duration", 60*time.Second, "measured window")
	fs.IntVar(&lcfg.Requests, "requests", 0, ">0 = stop after N measured RPCs (ignores --duration)")
	fs.DurationVar(&lcfg.Warmup, "warmup", 10*time.Second, "warm-up (phase=warmup in the CSV)")
	fs.IntVar(&accounts, "accounts", 10000, "accounts 1..N to draw from (the ledger's seed)")
	fs.DurationVar(&lcfg.Deadline, "deadline", 2*time.Second, "per-RPC timeout")
	fs.BoolVar(&strict, "strict", false, "exit 1 on any non-ok RPC or more than one connection")
	fs.StringVar(&out, "out", "/results/requests.csv", "requests.csv path")
	level := slog.LevelInfo
	var levelErr error
	if v, ok := os.LookupEnv("LOG_LEVEL"); ok {
		if err := level.UnmarshalText([]byte(v)); err != nil {
			levelErr = fmt.Errorf("env LOG_LEVEL: %w", err)
		}
	}
	fs.TextVar(&level, "log-level", level, "debug|info|warn|error; debug logs every payment [LOG_LEVEL]")
	if err := fs.Parse(args); err != nil {
		return err
	}
	logSet := false
	fs.Visit(func(f *flag.Flag) { logSet = logSet || f.Name == "log-level" })

	lcfg.ReqBytes = proto.Size(newPayment(max(accounts, 1)))
	errs := []error{tcfg.Validate(transport.RoleClient), lcfg.Validate()}
	if levelErr != nil && !logSet {
		errs = append(errs, levelErr)
	}
	if _, _, err := net.SplitHostPort(target); err != nil {
		errs = append(errs, fmt.Errorf("target %q: %w", target, err))
	}
	if accounts < 1 {
		errs = append(errs, errors.New("accounts must be > 0"))
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	logLevel.Set(level)
	debug := log.Enabled(ctx, slog.LevelDebug)

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
	gw := paymentv1connect.NewPaymentServiceClient(cli, "https://"+target,
		connect.WithGRPC(), connect.WithAcceptCompression("gzip", nil, nil))

	// pay sends one payment; a non-SETTLED payment is an error, so it is never counted ok.
	pay := func(ctx context.Context) (*connect.Response[paymentv1.CreatePaymentResponse], error) {
		msg := newPayment(accounts)
		start := time.Now()
		if debug {
			log.DebugContext(ctx, "payment request", "payment_id", msg.GetPaymentId(),
				"account_id", msg.GetAccountId(), "amount_cents", msg.GetAmountCents())
		}
		resp, err := gw.CreatePayment(ctx, connect.NewRequest(msg))
		if err == nil && resp.Msg.GetStatus() != paymentv1.PaymentStatus_PAYMENT_STATUS_SETTLED {
			err = connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("payment %s", resp.Msg.GetStatus()))
		}
		if debug {
			attrs := []any{"payment_id", msg.GetPaymentId(),
				"latency_ms", float64(time.Since(start).Microseconds()) / 1000, "code", codeName(err)}
			if resp != nil {
				attrs = append(attrs, "status", resp.Msg.GetStatus().String(), "auth_code", resp.Msg.GetAuthCode())
			}
			log.DebugContext(ctx, "payment response", attrs...)
		}
		return resp, err
	}

	// Probe: establishes the single connection (and the chain behind the
	// gateway) and checks the served protocol.
	pctx, cancel := context.WithTimeout(ctx, tcfg.HandshakeTimeout+lcfg.Deadline)
	start := time.Now()
	resp, err := pay(pctx)
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
		r, err := pay(ctx)
		if err != nil {
			return 0, err
		}
		return proto.Size(r.Msg), nil
	}, rec)
	if err := rec.Close(); err != nil {
		return fmt.Errorf("write %s: %w", out, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("write %s: %w", out, err)
	}

	meta := loadgen.Meta{
		Version: version, Transport: tcfg.Transport.String(), Target: target,
		TransportCfg: tcfg, Load: lcfg,
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

// codeName is the gRPC code of err as the CSV writes it ("ok" for nil).
func codeName(err error) string {
	if err == nil {
		return "ok"
	}
	return connect.CodeOf(err).String()
}
