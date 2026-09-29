package rpclog_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"

	benchv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/bench/v1"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/bench/v1/benchv1connect"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/benchsvc"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/rpclog"
)

// call serves BenchService with the interceptor at level and sends one RPC.
func call(t *testing.T, level slog.Level, size uint32) ([]map[string]any, error) {
	t.Helper()
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: level}))
	mux := http.NewServeMux()
	mux.Handle(benchv1connect.NewBenchServiceHandler(benchsvc.New(), connect.WithInterceptors(rpclog.Interceptor(log))))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cli := benchv1connect.NewBenchServiceClient(srv.Client(), srv.URL)
	_, err := cli.Unary(context.Background(), connect.NewRequest(&benchv1.UnaryRequest{ResponseSize: size}))
	var lines []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("bad log line %q: %v", l, err)
		}
		lines = append(lines, m)
	}
	return lines, err
}

func TestLogsOK(t *testing.T) {
	lines, err := call(t, slog.LevelDebug, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 1 {
		t.Fatalf("lines = %v; want 1", lines)
	}
	l := lines[0]
	if l["msg"] != "rpc" || l["level"] != "DEBUG" || l["code"] != "ok" ||
		l["procedure"] != benchv1connect.BenchServiceUnaryProcedure {
		t.Fatalf("line = %v", l)
	}
	if d, ok := l["duration_us"].(float64); !ok || d < 0 {
		t.Fatalf("duration_us = %v", l["duration_us"])
	}
	if l["peer"] == "" || l["protocol"] != connect.ProtocolConnect {
		t.Fatalf("peer/protocol = %v %v", l["peer"], l["protocol"])
	}
}

func TestLogsError(t *testing.T) {
	lines, err := call(t, slog.LevelDebug, benchsvc.MaxResponseBytes+1)
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("client err = %v; want invalid_argument unchanged", err)
	}
	if len(lines) != 1 || lines[0]["code"] != "invalid_argument" || lines[0]["level"] != "DEBUG" {
		t.Fatalf("lines = %v", lines)
	}
}

func TestSilentAtInfo(t *testing.T) {
	for _, size := range []uint32{16, benchsvc.MaxResponseBytes + 1} {
		lines, _ := call(t, slog.LevelInfo, size)
		if len(lines) != 0 {
			t.Fatalf("size %d: lines = %v; want none at info", size, lines)
		}
	}
}
