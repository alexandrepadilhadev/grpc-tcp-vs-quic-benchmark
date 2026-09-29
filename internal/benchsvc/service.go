// Package benchsvc implements bench.v1.BenchService for the POC.
package benchsvc

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"

	benchv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/bench/v1"
)

// MaxResponseBytes caps response_size.
const MaxResponseBytes = 1 << 20

// Service returns slices of a random payload generated once.
type Service struct {
	payload []byte
}

func New() *Service {
	p := make([]byte, MaxResponseBytes)
	_, _ = rand.Read(p) // never fails (crypto/rand contract)
	return &Service{payload: p}
}

// Unary waits server_delay_us and returns response_size bytes.
func (s *Service) Unary(ctx context.Context, req *connect.Request[benchv1.UnaryRequest]) (*connect.Response[benchv1.UnaryResponse], error) {
	n := req.Msg.GetResponseSize()
	if n > MaxResponseBytes {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("response_size %d > %d", n, MaxResponseBytes))
	}
	if d := time.Duration(req.Msg.GetServerDelayUs()) * time.Microsecond; d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			code := connect.CodeCanceled
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				code = connect.CodeDeadlineExceeded
			}
			return nil, connect.NewError(code, ctx.Err())
		}
	}
	return connect.NewResponse(&benchv1.UnaryResponse{Payload: s.payload[:n]}), nil
}
