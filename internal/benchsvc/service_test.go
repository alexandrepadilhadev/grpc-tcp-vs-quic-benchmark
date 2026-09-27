package benchsvc

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"

	benchv1 "github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/bench/v1"
)

func call(ctx context.Context, req *benchv1.UnaryRequest) (*connect.Response[benchv1.UnaryResponse], error) {
	return New().Unary(ctx, connect.NewRequest(req))
}

func TestResponseSize(t *testing.T) {
	for _, n := range []uint32{0, 100} {
		resp, err := call(context.Background(), &benchv1.UnaryRequest{ResponseSize: n})
		if err != nil || len(resp.Msg.Payload) != int(n) {
			t.Fatalf("size %d: got %d bytes, err %v", n, len(resp.Msg.GetPayload()), err)
		}
	}
}

func TestDelay(t *testing.T) {
	start := time.Now()
	if _, err := call(context.Background(), &benchv1.UnaryRequest{ServerDelayUs: 20_000}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 20*time.Millisecond {
		t.Fatalf("took %v; want >= 20ms", d)
	}
}

func TestTooLarge(t *testing.T) {
	_, err := call(context.Background(), &benchv1.UnaryRequest{ResponseSize: MaxResponseBytes + 1})
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("err = %v; want invalid_argument", err)
	}
}

func TestDelayHonorsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := call(ctx, &benchv1.UnaryRequest{ServerDelayUs: 1_000_000})
	if connect.CodeOf(err) != connect.CodeDeadlineExceeded || time.Since(start) > 100*time.Millisecond {
		t.Fatalf("err = %v after %v; want deadline_exceeded quickly", err, time.Since(start))
	}
}
