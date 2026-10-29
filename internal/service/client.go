package service

import (
	"context"
	"log/slog"

	"connectrpc.com/connect"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/rpclog"
	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
)

// NewClient opens one transport.Client (one connection per server) and returns
// the options every service client uses: gRPC protocol, no compression,
// per-RPC lines and the served-protocol check.
func NewClient(cfg transport.Config, log *slog.Logger) (*transport.Client, []connect.ClientOption, error) {
	cli, err := transport.NewClient(cfg)
	if err != nil {
		return nil, nil, err
	}
	return cli, []connect.ClientOption{
		connect.WithGRPC(),
		connect.WithAcceptCompression("gzip", nil, nil),
		connect.WithInterceptors(VerifyProto(cfg.Transport), rpclog.Interceptor(log)),
	}, nil
}

// VerifyProto fails a response whose x-served-proto differs from t with
// connect.CodeInternal wrapping transport.ErrProtoMismatch.
func VerifyProto(t transport.Transport) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			resp, err := next(ctx, req)
			if err != nil {
				return resp, err
			}
			if err := transport.VerifyServedProto(t, resp.Header()); err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			return resp, nil
		}
	})
}
