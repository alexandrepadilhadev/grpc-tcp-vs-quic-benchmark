// Package rpclog logs connect RPCs.
package rpclog

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	"github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/internal/transport"
)

// Level is the level of the per-RPC line; off unless the logger enables it.
const Level = slog.LevelDebug

// Interceptor logs one line per unary RPC: side (client|server), procedure,
// code, duration_us, peer, protocol (the RPC protocol), payment_id when the
// request has one and, on the client side, http (the served HTTP version).
// It never changes the response or the error.
func Interceptor(log *slog.Logger) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if !log.Enabled(ctx, Level) {
				return next(ctx, req)
			}
			start := time.Now()
			resp, err := next(ctx, req)
			code := "ok"
			if err != nil {
				code = connect.CodeOf(err).String()
			}
			side := "server"
			if req.Spec().IsClient {
				side = "client"
			}
			attrs := []slog.Attr{
				slog.String("side", side),
				slog.String("procedure", req.Spec().Procedure),
				slog.String("code", code),
				slog.Int64("duration_us", time.Since(start).Microseconds()),
				slog.String("peer", req.Peer().Addr),
				slog.String("protocol", req.Peer().Protocol),
			}
			if m, ok := req.Any().(interface{ GetPaymentId() string }); ok && m.GetPaymentId() != "" {
				attrs = append(attrs, slog.String("payment_id", m.GetPaymentId()))
			}
			// protocol is the RPC protocol (grpc); the HTTP version comes from the
			// response header, which only the client side sees here
			if resp != nil && req.Spec().IsClient {
				if v := resp.Header().Get(transport.HeaderServedProto); v != "" {
					attrs = append(attrs, slog.String("http", v))
				}
			}
			log.LogAttrs(ctx, Level, "rpc", attrs...)
			return resp, err
		}
	})
}
