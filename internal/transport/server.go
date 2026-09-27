package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// HeaderServedProto carries the protocol the server actually used (r.Proto).
const HeaderServedProto = "x-served-proto"

// Server serves one http.Handler over the configured transport.
type Server struct {
	cfg      Config
	addr     net.Addr
	serve    func() error
	shutdown func(context.Context) error
}

// Listen validates cfg, loads TLS and binds the TCP or UDP socket.
// Every response carries HeaderServedProto.
func Listen(cfg Config, h http.Handler, log *slog.Logger) (*Server, error) {
	if err := cfg.Validate(RoleServer); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	tlsCfg, err := serverTLSConfig(cfg.Transport, cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	h = servedProto(h)
	s := &Server{cfg: cfg}

	switch cfg.Transport {
	case H2:
		ln, err := net.Listen("tcp", cfg.Addr)
		if err != nil {
			return nil, fmt.Errorf("transport: listen tcp %s: %w", cfg.Addr, err)
		}
		var p http.Protocols
		p.SetHTTP2(true)
		srv := &http.Server{
			Handler:           h,
			TLSConfig:         tlsCfg,
			Protocols:         &p,
			HTTP2:             &http.HTTP2Config{MaxConcurrentStreams: int(cfg.MaxConcurrentStreams)},
			ReadHeaderTimeout: cfg.HandshakeTimeout, // also bounds the TLS handshake
			IdleTimeout:       cfg.IdleTimeout,
			ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
		}
		s.addr = ln.Addr()
		s.serve = func() error { return srv.ServeTLS(ln, "", "") }
		s.shutdown = srv.Shutdown
	case H3:
		udpAddr, err := net.ResolveUDPAddr("udp", cfg.Addr)
		if err != nil {
			return nil, fmt.Errorf("transport: resolve udp %s: %w", cfg.Addr, err)
		}
		pc, err := net.ListenUDP("udp", udpAddr) // *net.UDPConn keeps GSO/ECN
		if err != nil {
			return nil, fmt.Errorf("transport: listen udp %s: %w", cfg.Addr, err)
		}
		srv := &http3.Server{
			Handler:     h,
			TLSConfig:   tlsCfg,
			IdleTimeout: cfg.IdleTimeout,
			QUICConfig: &quic.Config{
				MaxIncomingStreams:   int64(cfg.MaxConcurrentStreams),
				MaxIdleTimeout:       cfg.IdleTimeout,
				HandshakeIdleTimeout: cfg.HandshakeTimeout,
			},
			Logger: log,
		}
		s.addr = pc.LocalAddr()
		s.serve = func() error { return srv.Serve(pc) }
		s.shutdown = func(ctx context.Context) error {
			defer pc.Close()
			return srv.Shutdown(ctx)
		}
	}
	return s, nil
}

// Addr is the bound address (useful with port 0).
func (s *Server) Addr() net.Addr { return s.addr }

// Serve blocks until ctx is done, then shuts down gracefully within
// ShutdownTimeout. Returns nil on clean shutdown.
func (s *Server) Serve(ctx context.Context) error {
	done := make(chan error, 1)
	go func() { done <- s.serve() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	err := s.shutdown(sctx)
	if serr := <-done; !errors.Is(serr, http.ErrServerClosed) && serr != nil && err == nil {
		err = serr
	}
	return err
}

func servedProto(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(HeaderServedProto, r.Proto)
		next.ServeHTTP(w, r)
	})
}

// VerifyServedProto checks the x-served-proto header against t.
func VerifyServedProto(t Transport, h http.Header) error {
	if got := h.Get(HeaderServedProto); got != t.Proto() {
		return fmt.Errorf("%w: served %q, want %q", ErrProtoMismatch, got, t.Proto())
	}
	return nil
}
