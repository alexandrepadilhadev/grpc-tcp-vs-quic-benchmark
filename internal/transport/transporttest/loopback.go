package transporttest

import (
	"net"
	"testing"
)

// NeedLoopback2 skips tb unless 127.0.0.2 is bindable for UDP and TCP.
func NeedLoopback2(tb testing.TB) {
	tb.Helper()
	u, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2)})
	if err != nil {
		tb.Skip("127.0.0.2 not bindable")
	}
	u.Close()
	l, err := net.Listen("tcp", "127.0.0.2:0")
	if err != nil {
		tb.Skip("127.0.0.2 not bindable")
	}
	l.Close()
}
