package transporttest

import "testing"

// SkipH3SwitchUnderRace skips tb in race builds. Use it in any h3 test that
// sends requests across a path switch: quic-go races on it (unfixed upstream).
func SkipH3SwitchUnderRace(tb testing.TB) {
	tb.Helper()
	if Race {
		tb.Skip("quic-go v0.63.0 races on Conn.conn during a path switch (connection.go:921 vs :786); run without -race")
	}
}
