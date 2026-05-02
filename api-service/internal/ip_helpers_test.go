package internal

import "testing"

func TestIsTrustedProxyIP(t *testing.T) {
	if !isTrustedProxyIP("127.0.0.1") {
		t.Fatal("expected loopback to be trusted")
	}
	if !isTrustedProxyIP("192.168.1.10") {
		t.Fatal("expected private address to be trusted")
	}
	if isTrustedProxyIP("8.8.8.8") {
		t.Fatal("expected public address to be untrusted")
	}
}

func TestFirstForwardedIP(t *testing.T) {
	got := firstForwardedIP("bad, 203.0.113.7, 198.51.100.5")
	if got != "203.0.113.7" {
		t.Fatalf("expected first valid forwarded ip, got %q", got)
	}
}

