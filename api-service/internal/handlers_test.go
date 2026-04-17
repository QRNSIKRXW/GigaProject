package internal

import (
	"net/http/httptest"
	"testing"
)

func TestClientIPFromRequest_DirectClientIgnoresSpoofedHeaders(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "203.0.113.10:45678"
	r.Header.Set("X-Real-IP", "8.8.8.8")
	r.Header.Set("X-Forwarded-For", "1.1.1.1, 9.9.9.9")

	got := clientIPFromRequest(r)
	if got != "203.0.113.10" {
		t.Fatalf("expected remote ip, got %q", got)
	}
}

func TestClientIPFromRequest_TrustedProxyUsesRealIP(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "172.18.0.3:8080"
	r.Header.Set("X-Real-IP", "198.51.100.20")

	got := clientIPFromRequest(r)
	if got != "198.51.100.20" {
		t.Fatalf("expected X-Real-IP, got %q", got)
	}
}

func TestClientIPFromRequest_TrustedProxyFallsBackToForwardedFor(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "127.0.0.1:1234"
	r.Header.Set("X-Real-IP", "invalid")
	r.Header.Set("X-Forwarded-For", "bad, 198.51.100.21, 1.1.1.1")

	got := clientIPFromRequest(r)
	if got != "198.51.100.21" {
		t.Fatalf("expected first valid X-Forwarded-For ip, got %q", got)
	}
}
