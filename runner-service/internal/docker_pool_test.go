package internal

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWaitRunnerReady_SucceedsOnHTTPResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer srv.Close()

	if err := waitRunnerReady(srv.URL, 2*time.Second); err != nil {
		t.Fatalf("expected ready endpoint, got error: %v", err)
	}
}

func TestWaitRunnerReady_TimesOutOnUnavailableEndpoint(t *testing.T) {
	err := waitRunnerReady("http://127.0.0.1:1/exec", 700*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout/error for unavailable endpoint")
	}
}
