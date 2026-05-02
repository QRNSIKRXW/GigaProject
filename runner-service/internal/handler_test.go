package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

type fakeRunner struct {
	status  string
	errMsg  string
	gotCode string
	gotID   string
	gotLang string
}

func (f *fakeRunner) RunDocker(_ *redis.Client, _ context.Context, code string, id string, lang string) (string, string) {
	f.gotCode = code
	f.gotID = id
	f.gotLang = lang
	return f.status, f.errMsg
}

func TestRunHandler_MethodNotAllowed(t *testing.T) {
	runner := &fakeRunner{}
	h := RunHandler(nil, runner)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
}

func TestRunHandler_InvalidJSON(t *testing.T) {
	runner := &fakeRunner{}
	h := RunHandler(nil, runner)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{invalid"))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestRunHandler_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty code", body: `{"taskId":"1","lang":"golang","code":""}`},
		{name: "empty lang", body: `{"taskId":"1","lang":"","code":"package main"}`},
		{name: "empty taskId", body: `{"taskId":"","lang":"golang","code":"package main"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner := &fakeRunner{}
			h := RunHandler(nil, runner)

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", rr.Code)
			}
		})
	}
}

func TestRunHandler_Success(t *testing.T) {
	runner := &fakeRunner{status: "done"}
	h := RunHandler(nil, runner)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(
		`{"taskId":"task-1","lang":"golang","code":"package main"}`,
	))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json content-type, got %q", ct)
	}

	var res RunResponse
	if err := json.NewDecoder(rr.Body).Decode(&res); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if res.Status != "done" {
		t.Fatalf("expected status done, got %q", res.Status)
	}
	if runner.gotCode != "package main" || runner.gotID != "task-1" || runner.gotLang != "golang" {
		t.Fatalf("runner got unexpected args: code=%q id=%q lang=%q", runner.gotCode, runner.gotID, runner.gotLang)
	}
}

func TestRunHandler_PropagatesRunnerError(t *testing.T) {
	runner := &fakeRunner{status: "error", errMsg: "compile failed"}
	h := RunHandler(nil, runner)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(
		`{"taskId":"task-2","lang":"python","code":"print(1)"}`,
	))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var res RunResponse
	if err := json.NewDecoder(rr.Body).Decode(&res); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if res.Status != "error" {
		t.Fatalf("expected status error, got %q", res.Status)
	}
	if res.Error != "compile failed" {
		t.Fatalf("expected propagated error, got %q", res.Error)
	}
}

