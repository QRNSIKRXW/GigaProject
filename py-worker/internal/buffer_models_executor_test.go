package internal

import (
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestLimitedBuffer_Truncates(t *testing.T) {
	buf := NewLimitedBuffer(5)

	if _, err := buf.Write([]byte("hello")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if _, err := buf.Write([]byte(" world")); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	got := buf.String()
	if !strings.Contains(got, "hello") {
		t.Fatalf("expected original prefix in buffer, got %q", got)
	}
	if !strings.Contains(got, "...output truncated...") {
		t.Fatalf("expected truncation marker, got %q", got)
	}
}

func TestParseTask(t *testing.T) {
	t.Run("success with explicit language", func(t *testing.T) {
		msg := redis.XMessage{
			Values: map[string]interface{}{
				"id":   "task-1",
				"code": "print(1)",
				"lang": "python",
			},
		}

		task, err := ParseTask(msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task.Id != "task-1" || task.Code != "print(1)" || task.Lang != "python" {
			t.Fatalf("unexpected parsed task: %+v", task)
		}
	})

	t.Run("defaults language when missing", func(t *testing.T) {
		msg := redis.XMessage{
			Values: map[string]interface{}{
				"id":   "task-2",
				"code": "print(2)",
			},
		}

		task, err := ParseTask(msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task.Lang != "golang" {
			t.Fatalf("expected default language golang, got %q", task.Lang)
		}
	})

	t.Run("type mismatch", func(t *testing.T) {
		msg := redis.XMessage{
			Values: map[string]interface{}{
				"id":   1,
				"code": "x",
			},
		}

		if _, err := ParseTask(msg); err == nil {
			t.Fatal("expected parse error for invalid id type")
		}
	})
}

func TestContainsUserError(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{in: "ok output", want: false},
		{in: "panic: boom", want: true},
		{in: "Traceback (most recent call last):", want: true},
		{in: "compile error: undefined x", want: true},
	}

	for _, tc := range tests {
		got := containsUserError(tc.in)
		if got != tc.want {
			t.Fatalf("containsUserError(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestExtractErrorLine(t *testing.T) {
	out := "line1\nTraceback (most recent call last):\nline3"
	got := extractErrorLine(out)
	if got != "Traceback (most recent call last):" {
		t.Fatalf("expected traceback line, got %q", got)
	}
}

