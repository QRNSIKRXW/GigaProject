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
				"code": "package main",
				"lang": "golang",
			},
		}

		task, err := ParseTask(msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task.Id != "task-1" || task.Code != "package main" || task.Lang != "golang" {
			t.Fatalf("unexpected parsed task: %+v", task)
		}
	})

	t.Run("defaults language when missing", func(t *testing.T) {
		msg := redis.XMessage{
			Values: map[string]interface{}{
				"id":   "task-2",
				"code": "fmt.Println(1)",
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

func TestExtractErrorLine(t *testing.T) {
	out := "line1\npanic: boom\nline3"
	got := extractErrorLine(out)
	if got != "panic: boom" {
		t.Fatalf("expected panic line, got %q", got)
	}
}

