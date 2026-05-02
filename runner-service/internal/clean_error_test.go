package internal

import "testing"

func TestCleanError_RemovesNoise(t *testing.T) {
	in := "command-line-arguments\nsh: x\nexit status 1\nbinary not found\n---\nuseful message"
	got := CleanError(in)
	if got != "useful message" {
		t.Fatalf("expected cleaned output, got %q", got)
	}
}

func TestCleanErrorLine(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "   ", want: ""},
		{name: "exit status", in: "exit status 1", want: ""},
		{name: "shell noise", in: "sh: python: not found", want: ""},
		{name: "go compile noise", in: "command-line-arguments", want: "compile error"},
		{name: "go file prefix", in: "/tmp/main.go:12:2: undefined: x", want: "undefined: x"},
		{name: "regular error", in: "Traceback line", want: "Traceback line"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := cleanErrorLine(tc.in)
			if got != tc.want {
				t.Fatalf("cleanErrorLine(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

