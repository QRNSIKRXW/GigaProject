package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

var semaphore = make(chan struct{}, 4)

type Request struct {
	Code string `json:"code"`
}

func main() {
	lang := os.Getenv("LANG_TYPE")
	if lang == "" {
		lang = "golang"
	}

	fmt.Printf("Starting API server for %s on :8080\n", lang)

	http.HandleFunc("/exec", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		select {
		case semaphore <- struct{}{}:
			defer func() { <-semaphore }()
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming not supported", 500)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		err := runCodeStream(ctx, w, flusher, req.Code, lang)
		if err != nil {
			fmt.Fprintf(w, "ERR:%v\n", err)
		}

		fmt.Fprintln(w, "END")
		flusher.Flush()
	})

	http.ListenAndServe(":8080", nil)
}

func runCodeStream(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, code, lang string) error {
	tmpDir, err := os.MkdirTemp("/tmp", "run_*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	var cmd *exec.Cmd

	switch lang {
	case "golang":
		mainFile := filepath.Join(tmpDir, "main.go")
		if err := os.WriteFile(mainFile, []byte(code), 0644); err != nil {
			return err
		}

		cmd = exec.CommandContext(ctx, "sh", "-c", fmt.Sprintf(`
cd %s
go build -o main main.go
./main
`, tmpDir))

	case "python":
		mainFile := filepath.Join(tmpDir, "main.py")
		if err := os.WriteFile(mainFile, []byte(code), 0644); err != nil {
			return err
		}

		cmd = exec.CommandContext(ctx, "python3", mainFile)

	default:
		return fmt.Errorf("unknown language")
	}

	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return err
	}

	// stdout
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			fmt.Fprintf(w, "OUT:%s\n", scanner.Text())
			flusher.Flush()
		}
	}()

	// stderr
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			fmt.Fprintf(w, "ERR:%s\n", scanner.Text())
			flusher.Flush()
		}
	}()

	err = cmd.Wait()
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("timeout")
	}

	return err
}
