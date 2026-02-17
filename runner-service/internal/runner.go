package internal

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/redis/go-redis/v9"
)

func RunDocker(client *redis.Client, ctx context.Context, code string, id string, lang string) (string, string) {

	dir, err := os.MkdirTemp("", "run-*")
	if err != nil {
		return "error", "internal error"
	}
	defer os.RemoveAll(dir)

	filename := "main.go"
	if lang == "python" {
		filename = "main.py"
	}

	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, []byte(code), 0644); err != nil {
		return "error", "internal error"
	}

	// если Go — компилируем заранее (как мы обсуждали)
	if lang == "golang" {
		buildCmd := exec.CommandContext(ctx, "go", "build", "-o", filepath.Join(dir, "app"), path)
		buildCmd.Env = append(os.Environ(),
			"GOOS=linux",
			"GOARCH=arm64", // или arm64
		)

		if out, err := buildCmd.CombinedOutput(); err != nil {
			WritePubSub(client, context.Background(), string(out), id)
			return "error", "build failed"
		}
	}

	cmd, err := CreateRequest(ctx, dir, filename, lang)
	if err != nil {
		return "error", err.Error()
	}

	stdoutPipe, _ := cmd.StdoutPipe()
	stderrPipe, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return "error", "docker start failed"
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Println("STDOUT:", line)
			if err := WritePubSub(client, context.Background(), line, id); err != nil {
				fmt.Println("PUBSUB ERROR (stdout):", err)
			}
		}
		if err := scanner.Err(); err != nil {
			fmt.Println("STDOUT SCAN ERROR:", err)
		}
	}()

	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Println("STDERR:", line)
			if err := WritePubSub(client, context.Background(), line, id); err != nil {
				fmt.Println("PUBSUB ERROR (stderr):", err)
			}
		}
		if err := scanner.Err(); err != nil {
			fmt.Println("STDERR SCAN ERROR:", err)
		}
	}()

	err = cmd.Wait()
	wg.Wait() // дожидаемся, пока всё дочитается и допишется в Redis

	if ctx.Err() == context.DeadlineExceeded {
		return "error", "timeout"
	}

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			switch exitErr.ExitCode() {
			case 125, 126, 127:
				return "error", "docker crashed"
			default:
				return "done", ""
			}
		}
		return "error", "docker failed"
	}

	return "done", ""
}
