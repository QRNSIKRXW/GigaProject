package internal

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func RunDocker(client *redis.Client, parentCtx context.Context, code string, id string, lang string) (string, string) {
	execCtx, cancel := context.WithTimeout(parentCtx, 3*time.Second)
	defer cancel()

	// 1. Временная директория
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

	// 2. Компиляция Go
	if lang == "golang" {
		buildCmd := exec.Command("go", "build", "-o", filepath.Join(dir, "app"), path)
		buildCmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64")

		if out, err := buildCmd.CombinedOutput(); err != nil {
			_ = WritePubSub(client, string(out), id)
			return "error", "build failed"
		}
	}

	// 3. docker run
	args, containerName, err := CreateRequest(dir, filename, lang, id)
	if err != nil {
		return "error", err.Error()
	}

	runCmd := exec.Command("docker", args...)
	if out, err := runCmd.CombinedOutput(); err != nil {
		return "error", fmt.Sprintf("docker run failed: %v (%s)", err, string(out))
	}

	// 4. docker logs -f (stdout + stderr)
	logsCmd := exec.Command("docker", "logs", "-f", containerName)

	stdoutPipe, err := logsCmd.StdoutPipe()
	if err != nil {
		return "error", "failed to get stdout"
	}

	stderrPipe, err := logsCmd.StderrPipe()
	if err != nil {
		return "error", "failed to get stderr"
	}

	if err := logsCmd.Start(); err != nil {
		return "error", "docker logs failed"
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// Чтение stdout
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			_ = WritePubSub(client, scanner.Text(), id)
		}
		if err := scanner.Err(); err != nil {
			// log but continue; we don't want to stop execution
			fmt.Println("stdout scanner error:", err)
		}
	}()

	// Чтение stderr (Python ошибки будут здесь)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			_ = WritePubSub(client, scanner.Text(), id)
		}
		if err := scanner.Err(); err != nil {
			fmt.Println("stderr scanner error:", err)
		}
	}()

	// 5. Ждём завершения контейнера
	waitCmd := exec.Command("docker", "wait", containerName)
	waitDone := make(chan error, 1)

	go func() {
		waitDone <- waitCmd.Run()
	}()

	select {
	case <-execCtx.Done():
		exec.Command("docker", "kill", containerName).Run()
		exec.Command("docker", "rm", "-f", containerName).Run()
		return "error", "timeout"

	case <-waitDone:
		// контейнер завершился
	}

	// 6. Останавливаем logs -f
	logsCmd.Process.Kill()
	wg.Wait()

	exec.Command("docker", "rm", "-f", containerName).Run()

	return "done", ""
}
