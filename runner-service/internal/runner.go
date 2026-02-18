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

	// ТАЙМАУТ ТОЛЬКО ДЛЯ КОНТЕЙНЕРА
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
			WritePubSub(client, context.Background(), string(out), id)
			return "error", "build failed"
		}
	}

	// 3. Команда docker run
	args, containerName, err := CreateRequest(dir, filename, lang, id)
	if err != nil {
		return "error", err.Error()
	}

	// 4. Запуск контейнера в фоне
	runCmd := exec.Command("docker", args...)
	if out, err := runCmd.CombinedOutput(); err != nil {
		return "error", fmt.Sprintf("docker run failed: %v (%s)", err, string(out))
	}

	// 5. attach вместо logs -f
	logsCtx, logsCancel := context.WithCancel(context.Background())
	defer logsCancel()

	logsCmd := exec.CommandContext(logsCtx, "docker", "attach", "--no-stdin", containerName)

	stdoutPipe, err := logsCmd.StdoutPipe()
	if err != nil {
		return "error", "failed to get stdout"
	}
	stderrPipe, err := logsCmd.StderrPipe()
	if err != nil {
		return "error", "failed to get stderr"
	}

	if err := logsCmd.Start(); err != nil {
		return "error", "docker attach failed"
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// stdout
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			WritePubSub(client, context.Background(), scanner.Text(), id)
		}
	}()

	// stderr
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			WritePubSub(client, context.Background(), scanner.Text(), id)
		}
	}()

	// 6. Ждём завершения контейнера
	waitCmd := exec.Command("docker", "wait", containerName)
	waitDone := make(chan error, 1)

	go func() {
		waitDone <- waitCmd.Run()
	}()

	select {
	case <-execCtx.Done():
		// ТАЙМАУТ — убиваем контейнер
		exec.Command("docker", "kill", containerName).Run()
		exec.Command("docker", "rm", "-f", containerName).Run()

		logsCancel()
		wg.Wait()

		return "error", "timeout"

	case err := <-waitDone:
		// Контейнер завершился сам
		logsCancel()
		wg.Wait()

		exec.Command("docker", "rm", "-f", containerName).Run()

		if err != nil {
			return "done", ""
		}
		return "done", ""
	}
}
