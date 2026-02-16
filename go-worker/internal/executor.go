package internal

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/redis/go-redis/v9"
)

func ExecuteTask(client *redis.Client, task Task) (result TaskStatus) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dir, err := CreateTempDir()
	if err != nil {
		return TaskStatus{
			Status: "error",
			Result: "",
			Error:  err.Error(),
		}
	}
	defer os.RemoveAll(dir)

	_, err = writeCodeFile(dir, task)
	if err != nil {
		return TaskStatus{
			Status: "error",
			Result: "",
			Error:  err.Error(),
		}
	}

	filename := "main.go"

	stdout, stderr, codeErr := runDocker(client, dir, ctx, filename, task.Id)
	if codeErr != nil {

		return TaskStatus{
			Status: "error",
			Result: "",
			Error:  codeErr.Error(),
		}

	}

	return TaskStatus{
		Status: "done",
		Result: stdout,
		Error:  stderr,
	}

}

func CreateTempDir() (string, error) {

	name := "Playground-*"
	dirname, err := os.MkdirTemp("", name)
	if err != nil {
		return "", err
	}
	return dirname, err

}

func writeCodeFile(dir string, task Task) (string, error) {

	fileName := filepath.Join(dir, ("main.go"))

	if len(task.Code) > MaxCodeSize {
		return "", fmt.Errorf("code size limit exceeded")
	}

	err := os.WriteFile(fileName, []byte(task.Code), 0666)
	if err != nil {
		return "", err
	}

	return fileName, nil

}

func runDocker(client *redis.Client, dir string, ctx context.Context, filename string, id string) (stdout string, stderr string, codeErr error) {

	cmd := createRequest(ctx, dir, filename)

	stdoutPipe, _ := cmd.StdoutPipe()
	stderrPipe, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return "", "", fmt.Errorf("docker start error: %w", err) // потом поменять на internal error
	}

	outBuf := NewLimitedBuffer(1_000_000)
	errBuf := NewLimitedBuffer(1_000_000)

	go func() {
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			middleResult := scanner.Text()
			outBuf.Write([]byte(middleResult + "\n"))
			WritePubSub(client, ctx, middleResult, id)
		}
	}()

	go func() {
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			middleResult := scanner.Text()
			errBuf.Write([]byte(middleResult + "\n"))
			WritePubSub(client, ctx, middleResult, id)
		}
	}()

	err := cmd.Wait()

	if ctx.Err() == context.DeadlineExceeded {
		return "", "timeout", fmt.Errorf("execution timeout")
	}

	stdout = outBuf.String()
	stderr = errBuf.String()

	// Ошибка выполнения контейнера
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {

			switch exitErr.ExitCode() {
			case 125, 126, 127:
				return "", "", fmt.Errorf("docker crashed (exit code %d)", exitErr.ExitCode()) // потом поменять internal error
			default:
				// обычная ошибка пользовательского кода
				return stdout, stderr, nil
			}
		}

		// docker не запустился вообще
		return "", "", fmt.Errorf("docker hadnt been started") // потом поменять на internal error
	}

	// Успешное выполнение
	return stdout, stderr, nil
}

func createRequest(ctx context.Context, dir string, filename string) (cmd *exec.Cmd) {

	args := []string{
		"run",
		"--rm",
		"--network", "none",
		"--memory", "128m",
		"--cpus", "0.5",
		"--pids-limit", "64",
		"--read-only",
		"--tmpfs", "/tmp:rw,size=16m",
		"-v", dir + ":/code",
		"gorunner",
		"go", "run", "/code/" + filename,
	}

	return exec.CommandContext(ctx, "docker", args...)
}

func recoverPending(client *redis.Client, ctx context.Context, stream string,
	group string, pendingIdle time.Duration) ([]PendingTask, error) {

	tasks := make([]PendingTask, 0, 10)

	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()
	if err != nil {
		return []PendingTask{}, err
	}

	for _, value := range pending {

		if value.Idle < pendingIdle {
			continue
		}

		tasks = append(tasks, PendingTask{
			value.ID,
			value.RetryCount,
		})

	}

	return tasks, nil

}

func claimAndExecute(worker *Worker, ctx context.Context, recoverIdArr []string) error {

	msgs, err := worker.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   worker.stream,
		Group:    worker.group,
		Consumer: worker.consumerID,
		MinIdle:  worker.pendingIdle,
		Messages: recoverIdArr,
	}).Result()
	if err != nil {
		return err
	}

	for _, msg := range msgs {

		task, err := ParseTask(msg)
		if err != nil {
			return err
		}

		result := ExecuteTask(worker.client, task)

		if result.Status == "done" {
			worker.processed++
		} else {
			worker.failed++
		}

		err = WriteResult(worker.client, ctx, result, task.Id)
		if err != nil {
			return err
		}

		err = AckTask(worker.client, ctx, worker.stream, worker.group, msg.ID)
		if err != nil {
			return err
		}
	}

	return nil

}
