package internal

import (
	"bufio"
	"context"
	"os/exec"

	"github.com/redis/go-redis/v9"
)

func RunDocker(client *redis.Client, ctx context.Context, dir string, filename string, id string, lang string) (status string, codeErr string) {

	cmd, err := CreateRequest(ctx, dir, filename, lang)
	if err != nil {
		return "error", err.Error()
	}

	stdoutPipe, _ := cmd.StdoutPipe()
	stderrPipe, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return "error", err.Error() // потом поменять на internal error
	}

	go func() {
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			middleResult := scanner.Text()
			WritePubSub(client, ctx, middleResult, id)
		}
	}()

	go func() {
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			middleResult := scanner.Text()
			WritePubSub(client, ctx, middleResult, id)
		}
	}()

	err = cmd.Wait()

	if ctx.Err() == context.DeadlineExceeded {
		return "error", "timeout"
	}

	// Ошибка выполнения контейнера
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {

			switch exitErr.ExitCode() {
			case 125, 126, 127:
				return "error", "docker crashed" // потом поменять internal error
			default:
				// обычная ошибка пользовательского кода
				return "done", ""
			}
		}

		// docker не запустился вообще
		return "error", "docker hadnt been started" // потом поменять на internal error
	}

	// Успешное выполнение
	return "done", ""
}
