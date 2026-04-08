// internal/runner.go
package internal

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Runner struct {
	Pool *Pool
}

// RunDocker сохраняет старый интерфейс для обратной совместимости
// Теперь использует HTTP API вместо docker exec
func (r *Runner) RunDocker(
	client *redis.Client,
	parentCtx context.Context,
	code string,
	id string,
	lang string,
) (string, string) {

	// Создаем контекст с таймаутом
	ctx, cancel := context.WithTimeout(parentCtx, 15*time.Second)
	defer cancel()

	// Используем новый метод Pool с HTTP API и чтением логов
	status, err := r.Pool.RunCodeWithLogs(client, ctx, code, id, lang)
	if err != nil {
		return "error", err.Error()
	}

	return status, ""
}
