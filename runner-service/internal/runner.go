package internal

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Runner struct {
	Pool *Pool
}

func (r *Runner) RunDocker(client *redis.Client, ctx context.Context, code, id, lang string) (string, string) {
	return r.Pool.RunDocker(client, ctx, code, id, lang)
}
