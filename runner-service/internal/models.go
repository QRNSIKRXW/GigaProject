package internal

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

type RunRequest struct {
	TaskId string `json:"taskId"`
	Lang   string `json:"lang"`
	Code   string `json:"code"`
}

type RedisLine struct {
	TaskId string `json:"taskId"`
	Line   string `json:"line"`
}

type RunResponse struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type DockerRunner interface {
	RunDocker(*redis.Client, context.Context, string, string, string) (string, string)
}

type Runner struct{}

func (r *Runner) RunDocker(client *redis.Client, ctx context.Context, code string, id string, lang string) (string, string) {
	return RunDocker(client, ctx, code, id, lang)
}
