package internal

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

type RunRequest struct {
	TaskId   string `json:"taskId"`
	Dir      string `json:"dir"`
	Lang     string `json:"lang"`
	Filename string `json:"filename"`
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
	RunDocker(*redis.Client, context.Context, string, string, string, string) (string, string)
}

type Runner struct{}

func (r *Runner) RunDocker(client *redis.Client, ctx context.Context, dir string, filename string, id string, lang string) (string, string) {
	return RunDocker(client, ctx, dir, filename, id, lang)
}
