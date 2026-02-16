package internal

import (
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Task struct {
	Id   string
	Code string
}

func ParseTask(msg redis.XMessage) (Task, error) {
	id, ok := msg.Values["id"].(string)
	if !ok {
		return Task{}, fmt.Errorf("types missmatch")
	}

	code, ok := msg.Values["code"].(string)
	if !ok {
		return Task{}, fmt.Errorf("types missmatch")
	}
	return Task{Id: id, Code: code}, nil
}

type TaskStatus struct {
	Status string `json:"status"`
	Result string `json:"result"`
	Error  string `json:"error"`
}

var WrongTask = Task{
	Id:   "none",
	Code: "_",
}

type RunResponse struct {
	Id string `json:"id"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type Worker struct {
	client       *redis.Client
	stream       string
	group        string
	consumerID   string
	pendingIdle  time.Duration // сколько задача должна висеть, чтобы считать её зависшей
	pendingBatch int           // сколько зависших задач за раз обрабатывать
	maxRetries   int64

	processed int64
	failed    int64
	dead      int64
}

type PendingTask struct {
	MsgId      string
	RetryCount int64
}

const MaxCodeSize = 64 * 1024
