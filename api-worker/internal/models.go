package internal

import (
	"fmt"

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

type WorkersStat struct {
	ProcessedCount      int64 `json:"processed"`
	FailedCount         int64 `json:"failed"`
	DeadCount           int64 `json:"dead"`
	OnlineWorkersCount  int64 `json:"online"`
	OfflineWorkersCount int64 `json:"offline"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type WorkerInfo struct {
	Id            string
	Status        string
	LastHeartbeat int64
	Processed     int64
	Failed        int64
	Dead          int64
}

type PendingTask struct {
	MsgId      string
	RetryCount int64
}

const MaxCodeSize = 64 * 1024
