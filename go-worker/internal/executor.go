package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func ExecuteTask(client *redis.Client, task Task) (result TaskStatus) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub := client.Subscribe(ctx, "channel:broadcast")
	ch := sub.Channel()

	buffer := NewLimitedBuffer(1_000_000)

	go func() {

		for msg := range ch {

			var line RedisLine
			json.Unmarshal([]byte(msg.Payload), &line)

			if line.TaskId != task.Id {
				continue
			}

			buffer.Write([]byte(line.Line + "\n"))

		}

	}()

	req := RunRequest{
		TaskId: task.Id,
		Lang:   task.Lang,
		Code:   task.Code,
	}

	body, _ := json.Marshal(req)

	resp, err := http.Post("http://host.docker.internal:9000/", "application/json", bytes.NewBuffer(body))
	if err != nil {
		result.Status = "error"
		result.Error = "internal error"
		result.Result = ""
		return result
	}

	sub.Close()
	cancel()

	var runRes RunResponse
	json.NewDecoder(resp.Body).Decode(&runRes)
	if runRes.Status == "error" {
		return TaskStatus{
			Status: "error",
			Result: "",
			Error:  runRes.Error,
		}
	}

	output := buffer.String()

	if containsUserError(output) {
		codeErr := extractErrorLine(output)
		return TaskStatus{
			Status: "failed",
			Result: output,
			Error:  codeErr,
		}
	}

	return TaskStatus{
		Status: "done",
		Result: output,
		Error:  "",
	}

}

func containsUserError(out string) bool {
	return strings.Contains(out, "panic") ||
		strings.Contains(out, "Traceback") ||
		strings.Contains(out, "error:")
}

func extractErrorLine(out string) string {
	lines := strings.Split(out, "\n")
	for _, l := range lines {
		if strings.Contains(l, "panic") ||
			strings.Contains(l, "Traceback") ||
			strings.Contains(l, "error:") {
			return l
		}
	}
	return ""
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
