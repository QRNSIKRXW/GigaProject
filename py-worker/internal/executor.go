package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

var pyRunnerHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
}

var (
	pyExecutorTasksTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "py_executor_tasks_total",
		Help: "Total number of python tasks processed",
	})

	pyExecutorTasksFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "py_executor_tasks_failed_total",
		Help: "Total number of failed python tasks",
	})

	pyExecutorTasksSuccess = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "py_executor_tasks_success_total",
		Help: "Total number of successful python tasks",
	})

	pyExecutorTaskDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "py_executor_task_duration_seconds",
		Help:    "Python task execution duration",
		Buckets: prometheus.DefBuckets,
	})

	pyExecutorRunnerErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "py_executor_runner_errors_total",
		Help: "Runner communication errors in python worker",
	})
)

func init() {
	prometheus.MustRegister(
		pyExecutorTasksTotal,
		pyExecutorTasksFailed,
		pyExecutorTasksSuccess,
		pyExecutorTaskDuration,
		pyExecutorRunnerErrors,
	)
}

func ExecuteTask(client *redis.Client, task Task) (result TaskStatus) {
	start := time.Now()
	pyExecutorTasksTotal.Inc()
	defer func() {
		pyExecutorTaskDuration.Observe(time.Since(start).Seconds())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	outCh := "task:" + task.Id + ":out"
	errCh := "task:" + task.Id + ":err"
	sub := client.Subscribe(ctx, outCh, errCh)
	_, subErr := sub.Receive(ctx)
	if subErr != nil {
		pyExecutorRunnerErrors.Inc()
		pyExecutorTasksFailed.Inc()
		return TaskStatus{
			Status: "error",
			Error:  "pubsub subscribe failed",
		}
	}
	ch := sub.Channel()

	buffer := NewLimitedBuffer(1_000_000)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var line RedisLine
				if err := json.Unmarshal([]byte(msg.Payload), &line); err != nil {
					continue
				}
				buffer.Write([]byte(line.Line + "\n"))
			case <-ctx.Done():
				return
			}
		}
	}()

	req := RunRequest{
		TaskId: task.Id,
		Lang:   task.Lang,
		Code:   task.Code,
	}

	body, _ := json.Marshal(req)

	reqHTTP, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://runner-service:9000/", bytes.NewBuffer(body))
	if err != nil {
		pyExecutorRunnerErrors.Inc()
		pyExecutorTasksFailed.Inc()
		result.Status = "error"
		result.Error = "internal error"
		result.Result = ""
		return result
	}
	reqHTTP.Header.Set("Content-Type", "application/json")

	resp, err := pyRunnerHTTPClient.Do(reqHTTP)
	if err != nil {
		pyExecutorRunnerErrors.Inc()
		pyExecutorTasksFailed.Inc()
		result.Status = "error"
		result.Error = "internal error"
		result.Result = ""
		return result
	}
	defer resp.Body.Close()

	var runRes RunResponse
	if err := json.NewDecoder(resp.Body).Decode(&runRes); err != nil {
		pyExecutorTasksFailed.Inc()
		return TaskStatus{
			Status: "error",
			Error:  "invalid runner response",
		}
	}

	sub.Close()
	cancel()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		log.Println("timeout waiting pubsub reader")
	}

	output := strings.TrimRight(buffer.String(), "\n")

	if runRes.Status == "error" {
		pyExecutorTasksFailed.Inc()

		if output != "" {
			return TaskStatus{
				Status: "failed",
				Result: output,
				Error:  extractErrorLine(output),
			}
		}

		return TaskStatus{
			Status: "error",
			Result: "",
			Error:  runRes.Error,
		}
	}

	pyExecutorTasksSuccess.Inc()
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
			atomic.AddInt64(&worker.processed, 1)
		} else {
			atomic.AddInt64(&worker.failed, 1)
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
