package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

var httpClient = &http.Client{
	Timeout: 20 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,
	},
}

// 🔥 МЕТРИКИ
var (
	executorTasksTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "executor_tasks_total",
		Help: "Total number of tasks processed",
	})

	executorTasksFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "executor_tasks_failed_total",
		Help: "Total number of failed tasks",
	})

	executorTasksSuccess = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "executor_tasks_success_total",
		Help: "Total number of successful tasks",
	})

	executorTaskDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "executor_task_duration_seconds",
		Help:    "Task execution duration",
		Buckets: prometheus.DefBuckets,
	})

	executorRunnerErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "executor_runner_errors_total",
		Help: "Runner communication errors",
	})

	executorPubSubErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "executor_pubsub_errors_total",
		Help: "Redis PubSub errors",
	})
)

func init() {
	prometheus.MustRegister(
		executorTasksTotal,
		executorTasksFailed,
		executorTasksSuccess,
		executorTaskDuration,
		executorRunnerErrors,
		executorPubSubErrors,
	)
}

func ExecuteTask(client *redis.Client, task Task) (result TaskStatus) {

	start := time.Now()
	executorTasksTotal.Inc()

	defer func() {
		executorTaskDuration.Observe(time.Since(start).Seconds())
	}()

	if len(task.Code) > MaxCodeSize {
		executorTasksFailed.Inc()
		return TaskStatus{
			Status: "error",
			Error:  "code size limit exceeded",
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	outCh := "task:" + task.Id + ":out"
	errCh := "task:" + task.Id + ":err"
	sub := client.Subscribe(ctx, outCh, errCh)

	// 🔥 ВАЖНО: дождаться подтверждения подписки
	_, err := sub.Receive(ctx)
	if err != nil {
		executorPubSubErrors.Inc()
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

	httpReq, err := http.NewRequest("POST", "http://runner-service:9000/", bytes.NewBuffer(body))
	if err != nil {
		executorTasksFailed.Inc()
		return TaskStatus{
			Status: "error",
			Error:  "internal request error",
		}
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		executorRunnerErrors.Inc()
		executorTasksFailed.Inc()
		return TaskStatus{
			Status: "error",
			Error:  "runner unavailable",
		}
	}
	defer resp.Body.Close()

	log.Println("Post successful")

	var runRes RunResponse
	if err := json.NewDecoder(resp.Body).Decode(&runRes); err != nil {
		executorTasksFailed.Inc()
		return TaskStatus{
			Status: "error",
			Error:  "invalid runner response",
		}
	}

	// 🔥 корректное завершение
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
		executorTasksFailed.Inc()

		if output != "" {
			errLine := extractErrorLine(output)
			if errLine == "" {
				errLine = output
			}
			return TaskStatus{
				Status: "failed",
				Result: output,
				Error:  errLine,
			}
		}

		errMsg := runRes.Error
		if errMsg == "" {
			errMsg = "execution failed"
		}

		return TaskStatus{
			Status: "error",
			Error:  errMsg,
		}
	}

	executorTasksSuccess.Inc()

	return TaskStatus{
		Status: "done",
		Result: output,
	}
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
