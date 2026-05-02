// internal/handler.go
package internal

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

func RunHandler(client *redis.Client, runner DockerRunner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lang := "unknown"
		outcome := "unknown"
		defer func() {
			runnerRequestsTotal.WithLabelValues(lang, outcome).Inc()
			runnerRequestDuration.WithLabelValues(lang, outcome).Observe(time.Since(start).Seconds())
		}()

		if r.Method != http.MethodPost {
			outcome = "method_not_allowed"
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "method not allowed"})
			return
		}

		var req RunRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			outcome = "invalid_json"
			runnerValidationErrorsTotal.WithLabelValues("invalid_json").Inc()
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "invalid JSON"})
			return
		}
		lang = req.Lang

		// Валидация
		if req.Code == "" {
			outcome = "validation_error"
			runnerValidationErrorsTotal.WithLabelValues("empty_code").Inc()
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "code is empty"})
			return
		}

		if req.Lang == "" {
			outcome = "validation_error"
			runnerValidationErrorsTotal.WithLabelValues("empty_lang").Inc()
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "lang is empty"})
			return
		}

		if req.TaskId == "" {
			outcome = "validation_error"
			runnerValidationErrorsTotal.WithLabelValues("empty_task_id").Inc()
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "taskId is empty"})
			return
		}

		// Создаем контекст с таймаутом
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		// Выполняем код
		status, errMsg := runner.RunDocker(client, ctx, req.Code, req.TaskId, req.Lang)
		log.Printf("executing code with id: %s", req.TaskId)

		resp := RunResponse{
			Status: status,
		}

		if errMsg != "" {
			resp.Error = errMsg
			outcome = "error"
		} else {
			outcome = status
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}
}
