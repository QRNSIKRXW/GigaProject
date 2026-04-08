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
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "method not allowed"})
			return
		}

		var req RunRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "invalid JSON"})
			return
		}

		// Валидация
		if req.Code == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "code is empty"})
			return
		}

		if req.Lang == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "lang is empty"})
			return
		}

		if req.TaskId == "" {
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
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}
}
