package internal

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/redis/go-redis/v9"
)

func RunHandler(client *redis.Client, runner DockerRunner) http.HandlerFunc {
	resultFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		ctx := r.Context()

		if r.Method != http.MethodPost {

			log.Println("wrong metthod")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "wrong method",
			})

			return

		}

		var req RunRequest
		var res RunResponse

		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {

			log.Println("error:", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "invalid json",
			})

			return

		}

		if req.Dir == "" || req.TaskId == "" || req.Lang == "" || req.Filename == "" {

			log.Println("error:", "empty request")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "empty request",
			})

			return

		}

		status, codeErr := runner.RunDocker(client, ctx, req.Dir, req.Filename, req.TaskId, req.Lang)

		res.Status = status
		res.Error = codeErr

		w.Header().Set("content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(res)
		if err != nil {

			log.Println("error:", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "invalid json",
			})

			return

		}

	})

	return resultFunc

}
