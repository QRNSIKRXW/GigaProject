package internal

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/redis/go-redis/v9"
)

func WorkersHandler(client *redis.Client) http.HandlerFunc {

	resultFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodGet {
			log.Println("error: wrong method")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "method is not allowed",
			})
			return
		}

		workersArr, err := GetWorkersArr(client, r.Context())
		if err != nil {
			log.Println("error:", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "internal error",
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")

		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(workersArr)
		if err != nil {
			log.Println("error:", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "internal error",
			})
			return
		}

	})

	return resultFunc

}

func WorkerByIdHandler(client *redis.Client) http.HandlerFunc {

	resultFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodGet {
			log.Println("error: wrong method")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "method is not allowed",
			})
			return
		}

		vars := mux.Vars(r)
		id := vars["id"]

		worker, err := GetWorkerById(client, r.Context(), id)
		if err != nil {
			log.Println("error:", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "not found",
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")

		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(worker)
		if err != nil {
			log.Println("error:", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "internal error",
			})
			return
		}

	})

	return resultFunc

}

func WorkersByStatusHandler(client *redis.Client) http.HandlerFunc {

	resultFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodGet {
			log.Println("error: wrong method")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "method is not allowed",
			})
			return
		}

		workersArr, err := GetWorkersArr(client, r.Context())
		if err != nil {
			log.Println("error:", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "internal error",
			})
			return
		}

		status := r.URL.Query().Get("status")
		status = strings.ToLower(status)

		if status == "" {

			w.Header().Set("Content-Type", "application/json")

			w.WriteHeader(http.StatusOK)
			err = json.NewEncoder(w).Encode(workersArr)
			if err != nil {
				log.Println("error:", err)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(ErrorResponse{
					Error: "internal error",
				})
				return
			}

		}

		workersByStatus := make([]WorkerInfo, 0, 15)

		for _, worker := range workersArr {

			if worker.Status == status {
				workersByStatus = append(workersByStatus, worker)
			}
		}

		w.Header().Set("Content-Type", "application/json")

		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(workersByStatus)
		if err != nil {
			log.Println("error:", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "internal error",
			})
			return
		}

	})

	return resultFunc

}

func WorkersStatsHandler(client *redis.Client) http.HandlerFunc {

	resultFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodGet {
			log.Println("error: wrong method")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "method is not allowed",
			})
			return
		}

		workersArr, err := GetWorkersArr(client, r.Context())
		if err != nil {
			log.Println("error:", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "internal error",
			})
			return
		}

		stat := WorkersStat{}

		for _, worker := range workersArr {
			stat.ProcessedCount += worker.Processed
			stat.FailedCount += worker.Failed
			stat.DeadCount += worker.Dead
			if worker.Status == "online" {
				stat.OnlineWorkersCount++
			} else if worker.Status == "offline" {
				stat.OfflineWorkersCount++
			}
		}

		w.Header().Set("Content-Type", "application/json")

		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(stat)
		if err != nil {
			log.Println("error:", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "internal error",
			})
			return
		}

	})

	return resultFunc

}

// Потом доделаю

// func QueueStatHandler(client *redis.Client) http.HandlerFunc {

// 	resultFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

// 		if r.Method != http.MethodGet {
// 			log.Println("error: wrong method")
// 			w.Header().Set("Content-Type", "application/json")
// 			w.WriteHeader(http.StatusMethodNotAllowed)
// 			json.NewEncoder(w).Encode(ErrorResponse{
// 				Error: "method is not allowed",
// 			})
// 			return
// 		}

// 		w.Header().Set("Content-Type", "application/json")

// 		w.WriteHeader(http.StatusOK)
// 		err = json.NewEncoder(w).Encode(workersArr)
// 		if err != nil {
// 			log.Println("error:", err)
// 			w.Header().Set("Content-Type", "application/json")
// 			w.WriteHeader(http.StatusInternalServerError)
// 			json.NewEncoder(w).Encode(ErrorResponse{
// 				Error: "internal error",
// 			})
// 			return
// 		}

// 	})

// 	return resultFunc

// }
