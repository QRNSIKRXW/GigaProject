package internal

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func GoRunHandler(client *redis.Client) http.HandlerFunc {

	resultFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		ip := r.RemoteAddr

		if err := RateLimit(client, r.Context(), ip, 20); err != nil {

			log.Println(err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: err.Error(),
			})

			return

		}

		if r.Method != http.MethodPost {

			log.Println("wrong method")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "method is not allowed",
			})

			return

		}

		var req Task

		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {

			log.Println("Decode error", err.Error())
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "invalid json",
			})

			return

		}

		var ownerId string
		coockie, err := r.Cookie("owner")
		if err != nil {
			ownerId = uuid.New().String()

			http.SetCookie(w, &http.Cookie{
				Name:     "owner",
				Value:    ownerId,
				Path:     "/",
				HttpOnly: true,
				SameSite: http.SameSiteLaxMode,
				MaxAge:   3600 * 24 * 12,
			})
		} else {
			ownerId = coockie.Value
		}

		taskId := uuid.New().String()

		req.Id = taskId
		req.OwnerId = ownerId
		req.Lang = "golang"

		res := RunResponse{
			Id: taskId,
		}

		ctx := r.Context()

		err = PushTask(req, client, ctx, "tasks:go")
		if err != nil {
			log.Println("PusTask error", err.Error())
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "pushtask failed",
			})

			return
		}

		w.Header().Set("Content-Type", "application/json")

		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(res)
		if err != nil {
			log.Println("error", err)
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

func PyRunHandler(client *redis.Client) http.HandlerFunc {

	resultFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodPost {

			log.Println("wrong method")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "method is not allowed",
			})

			return

		}

		var req Task

		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {

			log.Println("Decode error", err.Error())
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "invalid json",
			})

			return

		}

		var ownerId string
		coockie, err := r.Cookie("owner")
		if err != nil {
			ownerId = uuid.New().String()

			http.SetCookie(w, &http.Cookie{
				Name:     "owner",
				Value:    ownerId,
				Path:     "/",
				HttpOnly: true,
				SameSite: http.SameSiteLaxMode,
				MaxAge:   3600 * 24 * 12,
			})
		} else {
			ownerId = coockie.Value
		}

		taskId := uuid.New().String()

		req.Id = taskId
		req.OwnerId = ownerId
		req.Lang = "python"

		res := RunResponse{
			Id: taskId,
		}

		ctx := r.Context()

		err = PushTask(req, client, ctx, "tasks:python")
		if err != nil {
			log.Println("PusTask error", err.Error())
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "pushtask failed",
			})

			return
		}

		w.Header().Set("Content-Type", "application/json")

		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(res)
		if err != nil {
			log.Println("error", err)
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

func ReturnHandler(client *redis.Client) http.HandlerFunc {

	resultFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodGet {

			log.Println("wrong method")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "method is not allowed",
			})

			return

		}

		vars := mux.Vars(r)
		id := vars["id"]
		if id == "" {

			log.Println("error", "emtpy id in GET request")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "empty id query",
			})

			return

		}

		cookie, err := r.Cookie("owner")
		if err != nil {

			log.Println("error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "missing owner cookie",
			})

			return
		}

		requestOwner := cookie.Value
		savedOwner, err := client.HGet(r.Context(), "task:"+id, "owner").Result()
		if err != nil {

			log.Println("error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "internal error",
			})

			return
		}

		if requestOwner != savedOwner {

			log.Println("error", "accsses denied")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "accsses denied",
			})

			return
		}

		ctx := r.Context()
		status, err := GetStatus(ctx, client, id)
		if err != nil {

			log.Println("error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "getstatus failed",
			})

			return

		}

		if status == Wrong_res {

			log.Println("error", "empty taskstatus")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "empty taskstatus",
			})

			return

		}

		w.Header().Set("Content-Type", "application/json")

		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(status)
		if err != nil {

			log.Println("error", err)
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

func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func HistoryHandler(client *redis.Client) http.HandlerFunc {

	resultFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if r.Method != http.MethodGet {

			log.Println("wrong method")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "method is not allowed",
			})

			return

		}

		cookie, err := r.Cookie("owner")
		if err != nil {

			log.Println("error:", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "missing owner",
			})

			return

		}

		ownerId := cookie.Value
		ctx := r.Context()

		limit := 20
		cursor := r.URL.Query().Get("cursor")
		var start int64

		if cursor == "" {
			start = 0
		} else {
			pos, err := client.LPos(ctx, "user:"+ownerId+":tasks", cursor, redis.LPosArgs{
				Rank:   1,
				MaxLen: 0,
			}).Result()
			if err != nil {

				log.Println("error:", err)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadGateway)
				json.NewEncoder(w).Encode(ErrorResponse{
					Error: "lpos failed",
				})

				return

			}

			start = pos + 1

		}

		stop := start + int64(limit)

		tasksIdArr, err := client.LRange(ctx, "user:"+ownerId, start, stop).Result()
		if err != nil {

			log.Println("error:", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: "cant find history",
			})

			return

		}

		tasks := make([]map[string]string, 0, len(tasksIdArr))

		for _, val := range tasksIdArr {

			data, err := client.HGetAll(ctx, val).Result()
			if err != nil || len(data) == 0 {
				continue
			}

			tasks = append(tasks, map[string]string{
				"id":     val,
				"code":   data["code"],
				"status": data["status"],
				"result": data["result"],
				"error":  data["error"],
				"lang":   data["lang"],
			})

		}

		var nextCursor *string

		if len(tasks) == limit {
			last := tasksIdArr[len(tasksIdArr)-1]
			nextCursor = &last
		} else {
			nextCursor = nil
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(map[string]interface{}{
			"tasks":  tasks,
			"cursor": nextCursor,
		})
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
