package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/QRNSIKRXW/GigaProject/api-worker/internal"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	client := internal.StartRedis()
	defer client.Close()

	r := mux.NewRouter()

	r.Handle("/metrics", promhttp.Handler())

	r.HandleFunc("/internals/workers", internal.WorkersHandler(client)).Methods("GET")
	r.HandleFunc("/internals/workers/{id}", internal.WorkerByIdHandler(client))
	r.HandleFunc("/internals/workers/status", internal.WorkersByStatusHandler(client)).Methods("GET")
	r.HandleFunc("/internals/workers/stats", internal.WorkersStatsHandler(client)).Methods("GET")

	srv := &http.Server{
		Handler: r,
		Addr:    ":8080",
	}

	log.Println("server started on :8080")
	go srv.ListenAndServe()

	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool)

	go func() {
		<-sigChan
		done <- true
	}()

	<-done
	log.Printf("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv.Shutdown(ctx)

}
