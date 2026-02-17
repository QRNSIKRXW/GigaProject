package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/QRNSIKRXW/GigaProject/go-worker/internal"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	r := mux.NewRouter()

	client := internal.StartRedis()

	worker := internal.NewWorker(client, "tasks:python", "workers")

	wg := new(sync.WaitGroup)

	ctx, cancel := context.WithCancel(context.Background())

	r.Handle("/metrics", promhttp.Handler())

	err := worker.StartWorker(ctx, wg)
	if err != nil {
		log.Fatalf("failed to create consumer group: %v", err)
	}

	go func() {
		log.Println("go-worker metrics on :9001")
		if err := http.ListenAndServe(":9001", r); err != nil {
			log.Fatalf("metrics server failed: %v", err)
		}
	}()

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	log.Println("shutting down")
	cancel()

	wg.Wait()

	client.Close()
}
