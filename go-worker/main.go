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
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("metrics server started on :9100")
		if err := http.ListenAndServe(":9100", nil); err != nil {
			log.Fatalf("metrics server failed: %v", err)
		}
	}()

	client := internal.StartRedis()

	worker := internal.NewWorker(client, "tasks:go", "workers")

	wg := new(sync.WaitGroup)

	ctx, cancel := context.WithCancel(context.Background())

	err := worker.StartWorker(ctx, wg)
	if err != nil {
		log.Fatalf("failed to create consumer group: %v", err)
	}

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	log.Println("shutting down")
	cancel()

	wg.Wait()

	client.Close()
}
