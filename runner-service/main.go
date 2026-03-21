package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/QRNSIKRXW/GigaProject/runner-service/internal"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	client := internal.StartRedis()
	defer client.Close()

	// создаём пул: 3 Go, 2 Python (можно настраивать)
	pool, err := internal.NewPool(3, 2)
	if err != nil {
		log.Fatal("failed to create pool:", err)
	}
	runner := &internal.Runner{Pool: pool}

	r := mux.NewRouter()
	r.Handle("/metrics", promhttp.Handler())
	r.HandleFunc("/", internal.RunHandler(client, runner))

	srv := &http.Server{
		Handler: r,
		Addr:    ":9000",
	}

	go srv.ListenAndServe()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}
