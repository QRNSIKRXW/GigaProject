package main

import (
	"context"
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

	runner := &internal.Runner{}

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
	done := make(chan bool)

	go func() {
		<-sig
		done <- true
	}()

	<-done

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv.Shutdown(ctx)

}
