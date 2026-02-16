package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/QRNSIKRXW/GigaProject/api-service/internal"
	"github.com/QRNSIKRXW/GigaProject/api-service/internal/ws"
	"github.com/gorilla/mux"
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
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())

	hub := ws.CreateHub()

	go hub.StartHub()

	go ws.StartPubSubListener(ctx, client, hub)

	r := mux.NewRouter()

	// r.HandleFunc("/", Home) - хендлер для главной страницы выбора песочницы

	r.HandleFunc("/ws", ws.ConnectionHandler(hub, client))

	r.HandleFunc("/api/history", internal.HistoryHandler(client)).Methods("GET")
	r.HandleFunc("/api/run/go", internal.GoRunHandler(client)).Methods("POST")
	r.HandleFunc("/api/run/python", internal.PyRunHandler(client)).Methods("POST")
	r.HandleFunc("/api/result/{id}", internal.ReturnHandler(client)).Methods("GET")
	r.HandleFunc("/internal/health", internal.HealthHandler)

	srv := &http.Server{
		Handler: r,
		Addr:    ":8080",
	}

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
	cancel()
	srv.Shutdown(ctx)
	hub.Shutdown()

}
