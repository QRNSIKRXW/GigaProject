package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/QRNSIKRXW/GigaProject/api-service/internal"
	"github.com/QRNSIKRXW/GigaProject/api-service/internal/ws"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	client := internal.StartRedis()
	defer client.Close()

	hub := ws.CreateHub(client)
	go hub.StartHub()

	r := mux.NewRouter()

	r.Handle("/metrics", promhttp.Handler())

	// r.HandleFunc("/", Home) - хендлер для главной страницы выбора песочницы

	r.HandleFunc("/ws", ws.ConnectionHandler(hub))

	r.HandleFunc("/api/history", internal.HistoryHandler(client)).Methods("GET")
	r.HandleFunc("/api/run/go", internal.GoRunHandler(client)).Methods("POST")
	r.HandleFunc("/api/run/python", internal.PyRunHandler(client)).Methods("POST")
	r.HandleFunc("/api/result/{id}", internal.ReturnHandler(client)).Methods("GET")
	r.HandleFunc("/internal/health", internal.HealthHandler)

	srv := &http.Server{
		Handler: r,
		Addr:    ":8080",
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("api-service failed: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool)

	go func() {
		<-sigChan
		done <- true
	}()

	<-done
	log.Printf("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}
	hub.Shutdown()

}
