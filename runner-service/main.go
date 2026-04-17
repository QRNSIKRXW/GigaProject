// main.go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/QRNSIKRXW/GigaProject/runner-service/internal"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func envInt(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}

	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be integer, got %q", name, raw)
	}
	if v < 0 {
		return 0, fmt.Errorf("%s must be >= 0, got %d", name, v)
	}

	return v, nil
}

func main() {
	// Инициализируем Redis
	client := internal.StartRedis()
	defer client.Close()

	goRunners, err := envInt("GO_RUNNER_COUNT", 3)
	if err != nil {
		log.Fatal(err)
	}

	pyRunners, err := envInt("PY_RUNNER_COUNT", 2)
	if err != nil {
		log.Fatal(err)
	}

	if goRunners+pyRunners == 0 {
		log.Fatal("at least one runner must be configured")
	}

	// Создаем пул воркеров (по env)
	pool, err := internal.NewPool(goRunners, pyRunners)
	if err != nil {
		log.Fatal("Failed to create pool:", err)
	}

	log.Printf("Pool created successfully: %d workers total",
		pool.GetWorkerCount("golang")+pool.GetWorkerCount("python"))

	// Создаем runner
	runner := &internal.Runner{Pool: pool}

	// Настраиваем роутер
	r := mux.NewRouter()
	r.Handle("/metrics", promhttp.Handler())
	r.HandleFunc("/", internal.RunHandler(client, runner))

	// Настраиваем сервер
	srv := &http.Server{
		Handler:      r,
		Addr:         ":9000",
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Запускаем сервер в горутине
	go func() {
		log.Printf("Server starting on :9000")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Ожидаем сигнал завершения
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("Shutting down server...")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Server stopped")
}
