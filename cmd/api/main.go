package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/4mzy/aviso-notification-service/internal/config"
	"github.com/4mzy/aviso-notification-service/internal/httpapi"
	"github.com/4mzy/aviso-notification-service/internal/notification"
	"github.com/4mzy/aviso-notification-service/internal/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

type healthResponse struct {
	Status string `json:"status"`
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := healthResponse{
		Status: "ok",
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("encode health response", "error", err)
	}
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load error", "err", err)
		os.Exit(1)
	}
	addr := cfg.HTTPAddr
	dbURL := cfg.DatabaseURL

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		slog.Error("create postgres pool", "err", err)
		os.Exit(1)
	}
	if err := pool.Ping(ctx); err != nil {
		slog.Error("ping postgres", "err", err)
		pool.Close()
		os.Exit(1)
	}
	defer pool.Close()
	slog.Info("database connected")

	repo := postgres.NewNotificationRepository(pool)
	service := notification.NewService(repo)
	handler := httpapi.NewNotificationHandler(service)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /notifications", handler.Create)

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	slog.Info("http server started", "addr", addr)

	if err := server.ListenAndServe(); err != nil {
		slog.Error("server failed", "err", err)
		pool.Close()
		os.Exit(1)
	}

}
