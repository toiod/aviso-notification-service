package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/toiod/aviso-notification-service/internal/config"
	"github.com/toiod/aviso-notification-service/internal/outbox"
	"github.com/toiod/aviso-notification-service/internal/postgres"
	"github.com/toiod/aviso-notification-service/internal/queue"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load error", "error", err)
		os.Exit(1)
	}

	dbURL := cfg.DatabaseURL
	redisAddr := cfg.RedisAddr

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		slog.Error("create postgres pool", "error", err)
		os.Exit(1)
	}
	if err := pool.Ping(ctx); err != nil {
		slog.Error("ping postgres", "error", err)
		pool.Close()
		os.Exit(1)
	}
	defer pool.Close()
	slog.Info("database connected")

	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer client.Close()
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = client.Ping(ctx).Err(); err != nil {
		slog.Error("ping redis", "error", err)
		os.Exit(1)
	}
	slog.Info("redis connected")
	outboxRepo := postgres.NewOutboxRepository(pool)
	redisQueue := queue.NewRedisQueue(client)
	publisher := outbox.NewPublisher(outboxRepo, redisQueue)

	workerCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err = publisher.Run(workerCtx)
	if err != nil {
		slog.Error("run publisher", "error", err)
		os.Exit(1)
	}
	slog.Info("worker stopped")
}
