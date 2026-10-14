package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNotificationRepository_Create(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	repo := NewNotificationRepository(pool)

	recipient := "123456789"
	channel := "telegram"
	message := "integration test"

	id, err := repo.Create(ctx, recipient, channel, message)
	if err != nil {
		t.Fatalf("create notification: %v", err)
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, err = pool.Exec(cleanupCtx, `delete from outbox where notification_id = $1`, id)
		if err != nil {
			t.Errorf("cleanup outbox: %v", err)
		}

		_, err = pool.Exec(cleanupCtx, `delete from notifications where id = $1`, id)
		if err != nil {
			t.Errorf("cleanup notifications: %v", err)
		}
	}()

	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	var gotRecipient, gotChannel, gotMessage, gotStatus string
	err = pool.QueryRow(ctx, `select recipient, channel, message, status from notifications where id = $1`, id).Scan(&gotRecipient, &gotChannel, &gotMessage, &gotStatus)

	if err != nil {
		t.Fatalf("read notificaton: %v", err)
	}

	if gotRecipient != recipient {
		t.Errorf("recipient: expected %q, got %q", recipient, gotRecipient)
	}
	if gotChannel != channel {
		t.Errorf("channel: expected %q, got %q", channel, gotChannel)
	}
	if gotMessage != message {
		t.Errorf("message: expected %q, got %q", message, gotMessage)
	}
	if gotStatus != "pending" {
		t.Errorf("status: expected %q, got %q", "pending", gotStatus)
	}
	var count int64
	err = pool.QueryRow(ctx, `select count(*) from outbox where notification_id = $1`, id).Scan(&count)
	if err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	if count != 1 {
		t.Errorf("count of notification_id in outbox: expected %d, got %d", 1, count)
	}

}

func TestNotificationRepository_CreateRollback(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	poolConfig, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	poolConfig.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()
	_, err = pool.Exec(ctx, `create temp table outbox (
						notification_id bigint check (notification_id < 0)
					)`)
	if err != nil {
		t.Fatalf("create temp outbox table: %v", err)
	}

	repo := NewNotificationRepository(pool)
	message := fmt.Sprintf("rollback test %d", time.Now().UnixNano())
	channel := "telegram"
	recipient := "123456789"

	id, err := repo.Create(ctx, recipient, channel, message)

	if err == nil {
		t.Fatalf("expected outbox insert error")
	}

	var pgErr *pgconn.PgError

	if !errors.As(err, &pgErr) {
		t.Fatalf("expected PostgreSQL error, got: %v", err)
	}

	if pgErr.Code != "23514" || pgErr.TableName != "outbox" {
		t.Fatalf("expected outbox CHECK validation, got: %v", pgErr)
	}

	if id != 0 {
		t.Errorf("id: expected %d, got %d", 0, id)
	}

	var count int64
	err = pool.QueryRow(ctx, `select count(*) from notifications where message = $1`, message).Scan(&count)
	if err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	if count != 0 {
		t.Errorf("count of notification_id in outbox: expected %d, got %d", 0, count)
	}
}
