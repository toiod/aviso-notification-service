package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/toiod/aviso-notification-service/internal/notification"
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

		_, err := pool.Exec(cleanupCtx, `delete from notification_attempts where notification_id = $1`, id)
		if err != nil {
			t.Errorf("cleanup notification attempt: %v", err)
		}

		_, err = pool.Exec(cleanupCtx, `delete from notifications where id = $1`, id)
		if err != nil {
			t.Errorf("cleanup notifications: %v", err)
		}

	}()

	if id <= 0 {
		t.Fatalf("expected positive id, got %d", id)
	}

	got, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("get notification by id: %v", err)
	}
	if got.ID != id {
		t.Errorf("id: expected %d, got %d", id, got.ID)
	}
	if got.Recipient != recipient {
		t.Errorf("recipient: expected %q, got %q", recipient, got.Recipient)
	}
	if got.Channel != channel {
		t.Errorf("channel: expected %q, got %q", channel, got.Channel)
	}
	if got.Message != message {
		t.Errorf("message: expected %q, got %q", message, got.Message)
	}
	if got.Status != "pending" {
		t.Errorf("status: expected %q, got %q", "pending", got.Status)
	}
	if got.Attempts != 0 {
		t.Errorf("attempts: expected %d, got %d", 0, got.Attempts)
	}
	if got.CreatedAt.IsZero() {
		t.Error("created_at: expected non-zero time")
	}
	if got.SentAt != nil {
		t.Error("sent_at: expected nil value")
	}
	if got.LastError != nil {
		t.Error("last_error: expected nil value")
	}
	if got.NextAttemptAt != nil {
		t.Error("next_attempt_at: expected nil value")
	}

	var count int64
	err = pool.QueryRow(ctx, `select count(*) from outbox where notification_id = $1`, id).Scan(&count)
	if err != nil {
		t.Fatalf("count outbox rows: %v", err)
	}
	if count != 1 {
		t.Errorf("count of notification_id in outbox: expected %d, got %d", 1, count)
	}

	attemptNo, err := repo.StartAttempt(ctx, id)
	if err != nil {
		t.Fatalf("start send attempt: %v", err)
	}
	if attemptNo != 1 {
		t.Errorf("attempt number: expected %d, got %d", 1, attemptNo)
	}

	nf, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	if nf.Status != "processing" {
		t.Errorf("status: expected %s, got %s", "processing", nf.Status)
	}
	if nf.Attempts != 1 {
		t.Errorf("notificatio attempts: expected %d, got %d", 1, nf.Attempts)
	}
	var attemptNoCheck int
	var outcome string
	var finished_at *time.Time
	err = pool.QueryRow(ctx, `select attempt_no, outcome, finished_at from notification_attempts where notification_id = $1`, id).Scan(&attemptNoCheck, &outcome, &finished_at)

	if err != nil {
		t.Fatalf("scan values for check: %v", err)
	}
	if attemptNoCheck != 1 {
		t.Errorf("attempt number: expected %d, got %d", 1, attemptNoCheck)
	}
	if outcome != "started" {
		t.Errorf("outcome: expected %s, got %s", "started", outcome)
	}
	if finished_at != nil {
		t.Errorf("finished_at: expected %v, got %v", nil, finished_at)
	}

	err = repo.MarkSent(ctx, id, attemptNo)
	if err != nil {
		t.Fatalf("mark notification sent: %v", err)
	}
	nf, err = repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("get notification after mark sent: %v", err)
	}
	if nf.Status != "sent" {
		t.Errorf("status: expected %s, got %s", "sent", nf.Status)
	}
	if nf.Attempts != 1 {
		t.Errorf("notificatio attempts: expected %d, got %d", 1, nf.Attempts)
	}
	if nf.SentAt == nil {
		t.Error("sent_at: expected non-nil value")
	}

	err = pool.QueryRow(ctx, `select outcome, finished_at from notification_attempts 
								where notification_id = $1 and attempt_no = $2`, id, attemptNo).Scan(
		&outcome, &finished_at,
	)
	if err != nil {
		t.Fatalf("scan values for check after mark: %v", err)
	}
	if outcome != "success" {
		t.Errorf("outcome: expected %s, got %s", "success", outcome)
	}
	if finished_at == nil {
		t.Errorf("finished_at: expected non-nil value")
	}

	_, err = repo.GetByID(ctx, -1)
	if !errors.Is(err, notification.ErrNotFound) {
		t.Fatalf("expected not found error, got: %v", err)
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

func TestNotificationRepository_MarkFailed(t *testing.T) {
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

		_, err := pool.Exec(cleanupCtx, `delete from notification_attempts where notification_id = $1`, id)
		if err != nil {
			t.Errorf("cleanup notification attempt: %v", err)
		}

		_, err = pool.Exec(cleanupCtx, `delete from notifications where id = $1`, id)
		if err != nil {
			t.Errorf("cleanup notifications: %v", err)
		}

	}()

	attemptNo, err := repo.StartAttempt(ctx, id)
	if err != nil {
		t.Fatalf("start send attempt: %v", err)
	}
	if attemptNo != 1 {
		t.Errorf("attempt number: expected %d, got %d", 1, attemptNo)
	}

	errorMessage := "bot was blocked"
	err = repo.MarkFailed(ctx, id, attemptNo, errorMessage)
	if err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	nf, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("get by id: %v", err)
	}

	if nf.Status != "failed" {
		t.Errorf("status: expected failed, got %s", nf.Status)
	}
	if nf.Attempts != 1 {
		t.Errorf("attempts: expected 1, got %d", nf.Attempts)
	}
	if nf.SentAt != nil {
		t.Errorf("sent_at: expected null, got %v", nf.SentAt)
	}
	if nf.LastError == nil {
		t.Fatalf("not expected last error")
	}
	if *nf.LastError != errorMessage {
		t.Errorf("*last_error: expected %s, got %s", errorMessage, *nf.LastError)
	}

	var outcome string
	var attemptError string
	var finishedAt *time.Time
	err = pool.QueryRow(ctx, `select outcome, error, finished_at from notification_attempts
						where notification_id = $1 and attempt_no = $2`,
		id, attemptNo).Scan(&outcome, &attemptError, &finishedAt)
	if err != nil {
		t.Fatalf("row scan: %v", err)
	}
	if outcome != "permanent_error" {
		t.Errorf("outcome: expected permanent_error, got %v", outcome)
	}
	if attemptError != errorMessage {
		t.Errorf("attempt_error: expected %s, got %s", errorMessage, attemptError)
	}
	if finishedAt == nil {
		t.Errorf("finished_at expected not null")
	}

}

func TestNotificationRepository_MarkTemporaryFailure(t *testing.T) {
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

		_, err := pool.Exec(cleanupCtx, `delete from notification_attempts where notification_id = $1`, id)
		if err != nil {
			t.Errorf("cleanup notification attempt: %v", err)
		}

		_, err = pool.Exec(cleanupCtx, `delete from notifications where id = $1`, id)
		if err != nil {
			t.Errorf("cleanup notifications: %v", err)
		}

	}()

	attemptNo, err := repo.StartAttempt(ctx, id)
	if err != nil {
		t.Fatalf("start send attempt: %v", err)
	}

	errorMessage := "telegram temporarily unavailable"
	nextAttempt := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)

	err = repo.MarkTemporaryFailure(ctx, id, attemptNo, errorMessage, nextAttempt)
	if err != nil {
		t.Fatalf("mark temporary failure: %v", err)
	}

	nf, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("get by id: %v", err)
	}

	if nf.Status != "pending" {
		t.Errorf("status: expected pending, got %s", nf.Status)
	}
	if nf.Attempts != 1 {
		t.Errorf("attempts: expected 1, got %d", nf.Attempts)
	}
	if nf.LastError == nil {
		t.Fatalf("last_error: expected not null")
	}
	if *nf.LastError != errorMessage {
		t.Errorf("*last_error: expected %s, got %s", errorMessage, *nf.LastError)
	}
	if nf.NextAttemptAt == nil {
		t.Fatalf("next_attempt_at: expected not null")
	}
	if !nf.NextAttemptAt.Equal(nextAttempt) {
		t.Errorf("next_attempt_at: expected equal nextAttempt")
	}
	var gotOutcome string
	var gotErr string
	var gotFinishedAt *time.Time
	err = pool.QueryRow(ctx, `select outcome, error, finished_at from notification_attempts
						where notification_id = $1 and attempt_no = $2`, id, attemptNo).Scan(
		&gotOutcome, &gotErr, &gotFinishedAt,
	)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	if gotOutcome != "temporary_error" {
		t.Errorf("got outcome: expected temporary_error, got %s", gotOutcome)
	}
	if gotErr != errorMessage {
		t.Errorf("got outcome: expected %s, got %s", errorMessage, gotErr)
	}
	if gotFinishedAt == nil {
		t.Errorf("got finished_at: expected not null")
	}
	_, err = repo.StartAttempt(ctx, id)
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected early attempt to be rejected, got: %v", err)
	}
	nf, err = repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf(": %v", err)
	}

	if nf.Status != "pending" {
		t.Errorf("status: expected pending, got %s", nf.Status)
	}

	if nf.Attempts != 1 {
		t.Errorf("attempts: expected 1, got %d", nf.Attempts)
	}
	tag, err := pool.Exec(ctx, `update notifications set next_attempt_at = now() - interval '1 second' where id = $1`, id)
	if err != nil {
		t.Fatalf("set retry time in past: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("notification not in expected state")
	}
	for i := 2; i <= 5; i++ {
		attemptNo, err = repo.StartAttempt(ctx, id)
		if err != nil {
			t.Fatalf("start send attempt: %v", err)
		}
		if attemptNo != i {
			t.Fatalf("expected attempt_no = cycle counter")
		}
		err = repo.MarkTemporaryFailure(ctx, id, attemptNo, errorMessage, time.Now().UTC().Add(-time.Second))
		if err != nil {
			t.Fatalf("mark temporary failure on attempt %d: %v", attemptNo, err)
		}
	}

	nf, err = repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf(": %v", err)
	}

	if nf.Status != "failed" {
		t.Errorf("status: expected failed, got %s", nf.Status)
	}

	if nf.Attempts != 5 {
		t.Errorf("attempts: expected 5, got %d", nf.Attempts)
	}

	if nf.NextAttemptAt != nil {
		t.Errorf("next_attempt_at: expected null")
	}
}
