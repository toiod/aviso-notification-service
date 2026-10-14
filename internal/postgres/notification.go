package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type NotificationRepository struct {
	pool *pgxpool.Pool
}

func NewNotificationRepository(pool *pgxpool.Pool) *NotificationRepository {
	return &NotificationRepository{
		pool: pool,
	}
}

func (r *NotificationRepository) Create(ctx context.Context, recipient, channel, message string) (int64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	queryNotification := `insert into notifications (recipient, channel, message)
											 values ($1, $2, $3) returning id`
	var id int64
	err = tx.QueryRow(ctx, queryNotification, recipient, channel, message).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert notification: %w", err)
	}
	queryOutbox := `insert into outbox (notification_id)
								values ($1)`
	_, err = tx.Exec(ctx, queryOutbox, id)
	if err != nil {
		return 0, fmt.Errorf("exec outbox query: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return 0, fmt.Errorf("commit transaction: %w", err)
	}

	return id, nil
}
