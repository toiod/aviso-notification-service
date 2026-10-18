package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/toiod/aviso-notification-service/internal/notification"
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

func (r *NotificationRepository) GetByID(ctx context.Context, id int64) (notification.Notification, error) {
	var n notification.Notification
	row := r.pool.QueryRow(ctx, `select id, recipient, channel, message, status, attempts, created_at,
										sent_at, last_error, next_attempt_at  
										from notifications where id = $1`, id)
	err := row.Scan(&n.ID, &n.Recipient, &n.Channel, &n.Message, &n.Status, &n.Attempts, &n.CreatedAt, &n.SentAt, &n.LastError, &n.NextAttemptAt)
	if err != nil {
		return notification.Notification{}, fmt.Errorf("get notification by id: %w", err)
	}
	return n, nil
}
