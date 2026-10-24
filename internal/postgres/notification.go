package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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
		if errors.Is(err, pgx.ErrNoRows) {
			return notification.Notification{}, notification.ErrNotFound
		}
		return notification.Notification{}, fmt.Errorf("get notification by id: %w", err)
	}
	return n, nil
}

func (r *NotificationRepository) StartAttempt(ctx context.Context, id int64) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	queryUpdate := `update notifications 
				set status = 'processing',
					attempts = attempts + 1,
					next_attempt_at = NULL
				where id = $1 and status = 'pending' and attempts < 5
				returning attempts`
	var attemptNo int
	err = tx.QueryRow(ctx, queryUpdate, id).Scan(&attemptNo)
	if err != nil {
		return 0, fmt.Errorf("get attempt no: %w", err)
	}
	queryInsert := `insert into notification_attempts
						(notification_id, attempt_no)
					values ($1, $2)`
	_, err = tx.Exec(ctx, queryInsert, id, attemptNo)
	if err != nil {
		return 0, fmt.Errorf("insert notification attempt: %w", err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		return 0, fmt.Errorf("commit transaction: %w", err)
	}
	return attemptNo, nil
}

func (r *NotificationRepository) MarkSent(ctx context.Context, id int64, attemptNo int) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	queryUpdate1 := `update notifications set status = 'sent',
										sent_at = now(),
										last_error = null,
										next_attempt_at = null
									where id = $1 and status = 'processing' and attempts = $2`
	tag, err := tx.Exec(ctx, queryUpdate1, id, attemptNo)
	if err != nil {
		return fmt.Errorf("mark notification sent: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("notification not in expected state")
	}
	queryUpdate2 := `update notification_attempts set outcome = 'success',
										finished_at = now(),
										error = null
									where notification_id = $1 and outcome = 'started' and attempt_no = $2`
	tag, err = tx.Exec(ctx, queryUpdate2, id, attemptNo)
	if err != nil {
		return fmt.Errorf("mark notification attempt succeed: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("notification attempt not in expected state")
	}
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (r *NotificationRepository) MarkFailed(ctx context.Context, id int64, attemptNo int, errorMessage string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	queryUpdate1 := `update notifications set status = 'failed',
											last_error = $3, 
											next_attempt_at = NULL, 
											sent_at = NULL
					where id = $1 and status = 'processing' and attempts = $2`
	tag, err := tx.Exec(ctx, queryUpdate1, id, attemptNo, errorMessage)
	if err != nil {
		return fmt.Errorf("mark notification failed: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("notification not in expected state")
	}
	queryUpdate2 := `update notification_attempts set outcome = 'permanent_error',
														finished_at = now(),
														error = $3
					where notification_id = $1 and outcome = 'started' and attempt_no = $2`
	tag, err = tx.Exec(ctx, queryUpdate2, id, attemptNo, errorMessage)
	if err != nil {
		return fmt.Errorf("mark notification fail permanently: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("notification not in expected state")
	}
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (r *NotificationRepository) MarkTemporaryFailure(ctx context.Context, id int64, attemptNo int, errorMessage string, nextAttempt time.Time) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	status := "failed"
	var retryAt *time.Time

	if attemptNo >= 1 && attemptNo <= 4 {
		status = "pending"
		retryAt = &nextAttempt
	}

	queryUpdate1 := `update notifications set status = $3,
											last_error = $4,
											next_attempt_at = $5, 
											sent_at = NULL
					where id = $1 and status = 'processing' and attempts = $2`
	tag, err := tx.Exec(ctx, queryUpdate1, id, attemptNo, status, errorMessage, retryAt)
	if err != nil {
		return fmt.Errorf("mark notification: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("notification not in expected state")
	}

	queryUpdate2 := `update notification_attempts set outcome = 'temporary_error',
													finished_at = now(),
													error = $3
					where notification_id = $1 and outcome = 'started' and attempt_no = $2`
	tag, err = tx.Exec(ctx, queryUpdate2, id, attemptNo, errorMessage)
	if err != nil {
		return fmt.Errorf("mark notifications attempts: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("notification attempts not in expected state")
	}
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
