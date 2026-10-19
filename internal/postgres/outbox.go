package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/toiod/aviso-notification-service/internal/outbox"
)

type OutboxRepository struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{
		pool: pool,
	}
}

func (r *OutboxRepository) ListUnpublished(ctx context.Context, limit int) ([]outbox.Event, error) {
	query := `select id, notification_id from outbox
				where published_at is null order by id limit $1`
	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("query rows: %w", err)
	}
	defer rows.Close()
	events := make([]outbox.Event, 0, limit)
	for rows.Next() {
		var event outbox.Event
		err = rows.Scan(&event.ID, &event.NotificationID)
		if err != nil {
			return nil, fmt.Errorf("row scan: %w", err)
		}
		events = append(events, event)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return events, nil
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, outboxID int64) error {
	query := `update outbox set published_at = now()
				where id = $1 and published_at is null`
	_, err := r.pool.Exec(ctx, query, outboxID)
	if err != nil {
		return fmt.Errorf("update error: %w", err)
	}
	return nil
}
