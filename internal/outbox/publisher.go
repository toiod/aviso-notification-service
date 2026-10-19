package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type Repository interface {
	ListUnpublished(ctx context.Context, limit int) ([]Event, error)
	MarkPublished(ctx context.Context, outboxID int64) error
}

type Queue interface {
	Publish(ctx context.Context, notificationID int64) (string, error)
}

type Publisher struct {
	repo  Repository
	queue Queue
}

func NewPublisher(repo Repository, queue Queue) *Publisher {
	return &Publisher{
		repo:  repo,
		queue: queue,
	}
}

func (p *Publisher) RunOnce(ctx context.Context) error {
	events, err := p.repo.ListUnpublished(ctx, 100)
	if err != nil {
		return fmt.Errorf("list unpublished: %w", err)
	}
	for _, event := range events {
		_, err := p.queue.Publish(ctx, event.NotificationID)
		if err != nil {
			return fmt.Errorf("publish event: %w", err)
		}
		err = p.repo.MarkPublished(ctx, event.ID)
		if err != nil {
			return fmt.Errorf("mark event published: %w", err)
		}

	}
	return nil
}

func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := p.RunOnce(timeoutCtx)
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Error("publisher pass failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			continue
		}
	}
}
