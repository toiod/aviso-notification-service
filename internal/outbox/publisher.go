package outbox

import (
	"context"
	"fmt"
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
