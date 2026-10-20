package worker

import (
	"context"
	"fmt"

	"github.com/toiod/aviso-notification-service/internal/notification"
	"github.com/toiod/aviso-notification-service/internal/sender"
)

type Repository interface {
	GetByID(ctx context.Context, id int64) (notification.Notification, error)
	StartAttempt(ctx context.Context, id int64) (int, error)
	MarkSent(ctx context.Context, id int64, attemptNo int) error
}

type Queue interface {
	Ack(ctx context.Context, streamID string) error
}

type Worker struct {
	repo   Repository
	queue  Queue
	sender sender.Sender
}

func NewWorker(r Repository, q Queue, s sender.Sender) *Worker {
	return &Worker{
		repo:   r,
		queue:  q,
		sender: s,
	}
}

func (w *Worker) Process(ctx context.Context, streamID string, notificationID int64) error {
	nf, err := w.repo.GetByID(ctx, notificationID)
	if err != nil {
		return fmt.Errorf("read notification: %w", err)
	}
	if nf.Status == "sent" || nf.Status == "failed" {
		err = w.queue.Ack(ctx, streamID)
		if err != nil {
			return fmt.Errorf("ack queue: %w", err)
		}
		return nil
	}
	attemptNo, err := w.repo.StartAttempt(ctx, notificationID)
	if err != nil {
		return fmt.Errorf("start attempt: %w", err)
	}
	err = w.sender.Send(ctx, nf.Recipient, nf.Message)
	if err != nil {
		return fmt.Errorf("send notification: %w", err)
	}
	err = w.repo.MarkSent(ctx, notificationID, attemptNo)
	if err != nil {
		return fmt.Errorf("mark notification sent: %w", err)
	}
	err = w.queue.Ack(ctx, streamID)
	if err != nil {
		return fmt.Errorf("ack queue: %w", err)
	}
	return nil
}
