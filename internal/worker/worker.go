package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/toiod/aviso-notification-service/internal/notification"
	"github.com/toiod/aviso-notification-service/internal/sender"
)

type Repository interface {
	GetByID(ctx context.Context, id int64) (notification.Notification, error)
	StartAttempt(ctx context.Context, id int64) (int, error)
	MarkSent(ctx context.Context, id int64, attemptNo int) error
	MarkFailed(ctx context.Context, id int64, attemptNo int, errorMessage string) error
	MarkTemporaryFailure(ctx context.Context, id int64, attemptNo int, errorMessage string, nextAttempt time.Time) error
}

type Queue interface {
	Ack(ctx context.Context, streamID string) error
	Read(ctx context.Context, consumer string) ([]redis.XStream, error)
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
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
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
		if nf.NextAttemptAt != nil {
			remaining := time.Until(*nf.NextAttemptAt)
			if remaining > 0 {
				timer := time.NewTimer(remaining)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
					continue
				}
			}
		}

		attemptNo, err := w.repo.StartAttempt(ctx, notificationID)
		if err != nil {
			return fmt.Errorf("start attempt: %w", err)
		}
		err = w.sender.Send(ctx, nf.Recipient, nf.Message)
		if err != nil {
			kind, retryAfter := classifySendError(err)
			if kind == failurePermanent {
				saveErr := w.repo.MarkFailed(ctx, notificationID, attemptNo, err.Error())
				if saveErr != nil {
					return fmt.Errorf("mark notification failed: %w", saveErr)
				}
				err = w.queue.Ack(ctx, streamID)
				if err != nil {
					return fmt.Errorf("ack error: %w", err)
				}
				return nil
			}
			if kind == failureTemporary {
				var delay time.Duration
				switch attemptNo {
				case 1:
					delay = time.Second
				case 2:
					delay = 3 * time.Second
				case 3:
					delay = 9 * time.Second
				case 4:
					delay = 27 * time.Second
				}
				if retryAfter > delay {
					delay = retryAfter
				}
				nextAttempt := time.Now().Add(delay)
				saveErr := w.repo.MarkTemporaryFailure(ctx, notificationID, attemptNo, err.Error(), nextAttempt)
				if saveErr != nil {
					return fmt.Errorf("mark temporary failed: %w", saveErr)
				}
				continue
			}
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
}

func (w *Worker) Run(ctx context.Context, consumer string) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		streams, err := w.queue.Read(ctx, consumer)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("read error", "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
				continue
			}
		}
		for _, stream := range streams {
			for _, message := range stream.Messages {
				value, ok := message.Values["notification_id"].(string)
				if !ok {
					slog.Error("invalid notification_id field", "streamID", message.ID)
					err = w.queue.Ack(ctx, message.ID)
					if err != nil {
						slog.Error("ack redis stream message", "error", err, "streamID", message.ID)
					}
					continue
				}
				notificationID, err := strconv.ParseInt(value, 10, 64)
				if err != nil || notificationID <= 0 {
					slog.Error("invalid notification_id field", "streamID", message.ID, "notificationID", value)
					err = w.queue.Ack(ctx, message.ID)
					if err != nil {
						slog.Error("ack redis stream message", "error", err, "streamID", message.ID)
					}
					continue
				}
				streamID := message.ID
				err = w.Process(ctx, streamID, notificationID)
				if err != nil {
					slog.Error("worker proccess", "error", err, "streamID", streamID, "notificationID", notificationID)
				}

			}
		}
	}
}
