package sender

import (
	"context"
	"log/slog"
)

type MockSender struct {
}

func (ms *MockSender) Send(ctx context.Context, recipient, message string) error {
	slog.Info("mock notification sent", "recipient", recipient)
	return nil
}
