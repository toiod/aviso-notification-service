package worker

import (
	"errors"
	"testing"
	"time"

	"github.com/toiod/aviso-notification-service/internal/sender"
)

func TestClassifySendError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantKind  failureKind
		wantDelay time.Duration
	}{
		{
			name: "telegram 401",
			err: &sender.TelegramError{
				HTTPStatus: 401,
				Code:       401,
			},
			wantKind:  failureFatal,
			wantDelay: 0 * time.Second,
		},
		{
			name: "telegram 429",
			err: &sender.TelegramError{
				HTTPStatus: 429,
				Code:       429,
				RetryAfter: 12,
			},
			wantKind:  failureTemporary,
			wantDelay: 12 * time.Second,
		}, {
			name: "telegram 503",
			err: &sender.TelegramError{
				HTTPStatus: 503,
				Code:       503,
			},
			wantKind:  failureTemporary,
			wantDelay: 0 * time.Second,
		}, {
			name: "telegram 403",
			err: &sender.TelegramError{
				HTTPStatus: 403,
				Code:       403,
			},
			wantKind:  failurePermanent,
			wantDelay: 0 * time.Second,
		},
		{
			name:      "default error",
			err:       errors.New("connection failed"),
			wantKind:  failureTemporary,
			wantDelay: 0 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, delay := classifySendError(tt.err)
			if kind != tt.wantKind {
				t.Fatalf("error kind: want %s, got %s", tt.wantKind, kind)
			}
			if delay != tt.wantDelay {
				t.Fatalf("delay: want %v, got %v", tt.wantDelay, delay)
			}
		})
	}
}
