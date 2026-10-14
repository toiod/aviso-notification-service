package notification

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeRepository struct {
	called bool
}

func (f *fakeRepository) Create(ctx context.Context, recipient, channel, message string) (int64, error) {
	f.called = true
	return 42, nil
}

func TestService_Create(t *testing.T) {
	tests := []struct {
		name              string
		recipient         string
		channel           string
		message           string
		wantID            int64
		wantValidationErr bool
		wantRepoCalled    bool
	}{
		{
			name:              "correct",
			recipient:         "123",
			channel:           "telegram",
			message:           "hello",
			wantID:            42,
			wantValidationErr: false,
			wantRepoCalled:    true,
		},
		{
			name:              "negative id",
			recipient:         "-123",
			channel:           "telegram",
			message:           "hello",
			wantID:            42,
			wantValidationErr: false,
			wantRepoCalled:    true,
		},
		{
			name:              "different channel",
			recipient:         "123",
			channel:           "email",
			message:           "hello",
			wantID:            0,
			wantValidationErr: true,
			wantRepoCalled:    false,
		},
		{
			name:              "wrong format recipient",
			recipient:         "abcd",
			channel:           "telegram",
			message:           "hello",
			wantID:            0,
			wantValidationErr: true,
			wantRepoCalled:    false,
		},
		{
			name:              "zero recipient",
			recipient:         "0",
			channel:           "telegram",
			message:           "hello",
			wantID:            0,
			wantValidationErr: true,
			wantRepoCalled:    false,
		},
		{
			name:              "empty message",
			recipient:         "123",
			channel:           "telegram",
			message:           "",
			wantID:            0,
			wantValidationErr: true,
			wantRepoCalled:    false,
		},
		{
			name:              "only spaces message",
			recipient:         "123",
			channel:           "telegram",
			message:           " \n\t",
			wantID:            0,
			wantValidationErr: true,
			wantRepoCalled:    false,
		},
		{
			name:              "4096 symbols message",
			recipient:         "123",
			channel:           "telegram",
			message:           strings.Repeat("й", 4096),
			wantID:            42,
			wantValidationErr: false,
			wantRepoCalled:    true,
		},
		{
			name:              "4097 symbols message",
			recipient:         "123",
			channel:           "telegram",
			message:           strings.Repeat("й", 4097),
			wantID:            0,
			wantValidationErr: true,
			wantRepoCalled:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeRepository{}
			service := NewService(repo)

			id, err := service.Create(t.Context(), tt.recipient, tt.channel, tt.message)

			if !tt.wantValidationErr && err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}

			gotValidationErr := errors.Is(err, ErrValidation)
			if gotValidationErr != tt.wantValidationErr {
				t.Errorf("validation error: expected %t, got %t", tt.wantValidationErr, gotValidationErr)
			}

			if id != tt.wantID {
				t.Errorf("id: expected %d, got %d", tt.wantID, id)
			}

			if repo.called != tt.wantRepoCalled {
				t.Errorf("repository called: expected %t, got %t", tt.wantRepoCalled, repo.called)
			}

		})
	}
}
