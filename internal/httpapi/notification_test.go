package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/toiod/aviso-notification-service/internal/notification"
)

type fakeRepository struct {
	called bool
	err    error
}

func (f *fakeRepository) Create(ctx context.Context, recipient, channel, message string) (int64, error) {
	f.called = true
	if f.err != nil {
		return 0, f.err
	}
	return 42, nil
}

func TestHandler_Create(t *testing.T) {
	tests := []struct {
		name           string
		body           string
		repoErr        error
		wantStatus     int
		wantRepoCalled bool
	}{
		{
			name:           "correct request",
			body:           `{"recipient":"123", "channel":"telegram", "message":"hello"}`,
			repoErr:        nil,
			wantStatus:     202,
			wantRepoCalled: true,
		},
		{
			name:           "bad json",
			body:           `{"recipient":"123", "channel":"telegram", "message":"hello"`,
			repoErr:        nil,
			wantStatus:     400,
			wantRepoCalled: false,
		},
		{
			name:           "2 json",
			body:           `{"recipient":"123", "channel":"telegram", "message":"hello"} {"extra":"info"}`,
			repoErr:        nil,
			wantStatus:     400,
			wantRepoCalled: false,
		},
		{
			name:           "wrong channel",
			body:           `{"recipient":"123", "channel":"email", "message":"hello"}`,
			repoErr:        nil,
			wantStatus:     400,
			wantRepoCalled: false,
		},
		{
			name:           "too large request body",
			body:           `{"recipient":"123", "channel":"telegram", "message":"` + strings.Repeat("a", 70*1024) + `"}`,
			repoErr:        nil,
			wantStatus:     413,
			wantRepoCalled: false,
		},
		{
			name:           "repository error",
			body:           `{"recipient":"123", "channel":"telegram", "message":"hello"}`,
			repoErr:        errors.New("database unavailable"),
			wantStatus:     500,
			wantRepoCalled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeRepository{err: tt.repoErr}
			service := notification.NewService(repo)
			handler := NewNotificationHandler(service)

			req := httptest.NewRequest("POST", "/notifications", strings.NewReader(tt.body))
			recorder := httptest.NewRecorder()

			handler.Create(recorder, req)

			if recorder.Code != tt.wantStatus {
				t.Errorf("status code: expected %d, got %d", tt.wantStatus, recorder.Code)
			}
			if repo.called != tt.wantRepoCalled {
				t.Errorf("repository called: expected %t, got %t", tt.wantRepoCalled, repo.called)
			}

			if tt.wantStatus == 202 {
				var response createNotificationResponse

				if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
					t.Fatalf("decode response: %v", err)
				}

				if response.ID != 42 {
					t.Errorf("id: expected 42, got: %v", response.ID)
				}

				if response.Status != "pending" {
					t.Errorf("status: expected pending, got: %v", response.Status)
				}

				if recorder.Header().Get("Content-Type") != "application/json" {
					t.Errorf("Header Content-Type: expected application/json, got: %v", recorder.Header().Get("Content-Type"))
				}
			}
			if tt.repoErr != nil {
				if strings.Contains(recorder.Body.String(), tt.repoErr.Error()) {
					t.Errorf("response contains internal error details")
				}
			}

		})
	}
}
