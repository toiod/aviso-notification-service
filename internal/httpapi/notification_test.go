package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/toiod/aviso-notification-service/internal/notification"
)

type fakeRepository struct {
	called bool
	err    error
	result notification.Notification
}

func (f *fakeRepository) Create(ctx context.Context, recipient, channel, message string) (int64, error) {
	f.called = true
	if f.err != nil {
		return 0, f.err
	}
	return 42, nil
}

func (f *fakeRepository) GetByID(ctx context.Context, id int64) (notification.Notification, error) {
	f.called = true
	return f.result, f.err
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

func TestHandler_GetByID(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		repoResult     notification.Notification
		repoErr        error
		wantStatus     int
		wantRepoCalled bool
	}{
		{
			name: "found",
			path: "/notifications/32",
			repoResult: notification.Notification{
				ID:        32,
				Recipient: "123",
				Channel:   "telegram",
				Message:   "hello",
				Status:    "pending",
			},
			repoErr:        nil,
			wantStatus:     200,
			wantRepoCalled: true,
		},
		{
			name:           "id: abc",
			path:           "/notifications/abc",
			repoResult:     notification.Notification{},
			repoErr:        nil,
			wantStatus:     400,
			wantRepoCalled: false,
		},
		{
			name:           "id: 0",
			path:           "/notifications/0",
			repoResult:     notification.Notification{},
			repoErr:        nil,
			wantStatus:     400,
			wantRepoCalled: false,
		},
		{
			name:           "not found",
			path:           "/notifications/3241234",
			repoResult:     notification.Notification{},
			repoErr:        notification.ErrNotFound,
			wantStatus:     404,
			wantRepoCalled: true,
		},
		{
			name:           "another repo error",
			path:           "/notifications/32",
			repoResult:     notification.Notification{},
			repoErr:        errors.New("database unavailable"),
			wantStatus:     500,
			wantRepoCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeRepository{err: tt.repoErr, result: tt.repoResult}
			service := notification.NewService(repo)
			handler := NewNotificationHandler(service)

			req := httptest.NewRequest("GET", tt.path, nil)

			mux := http.NewServeMux()
			mux.HandleFunc("GET /notifications/{id}", handler.GetByID)

			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, req)

			if recorder.Code != tt.wantStatus {
				t.Errorf("status code: expected: %d, got %d", tt.wantStatus, recorder.Code)
			}
			if repo.called != tt.wantRepoCalled {
				t.Errorf("repository called: expected: %t, got %t", tt.wantRepoCalled, repo.called)
			}

			if tt.wantStatus == 200 {
				var response notification.Notification
				if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
					t.Fatalf("decode response: %v", err)
				}

				if response.ID != tt.repoResult.ID {
					t.Errorf("id: expected %v, got %v", tt.repoResult.ID, response.ID)
				}

				if response.Message != tt.repoResult.Message {
					t.Errorf("message: expected %v, got %v", tt.repoResult.Message, response.Message)
				}

				if response.Status != tt.repoResult.Status {
					t.Errorf("status: expected %v, got %v", tt.repoResult.Status, response.Status)
				}

				if recorder.Header().Get("Content-Type") != "application/json" {
					t.Errorf("Header Content-Type: expected application/json, got %v", recorder.Header().Get("Content-Type"))
				}

			}

			if tt.wantStatus == 500 && tt.repoErr != nil {
				if strings.Contains(recorder.Body.String(), tt.repoErr.Error()) {
					t.Error("response contains internal server error")
				}
			}
		})
	}
}
