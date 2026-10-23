package sender

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type fakeTransport struct {
	statusCode int
	body       string
}

func (ft *fakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: ft.statusCode,
		Body:       io.NopCloser(strings.NewReader(ft.body)),
	}, nil
}

func TestTelegramSender_Send_RateLimited(t *testing.T) {
	tests := []struct {
		name           string
		statusCode     int
		body           string
		wantError      bool
		wantCode       int
		wantRetryAfter int
	}{
		{
			name:       "success",
			statusCode: 200,
			body:       `{"ok":true}`,
			wantError:  false,
		},
		{
			name:       "too many requests",
			statusCode: 429,
			body: `{"ok":false,
					"error_code":429,
					"description":"Too Many Requests",
					"parameters":{
					"retry_after":12
					}}`,
			wantError:      true,
			wantCode:       429,
			wantRetryAfter: 12,
		},
		{
			name:       "service unavailable",
			statusCode: 503,
			body:       `<html>Unavailable</html>`,
			wantError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ft := fakeTransport{
				statusCode: tt.statusCode,
				body:       tt.body,
			}

			client := &http.Client{
				Transport: &ft,
			}
			sender := NewTelegramSender("test-token", client)
			err := sender.Send(t.Context(), "123", "hello")
			if !tt.wantError {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
				return
			}
			var tgErr *TelegramError
			if !errors.As(err, &tgErr) {
				t.Fatalf("expected TelegramError, got: %v", err)
			}
			if tgErr.HTTPStatus != tt.statusCode {
				t.Fatalf("status code: expected %d, got %d", tt.statusCode, tgErr.HTTPStatus)
			}
			if tgErr.Code != tt.wantCode {
				t.Fatalf("tg code: expected %d, got %d", tt.wantCode, tgErr.Code)
			}
			if tgErr.RetryAfter != tt.wantRetryAfter {
				t.Fatalf("retry after: expected %d, got %d", tt.wantRetryAfter, tgErr.RetryAfter)
			}
		})
	}
}
