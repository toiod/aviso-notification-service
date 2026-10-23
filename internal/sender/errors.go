package sender

import "fmt"

type TelegramError struct {
	HTTPStatus  int
	Code        int
	Description string
	RetryAfter  int
}

func (e *TelegramError) Error() string {
	return fmt.Sprintf("HTTP Status: %d, Code: %d, Description: %s", e.HTTPStatus, e.Code, e.Description)
}
