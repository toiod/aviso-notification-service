package notification

import "time"

type Notification struct {
	ID            int64      `json:"id"`
	Recipient     string     `json:"recipient"`
	Channel       string     `json:"channel"`
	Message       string     `json:"message"`
	Status        string     `json:"status"`
	Attempts      int        `json:"attempts"`
	CreatedAt     time.Time  `json:"created_at"`
	SentAt        *time.Time `json:"sent_at"`
	LastError     *string    `json:"last_error"`
	NextAttemptAt *time.Time `json:"next_attempt_at"`
}
