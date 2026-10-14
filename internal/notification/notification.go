package notification

import "time"

type Notification struct {
	ID            int64
	Recipient     string
	Channel       string
	Message       string
	Status        string
	Attempts      int
	CreatedAt     time.Time
	SentAt        *time.Time
	LastError     *string
	NextAttemptAt *time.Time
}
