package outbox

type Event struct {
	ID             int64
	NotificationID int64
}
