package sender

import "context"

type Sender interface {
	Send(ctx context.Context, recipient, message string) error
}
