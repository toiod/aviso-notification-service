package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/toiod/aviso-notification-service/internal/notification"
	"github.com/toiod/aviso-notification-service/internal/sender"
)

type fakeRepository struct {
	nf            notification.Notification
	attemptNo     int
	markSent      bool
	markedID      int64
	markedAttempt int
	markErr       error
	startCalled   bool
	markFailed    bool
	failedID      int64
	failedAttempt int
	failedMessage string
	markFailedErr error
}

type fakeSender struct {
	called    bool
	recipient string
	message   string
	err       error
}

type fakeQueue struct {
	ack      bool
	streamID string
}

func (fr *fakeRepository) GetByID(ctx context.Context, id int64) (notification.Notification, error) {
	return fr.nf, nil
}

func (fr *fakeRepository) StartAttempt(ctx context.Context, id int64) (int, error) {
	fr.startCalled = true
	return fr.attemptNo, nil
}

func (fr *fakeRepository) MarkSent(ctx context.Context, id int64, attemptNo int) error {
	fr.markSent = true
	fr.markedID = id
	fr.markedAttempt = attemptNo
	return fr.markErr
}

func (fr *fakeRepository) MarkFailed(ctx context.Context, id int64, attemptNo int, errorMessage string) error {
	fr.markFailed = true
	fr.failedID = id
	fr.failedAttempt = attemptNo
	fr.failedMessage = errorMessage

	return fr.markFailedErr
}

func (fs *fakeSender) Send(ctx context.Context, recipient, message string) error {
	fs.called = true
	fs.recipient = recipient
	fs.message = message
	return fs.err
}

func (fq *fakeQueue) Ack(ctx context.Context, streamID string) error {
	fq.streamID = streamID
	fq.ack = true
	return nil
}

func (fq *fakeQueue) Read(ctx context.Context, consumer string) ([]redis.XStream, error) {
	return nil, nil
}
func TestWorker_Process_Success(t *testing.T) {
	repo := &fakeRepository{
		nf: notification.Notification{
			ID:        33,
			Recipient: "123",
			Message:   "hello",
			Status:    "pending",
		},
		attemptNo: 1,
	}
	queue := &fakeQueue{}
	sender := &fakeSender{}

	worker := NewWorker(repo, queue, sender)
	err := worker.Process(t.Context(), "1234567890-0", 33)
	if err != nil {
		t.Fatalf("worker process: %v", err)
	}
	if sender.called != true {
		t.Errorf("sender called: expected %t, got %t", true, sender.called)
	}
	if sender.recipient != "123" {
		t.Errorf("sender recipient: expected %s, got %s", "123", sender.recipient)
	}
	if sender.message != "hello" {
		t.Errorf("sender message: expected %s, got %s", "hello", sender.message)
	}
	if repo.markSent != true {
		t.Errorf("repo mark sent: expected %t, got %t", true, repo.markSent)
	}
	if repo.markedID != 33 {
		t.Errorf("repo marked id: expected %d, got %d", 33, repo.markedID)
	}
	if repo.markedAttempt != 1 {
		t.Errorf("repo marked attempt: expected %d, got %d", 1, repo.markedAttempt)
	}
	if queue.ack != true {
		t.Errorf("queue ack: expected %t, got %t", true, queue.ack)
	}
	if queue.streamID != "1234567890-0" {
		t.Errorf("queue stream id: expected %s, got %s", "1234567890-0", queue.streamID)
	}
}

func TestWorker_Process_MarkErr(t *testing.T) {
	markErr := errors.New("database unavailable")

	repo := &fakeRepository{
		nf: notification.Notification{
			ID:        33,
			Recipient: "123",
			Message:   "hello",
			Status:    "pending",
		},
		attemptNo: 1,
		markErr:   markErr,
	}
	queue := &fakeQueue{}
	sender := &fakeSender{}
	worker := NewWorker(repo, queue, sender)
	err := worker.Process(t.Context(), "1234567890-0", 33)
	if !errors.Is(err, markErr) {
		t.Fatalf("worker process error: expected %v, got %v", markErr, err)
	}
	if sender.called != true {
		t.Errorf("sender called: expected %t, got %t", true, sender.called)
	}
	if repo.markSent != true {
		t.Errorf("repo mark sent: expected %t, got %t", true, repo.markSent)
	}
	if queue.ack != false {
		t.Errorf("queue ack: expected %t, got %t", false, queue.ack)
	}

}

func TestWorker_Process_Terminal(t *testing.T) {
	repo := &fakeRepository{
		nf: notification.Notification{
			ID:        33,
			Recipient: "123",
			Message:   "hello",
			Status:    "sent",
		},
		attemptNo: 1,
	}
	queue := &fakeQueue{}
	sender := &fakeSender{}
	worker := NewWorker(repo, queue, sender)
	err := worker.Process(t.Context(), "1234567890-0", 33)
	if err != nil {
		t.Fatalf("worker process: %v", err)
	}
	if queue.ack != true {
		t.Errorf("queue ack: expected %t, got %t", true, queue.ack)
	}
	if sender.called != false {
		t.Errorf("sender called: expected %t, got %t", false, sender.called)
	}
	if repo.markSent != false {
		t.Errorf("repo mark sent: expected %t, got %t", false, repo.markSent)
	}
	if repo.startCalled != false {
		t.Errorf("repo start call: expected %t, got %t", false, repo.startCalled)
	}
	if queue.streamID != "1234567890-0" {
		t.Errorf("queue stream id: expected %s, got %s", "1234567890-0", queue.streamID)
	}
}

func TestWorker_Process_PermanentError(t *testing.T) {
	repo := &fakeRepository{
		nf: notification.Notification{
			ID:        33,
			Recipient: "123",
			Message:   "hello",
			Status:    "pending",
		},
		attemptNo: 1,
	}
	sendErr := &sender.TelegramError{
		HTTPStatus:  403,
		Code:        403,
		Description: "bot was blocked",
	}
	queue := &fakeQueue{}
	fakeSend := &fakeSender{err: sendErr}
	worker := NewWorker(repo, queue, fakeSend)
	err := worker.Process(t.Context(), "1234567890-0", 33)
	if err != nil {
		t.Fatalf("worker process: %v", err)
	}
	if repo.markFailed != true {
		t.Errorf("mark failed: expected %t, got %t", true, repo.markFailed)
	}
	if repo.failedID != 33 {
		t.Errorf("failed id: expected %d, got %d", 33, repo.failedID)
	}
	if repo.failedAttempt != 1 {
		t.Errorf("failed attempt: expected %d, got %d", 1, repo.failedAttempt)
	}
	if repo.failedMessage != sendErr.Error() {
		t.Errorf("failed message: expected %s, got %s", sendErr.Error(), repo.failedMessage)
	}
	if repo.markSent != false {
		t.Errorf("mark sent: expected %t, got %t", false, repo.markSent)
	}
	if queue.ack != true {
		t.Errorf("queue ack: expected %t, got %t", true, queue.ack)
	}
	if queue.streamID != "1234567890-0" {
		t.Errorf("queue ack: expected %s, got %s", "1234567890-0", queue.streamID)
	}
}
