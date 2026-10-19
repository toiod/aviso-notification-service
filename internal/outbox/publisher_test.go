package outbox

import (
	"context"
	"errors"
	"testing"
)

type fakeRepository struct {
	events     []Event
	markCalled bool
	markedID   int64
	markErr    error
}

type fakeQueue struct {
	called         bool
	notificationID int64
	err            error
}

func (fr *fakeRepository) ListUnpublished(ctx context.Context, limit int) ([]Event, error) {
	return fr.events, nil
}
func (fr *fakeRepository) MarkPublished(ctx context.Context, outboxID int64) error {
	fr.markCalled = true
	fr.markedID = outboxID
	return fr.markErr
}

func (fq *fakeQueue) Publish(ctx context.Context, notificationID int64) (string, error) {
	fq.called = true
	fq.notificationID = notificationID
	if fq.err != nil {
		return "", fq.err
	}
	return "1234567890-0", nil
}

func TestPublisher_RunOnce_Success(t *testing.T) {
	repo := &fakeRepository{
		events: []Event{
			{ID: 7, NotificationID: 33},
		},
	}
	queue := &fakeQueue{}
	publisher := NewPublisher(repo, queue)
	err := publisher.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("run once publisher: %v", err)
	}
	if queue.called != true {
		t.Fatalf("queue called: expected: %t, got %t", true, queue.called)
	}
	if queue.notificationID != 33 {
		t.Fatalf("queue notification id: expected: %d, got %d", 33, queue.notificationID)
	}
	if repo.markCalled != true {
		t.Fatalf("repo called: expected: %t, got %t", true, repo.markCalled)
	}
	if repo.markedID != 7 {
		t.Fatalf("repo marked id: expected: %d, got %d", 7, repo.markedID)
	}

}
func TestPublisher_RunOnce_PublishError(t *testing.T) {
	publishErr := errors.New("Redis unavailable")
	repo := &fakeRepository{
		events: []Event{
			{ID: 7, NotificationID: 33},
		},
	}
	queue := &fakeQueue{
		err: publishErr,
	}
	publisher := NewPublisher(repo, queue)
	err := publisher.RunOnce(t.Context())

	if !errors.Is(err, publishErr) {
		t.Fatalf("error: expected %v, got %v", publishErr, err)
	}
	if queue.called != true {
		t.Fatalf("queue called: expected: %t, got %t", true, queue.called)
	}
	if repo.markCalled != false {
		t.Fatalf("repo called: expected: %t, got %t", false, repo.markCalled)
	}
}

func TestPublisher_RunOnce_MarkPublishedError(t *testing.T) {
	markErr := errors.New("Mark error")
	repo := &fakeRepository{
		events: []Event{
			{ID: 7, NotificationID: 33},
		},
		markErr: markErr,
	}
	queue := &fakeQueue{}

	publisher := NewPublisher(repo, queue)

	err := publisher.RunOnce(t.Context())

	if !errors.Is(err, markErr) {
		t.Fatalf("error: expected %v, got %v", markErr, err)
	}
	if !queue.called {
		t.Fatalf("queue called: expected: %t, got %t", true, queue.called)
	}
	if !repo.markCalled {
		t.Fatalf("repo called: expected: %t, got %t", true, repo.markCalled)
	}
}
