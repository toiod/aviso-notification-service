package queue

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisQueue_PublishReadAck(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{
		Addr:                  addr,
		ContextTimeoutEnabled: true,
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}

	queue := NewRedisQueue(client)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	queue.stream = "test:notifications:" + suffix
	queue.group = "test:telegram-workers:" + suffix
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := client.Del(cleanupCtx, queue.stream).Err(); err != nil {
			t.Errorf("delete test stream: %v", err)
		}
	}()
	err := queue.EnsureGroup(ctx)
	if err != nil {
		t.Fatalf("ensure group error: %v", err)
	}
	err = queue.EnsureGroup(ctx)
	if err != nil {
		t.Fatalf("second ensure group call error: %v", err)
	}
	streamID, err := queue.Publish(ctx, 33)
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}
	stream, err := queue.Read(ctx, "test-consumer")
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if len(stream) != 1 {
		t.Fatalf("expected one stream, got: %v", len(stream))
	}
	if len(stream[0].Messages) != 1 {
		t.Fatalf("expected one element in stream messages, got: %v", len(stream[0].Messages))
	}
	if stream[0].Messages[0].ID != streamID {
		t.Errorf("stream ID: expected %q, got %q", streamID, stream[0].Messages[0].ID)
	}
	if stream[0].Messages[0].Values["notification_id"] != "33" {
		t.Errorf("value id: expected %q, got %q", "33", stream[0].Messages[0].Values["notification_id"])
	}

	pending, err := client.XPending(ctx, queue.stream, queue.group).Result()
	if err != nil {
		t.Fatalf("pending error: %v", err)
	}
	if pending.Count != 1 {
		t.Fatalf("pending count: expected %d, got %d", 1, pending.Count)
	}

	err = queue.Ack(ctx, streamID)
	if err != nil {
		t.Fatalf("ack error: %v", err)
	}

	pending, err = client.XPending(ctx, queue.stream, queue.group).Result()
	if err != nil {
		t.Fatalf("pending error after ack: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending count after ack: expected %d, got %d", 0, pending.Count)
	}
}

func TestRedisQueue_AutoClaim(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{
		Addr:                  addr,
		ContextTimeoutEnabled: true,
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}

	queue := NewRedisQueue(client)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	queue.stream = "test:notifications:" + suffix
	queue.group = "test:telegram-workers:" + suffix
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := client.Del(cleanupCtx, queue.stream).Err(); err != nil {
			t.Errorf("delete test stream: %v", err)
		}
	}()
	err := queue.EnsureGroup(ctx)
	if err != nil {
		t.Fatalf("ensure group error: %v", err)
	}
	streamID, err := queue.Publish(ctx, 33)
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}
	stream, err := queue.Read(ctx, "old-worker")
	if err != nil {
		t.Fatalf("queue read: %v", err)
	}
	if len(stream) != 1 {
		t.Fatalf("expected one stream, got: %v", len(stream))
	}
	if len(stream[0].Messages) != 1 {
		t.Fatalf("expected one element in stream messages, got: %v", len(stream[0].Messages))
	}

	messages, nextCursor, err := queue.AutoClaim(ctx, "new-worker", "0-0", 0)
	if err != nil {
		t.Fatalf("auto claim: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one message, got: %v", len(messages))
	}
	if messages[0].ID != streamID {
		t.Fatalf("message id: expected %v, got %v", streamID, messages[0].ID)
	}

	if messages[0].Values["notification_id"] != "33" {
		t.Fatalf("message notification id: expected %s, got %s", "33", messages[0].Values["notification_id"])
	}
	if nextCursor != "0-0" {
		t.Fatalf("next cursor: expected %s, got %s", "0-0", nextCursor)
	}

	pending, err := client.XPending(ctx, queue.stream, queue.group).Result()
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if pending.Count != 1 {
		t.Fatalf("pending count: expected %v, got %v", 1, pending.Count)
	}
	if pending.Consumers["new-worker"] != 1 {
		t.Fatalf("new-worker: expected %v, got %v", 1, pending.Consumers["new-worker"])
	}
	if pending.Consumers["old-worker"] != 0 {
		t.Fatalf("old-worker: expected %v, got %v", 0, pending.Consumers["old-worker"])
	}
	if err := queue.Ack(ctx, streamID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	pending, err = client.XPending(ctx, queue.stream, queue.group).Result()
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending count: expected %v, got %v", 0, pending.Count)
	}
}
