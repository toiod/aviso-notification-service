package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	client *redis.Client
	stream string
	group  string
}

func NewRedisQueue(client *redis.Client) *RedisQueue {
	return &RedisQueue{
		client: client,
		stream: "notifications",
		group:  "telegram-workers",
	}
}

func (q *RedisQueue) Publish(ctx context.Context, notificationID int64) (string, error) {
	args := redis.XAddArgs{
		Stream: q.stream,
		ID:     "*",
		Values: map[string]any{
			"notification_id": strconv.FormatInt(notificationID, 10),
		},
	}
	streamID, err := q.client.XAdd(ctx, &args).Result()
	if err != nil {
		return "", fmt.Errorf("publish notification: %w", err)
	}
	return streamID, nil
}

func (q *RedisQueue) EnsureGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, q.stream, q.group, "0").Err()
	if err != nil {
		if redis.HasErrorPrefix(err, "BUSYGROUP") {
			return nil
		}
		return fmt.Errorf("create group: %w", err)
	}
	return nil
}

func (q *RedisQueue) Read(ctx context.Context, consumer string) ([]redis.XStream, error) {
	args := redis.XReadGroupArgs{
		Group:    q.group,
		Consumer: consumer,
		Streams:  []string{q.stream, ">"},
		Count:    1,
		Block:    5 * time.Second,
	}
	streams, err := q.client.XReadGroup(ctx, &args).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("read group: %w", err)
	}
	return streams, nil
}

func (q *RedisQueue) Ack(ctx context.Context, streamID string) error {
	err := q.client.XAck(ctx, q.stream, q.group, streamID).Err()
	if err != nil {
		return fmt.Errorf("ack error: %w", err)
	}
	return nil
}

func (q *RedisQueue) AutoClaim(ctx context.Context, consumer string, cursor string, minIdle time.Duration) ([]redis.XMessage, string, error) {
	args := redis.XAutoClaimArgs{
		Stream:   q.stream,
		Group:    q.group,
		MinIdle:  minIdle,
		Start:    cursor,
		Count:    1,
		Consumer: consumer,
	}
	messages, nextCursor, err := q.client.XAutoClaim(ctx, &args).Result()
	if err != nil {
		return nil, "", fmt.Errorf("auto claim messages: %w", err)
	}
	return messages, nextCursor, nil
}
