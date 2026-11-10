// Package redisevents publishes realtime events through Redis Pub/Sub.
package redisevents

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Publisher adapts a go-redis client to the Publisher interface. It is the only
// file in this package that depends on go-redis, keeping the event contract and the
// Producer transport-agnostic and unit-testable with a fake.
type Publisher struct {
	client *redis.Client
}

// NewPublisher wraps an existing go-redis client. The caller owns the client's
// lifecycle; this adapter only publishes.
func NewPublisher(client *redis.Client) Publisher {
	return Publisher{client: client}
}

// Publish sends payload on channel, returning the transport error (if any).
func (p Publisher) Publish(ctx context.Context, channel string, payload []byte) error {
	return p.client.Publish(ctx, channel, payload).Err()
}
