package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// PrunerClient adapts Redis to the pruning package's KeyStore/AnchorFloorStore
// ports.
//
// The port lives in the consumer (internal/pruning); this type is the only place
// that knows a *redis.Client is behind it. The anchor-floor key name is passed
// in by the composition root so this adapter does not import the consumer — the
// consumer imports this package for key layouts, and importing back would be a
// cycle.
type PrunerClient struct {
	client         *redis.Client
	anchorFloorKey string
}

// NewPrunerClient builds the adapter over an existing store.
func NewPrunerClient(store *RedisStore, anchorFloorKey string) *PrunerClient {
	return &PrunerClient{client: store.Client(), anchorFloorKey: anchorFloorKey}
}

// ScanKeys advances one SCAN page from cursor. Passing the cursor through is the whole
// point: hardcoding 0 makes every iteration re-read page one and return the same non-zero
// cursor, and the caller's correct pagination loop (cursor = next; break when 0) never
// terminates.
//
// This is not theoretical. The three scanned patterns hold 201, 1,680 and 203 keys in
// production against this COUNT of 100, so the very first call returns a non-zero cursor
// for all of them. The owning goroutine would spin on SCAN forever, and every housekeeping
// step after the call would be unreachable.
func (c *PrunerClient) ScanKeys(ctx context.Context, cursor uint64, pattern string) ([]string, uint64, error) {
	cmd := c.client.Scan(ctx, cursor, pattern, 100)
	return cmd.Result()
}

// UnlinkKeys removes keys with UNLINK (asynchronous reclamation).
func (c *PrunerClient) UnlinkKeys(ctx context.Context, keys ...string) (int64, error) {
	cmd := c.client.Unlink(ctx, keys...)
	return cmd.Result()
}

// AnchorFloor reads the last accepted active-set cardinality.
func (c *PrunerClient) AnchorFloor(ctx context.Context) (int, bool, error) {
	raw, err := c.client.Get(ctx, c.anchorFloorKey).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("pruner: read anchor floor: %w", err)
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		// An unreadable value is treated as ABSENT, not as zero: zero would pass any
		// comparison and silently disarm the guard.
		return 0, false, nil
	}
	return n, true, nil
}

// SetAnchorFloor persists the accepted cardinality without a TTL.
func (c *PrunerClient) SetAnchorFloor(ctx context.Context, n int) error {
	if err := c.client.Set(ctx, c.anchorFloorKey, n, 0).Err(); err != nil {
		return fmt.Errorf("pruner: write anchor floor: %w", err)
	}
	return nil
}
