package redisstore

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
)

// Exercise the production constructor, not a separately configured test client.
func TestProductionRedisHonorsDerivativesReadDeadline(t *testing.T) {
	for _, command := range []string{"zrevrange", "get", "zrangebyscore"} {
		t.Run(command, func(t *testing.T) {
			mr := miniredis.RunT(t)
			reg := candle.NewTimeframeRegistry([]string{"30m"})
			s, err := NewRedisStore(t.Context(), config.Config{
				RedisAddr: mr.Addr(), RedisPoolSize: 2,
				RedisDialTimeoutS: 1, RedisReadTimeoutS: 1, RedisWriteTimeoutS: 1,
				RedisPoolTimeoutS: 1, RedisMaxRetries: -1,
			}, 200, reg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Client().Close(); err != nil {
					t.Errorf("close Redis client: %v", err)
				}
			})
			d := NewDerivativesRedisStore(s.Client(), config.DerivativesConfig{}, reg)
			seen := make(chan struct{}, 1)
			mr.Server().SetPreHook(func(_ *server.Peer, cmd string, _ ...string) bool {
				if strings.ToLower(cmd) == command {
					seen <- struct{}{}
					return true // command accepted, but no response on the socket
				}
				return false
			})
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			started := time.Now()
			switch command {
			case "zrevrange":
				_, err = d.GetSnapshotSeries(ctx, "binance", "future", "BTC/USDT", "30m", 3)
			case "get":
				_, err = d.GetBaseline(ctx, "binance", "future", "BTC/USDT", "30m")
			case "zrangebyscore":
				_, _, err = d.PriceIncrement(ctx, "binance", "future", "BTC/USDT", "30m", 1_700_000_100_000)
			}
			if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
				t.Fatalf("context deadline ignored on socket: elapsed=%v", elapsed)
			}
			var timeout net.Error
			if !errors.As(err, &timeout) || !timeout.Timeout() {
				t.Fatalf("unanswered physical read did not time out: %v", err)
			}
			// The socket timer may fire before Go schedules the context timer.
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
				t.Fatal("context deadline timer never completed")
			}
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("unanswered physical read: err=%v ctx=%v", err, ctx.Err())
			}
			select {
			case <-seen:
			default:
				t.Fatal("fixture never received the physical command")
			}
			mr.Server().SetPreHook(nil)
			if _, err := d.GetSnapshotSeries(context.Background(), "binance", "future", "BTC/USDT", "30m", 3); err != nil {
				t.Fatalf("pool did not recover after deadline: %v", err)
			}
		})
	}
}
