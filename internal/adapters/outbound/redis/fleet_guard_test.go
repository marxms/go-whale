package redisstore

import (
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/platform/config"
)

func newFleetStore(t *testing.T) *RedisStore {
	t.Helper()
	mr := miniredis.RunT(t)
	store, err := NewRedisStore(t.Context(), config.Config{
		RedisAddr:          mr.Addr(),
		RedisPoolSize:      4,
		RedisDialTimeoutS:  1,
		RedisReadTimeoutS:  1,
		RedisWriteTimeoutS: 1,
		RedisPoolTimeoutS:  1,
		RedisMaxRetries:    1,
	}, candleStoretestCap, candle.NewTimeframeRegistry([]string{"1h"}))
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// A write for a symbol the fleet no longer contains must not widen the authoritative registry.
// Gap backfills and seeds still run for those symbols, and every write used to re-register itself,
// so the product showed more assets than discovery.max_assets until the next reconciliation.
func TestAddActiveSymbolRefusesASymbolOutsideTheFleet(t *testing.T) {
	store := newFleetStore(t)
	store.SetFleet("binance", "future", []string{"BTC/USDT", "ETH/USDT"})

	if err := store.AddActiveSymbol(t.Context(), "binance", "future", "SOMETHING/USDT"); err != nil {
		t.Fatalf("AddActiveSymbol outsider: %v", err)
	}
	if err := store.AddActiveSymbol(t.Context(), "binance", "future", "BTC/USDT"); err != nil {
		t.Fatalf("AddActiveSymbol member: %v", err)
	}

	members, err := store.GetActiveSymbols(t.Context(), "binance", "future")
	if err != nil {
		t.Fatalf("GetActiveSymbols: %v", err)
	}
	if len(members) != 1 || members[0] != "BTC/USDT" {
		t.Fatalf("members = %v, want only the fleet member", members)
	}
}

// No fleet known is not the same as an empty fleet: a caller that never declares one keeps the
// behaviour from before the guard, so the guard cannot invent a restriction from missing evidence.
func TestAddActiveSymbolRegistersWhenNoFleetIsKnown(t *testing.T) {
	store := newFleetStore(t)

	if err := store.AddActiveSymbol(t.Context(), "binance", "future", "ANY/USDT"); err != nil {
		t.Fatalf("AddActiveSymbol: %v", err)
	}
	members, err := store.GetActiveSymbols(t.Context(), "binance", "future")
	if err != nil {
		t.Fatalf("GetActiveSymbols: %v", err)
	}
	if len(members) != 1 || members[0] != "ANY/USDT" {
		t.Fatalf("members = %v, want the symbol to be registered", members)
	}
}

// The fleet is per scope: setting one scope must not restrict another.
func TestFleetIsScopedToItsExchangeAndMarket(t *testing.T) {
	store := newFleetStore(t)
	store.SetFleet("binance", "future", []string{"BTC/USDT"})

	if err := store.AddActiveSymbol(t.Context(), "bingx", "spot", "OTHER/USDT"); err != nil {
		t.Fatalf("AddActiveSymbol other scope: %v", err)
	}
	members, err := store.GetActiveSymbols(t.Context(), "bingx", "spot")
	if err != nil {
		t.Fatalf("GetActiveSymbols: %v", err)
	}
	if len(members) != 1 || members[0] != "OTHER/USDT" {
		t.Fatalf("members = %v, want the other scope untouched", members)
	}
}
