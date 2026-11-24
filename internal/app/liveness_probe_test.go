package app

import (
	"testing"
	"time"

	binancesdk "github.com/marxms/go-whale/internal/adapters/outbound/binance"
)

// probeFeed is enough of the klineFeed surface for the health probes.
type probeFeed struct{ stats []binancesdk.PartitionStat }

func (f *probeFeed) Start([]string) error      { return nil }
func (f *probeFeed) AddSymbols([]string) error { return nil }
func (f *probeFeed) Mute([]string)             {}
func (f *probeFeed) Stop()                     {}
func (f *probeFeed) SecondsSinceLastFrame() (float64, bool) {
	return 0, true
}
func (f *probeFeed) QuietestPartition() (float64, int) { return 0, 0 }
func (f *probeFeed) NeedsRestart() bool                { return false }
func (f *probeFeed) PartitionStats() []binancesdk.PartitionStat {
	return f.stats
}

// The watchdog holds klineMu across Stop/Start (dials bounded at 40 s) while the compose
// healthcheck times out at 3 s. A probe that waited for the lock marked a recovering feed
// unhealthy; it must answer from the published snapshot instead.
func TestHealthProbesDoNotBlockWhileWatchdogHoldsTheLock(t *testing.T) {
	a := &App{
		klineFeed:  &probeFeed{stats: []binancesdk.PartitionStat{{ID: 1, Symbols: 3}}},
		klineScope: "binance:future",
	}
	a.klineMu.Lock()
	a.publishKlineSnapshotLocked()

	type result struct {
		rows    int
		rowsOK  bool
		exitOK  bool
		exhaust bool
	}
	done := make(chan result, 1)
	go func() {
		rows, ok := a.partitionHealth()
		exhausted, configured := a.klineReconnectExhaustedProbe()
		done <- result{rows: len(rows), rowsOK: ok, exitOK: configured, exhaust: exhausted}
	}()

	select {
	case got := <-done:
		if !got.rowsOK || got.rows != 1 {
			t.Fatalf("partitionHealth = %d rows (ok=%v), want 1 from the snapshot", got.rows, got.rowsOK)
		}
		if !got.exitOK || got.exhaust {
			t.Fatalf("exhausted probe = (%v, %v), want (false, true)", got.exhaust, got.exitOK)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("a health probe blocked on klineMu held by the watchdog")
	}
	a.klineMu.Unlock()
}
