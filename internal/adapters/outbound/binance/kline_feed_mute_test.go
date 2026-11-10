package binancesdk

import (
	"context"
	"sync"
	"testing"
)

// Mute is read on the hot path (every frame, every symbol) and written only when discovery deranks
// something. Copy-on-write behind an atomic pointer, so this must hold under -race.
func TestKlineFeedMute(t *testing.T) {
	f := NewKlineFeed(context.Background(), "binance", "future", "5m", nil)

	if f.isMuted("BTC/USDT") {
		t.Fatal("nothing muted yet")
	}
	f.Mute([]string{"DEAD/USDT"})
	if !f.isMuted("DEAD/USDT") {
		t.Fatal("muted symbol not discarded")
	}
	if f.isMuted("BTC/USDT") {
		t.Fatal("muting one symbol silenced another")
	}

	// A second Mute must ADD, not replace: two deranks in different discovery cycles are normal.
	f.Mute([]string{"ALSODEAD/USDT"})
	if !f.isMuted("DEAD/USDT") || !f.isMuted("ALSODEAD/USDT") {
		t.Fatal("second Mute dropped the first")
	}
}

func TestKlineFeedMuteIsRaceFree(_ *testing.T) {
	f := NewKlineFeed(context.Background(), "binance", "future", "5m", nil)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 200 {
			f.Mute([]string{string(rune('a' + i%26))})
		}
	}()
	go func() {
		defer wg.Done()
		for range 2000 {
			_ = f.isMuted("BTC/USDT")
		}
	}()
	wg.Wait()
}
