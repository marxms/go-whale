package binancesdk

import (
	"context"
	"testing"
)

// MUTE WITHOUT UNMUTE IS A ONE-WAY DOOR, AND DISCOVERY IS A REVOLVING ONE.
//
// The fleet is a liquidity-ranked top-500 refreshed every six hours, and the margin at the cut is
// about one percent of 24h volume — symbols cross it in BOTH directions, routinely. Mute shipped
// without its sibling, so a symbol deranked in one cycle and re-promoted in a later one was
// re-subscribed, re-seeded, counted in klineSymbolsAttached and logged as attached, while the
// handler's first statement discarded every one of its frames.
//
// With ownership silencing the tape for the whole scope, that symbol had NO producer on any of the
// eight timeframes. The watchdog does not rescue it either: a discovery cycle packs its symbols
// onto ONE partition, so a genuinely-new symbol on that connection keeps its clock fresh and the
// feed never looks silent.
func TestAddSymbolsClearsAStaleMute(t *testing.T) {
	f := NewKlineFeed(context.Background(), "binance", "future", "5m", nil)

	f.Mute([]string{"BACK/USDT", "STILLGONE/USDT"})
	if !f.isMuted("BACK/USDT") {
		t.Fatal("setup: symbol was not muted")
	}

	// AddSymbols dials, which a unit test must not do, so exercise the contract AddSymbols relies
	// on: attaching a symbol clears the stale instruction to silence it.
	f.Unmute([]string{"BACK/USDT"})

	if f.isMuted("BACK/USDT") {
		t.Fatal("a re-promoted symbol is still silenced — it has NO producer on any timeframe")
	}
	if !f.isMuted("STILLGONE/USDT") {
		t.Fatal("unmuting one symbol released another that is still deranked")
	}
}

// Unmute on a feed that never muted anything must be a no-op, not a panic on the nil map pointer.
func TestUnmuteBeforeAnyMute(t *testing.T) {
	f := NewKlineFeed(context.Background(), "binance", "future", "5m", nil)
	f.Unmute([]string{"BTC/USDT"})
	if f.isMuted("BTC/USDT") {
		t.Fatal("unmute invented a mute")
	}
}
