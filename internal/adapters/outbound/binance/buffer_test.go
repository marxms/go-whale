package binancesdk

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/trade"
)

// TestTradeBuffer_Append_HonorsCap pins the return value to DROPPED, not to at-capacity.
//
// Both callers spend that return as "this trade was lost" — they increment
// trades_dropped_total and log an overflow — so an append that reaches cap and stores its
// trade must not report a drop: that would charge one phantom drop against a trade that is
// right there in the next flush.
func TestTradeBuffer_Append_HonorsCap(t *testing.T) {
	buf := NewTradeBuffer("BTCUSDT", 3)

	// Append within cap
	dropped := buf.Append(trade.Trade{ID: "1"})
	if dropped {
		t.Error("first append must not report a drop")
	}
	if buf.Len() != 1 {
		t.Fatalf("expected len 1, got %d", buf.Len())
	}

	// Fill to cap. The append that REACHES cap stores its trade, so it is NOT a drop.
	buf.Append(trade.Trade{ID: "2"})
	dropped = buf.Append(trade.Trade{ID: "3"})
	if dropped {
		t.Error("the append that fills the buffer STORED its trade — reporting it as dropped is the phantom-drop defect")
	}
	if buf.Len() != 3 {
		t.Fatalf("expected len 3, got %d", buf.Len())
	}

	// Overflow — must reject, not grow, and IS a drop.
	dropped = buf.Append(trade.Trade{ID: "4"})
	if !dropped {
		t.Error("expected buffer overflow to report a drop")
	}
	if buf.Len() != 3 {
		t.Fatalf("expected len still 3 after overflow, got %d", buf.Len())
	}

	// Verify only first 3 trades retained
	trades := buf.Flush()
	if len(trades) != 3 {
		t.Fatalf("expected 3 trades flushed, got %d", len(trades))
	}
	if trades[0].ID != "1" || trades[1].ID != "2" || trades[2].ID != "3" {
		t.Errorf("unexpected trade IDs: %v", trades)
	}
}

func TestTradeBuffer_Flush_Clears(t *testing.T) {
	buf := NewTradeBuffer("ETHUSDT", 10)
	buf.Append(trade.Trade{ID: "a"})
	buf.Append(trade.Trade{ID: "b"})

	first := buf.Flush()
	if len(first) != 2 {
		t.Fatalf("expected 2 trades, got %d", len(first))
	}

	second := buf.Flush()
	if len(second) != 0 {
		t.Fatalf("expected empty after flush, got %d", len(second))
	}
}

func TestTradeBuffer_TimeSinceLastAdd(t *testing.T) {
	buf := NewTradeBuffer("XRPUSDT", 10)
	before := time.Now()
	buf.Append(trade.Trade{ID: "1"})
	elapsed := buf.TimeSinceLastAdd()
	if elapsed < 0 || elapsed > time.Since(before)+10*time.Millisecond {
		t.Errorf("unexpected TimeSinceLastAdd: %v", elapsed)
	}
}

func TestBufferManager_GetOrCreate_AndSymbols(t *testing.T) {
	mgr := NewBufferManager(5)

	// First access creates
	b1 := mgr.GetOrCreate("BTCUSDT")
	if b1 == nil {
		t.Fatal("expected buffer, got nil")
	}
	if b1.Len() != 0 {
		t.Fatalf("expected empty buffer, got %d", b1.Len())
	}

	// Second access returns same instance
	b2 := mgr.GetOrCreate("BTCUSDT")
	if b1 != b2 {
		t.Error("expected same buffer instance")
	}

	// Symbols tracking
	syms := mgr.Symbols()
	if len(syms) != 1 || syms[0] != "BTCUSDT" {
		t.Errorf("expected [BTCUSDT], got %v", syms)
	}

	// Remove
	mgr.Remove("BTCUSDT")
	syms = mgr.Symbols()
	if len(syms) != 0 {
		t.Errorf("expected empty symbols, got %v", syms)
	}
}

func TestBufferManager_FlushAll(t *testing.T) {
	mgr := NewBufferManager(10)
	mgr.GetOrCreate("BTCUSDT").Append(trade.Trade{ID: "1"})
	mgr.GetOrCreate("ETHUSDT").Append(trade.Trade{ID: "2"})

	all := mgr.FlushAll()
	if len(all) != 2 {
		t.Fatalf("expected 2 symbols, got %d", len(all))
	}
	if len(all["BTCUSDT"]) != 1 || len(all["ETHUSDT"]) != 1 {
		t.Errorf("unexpected FlushAll contents: %v", all)
	}

	// Empty buffers after flush
	empty := mgr.FlushAll()
	if len(empty) != 0 {
		t.Errorf("expected empty after flush, got %v", empty)
	}
}

func TestBufferManager_EmptyFlushAll(t *testing.T) {
	mgr := NewBufferManager(10)
	all := mgr.FlushAll()
	if len(all) != 0 {
		t.Errorf("expected empty map, got %v", all)
	}
}

func TestBufferManager_ConcurrentAccess(t *testing.T) {
	mgr := NewBufferManager(100)
	done := make(chan struct{}, 100)

	for i := 0; i < 100; i++ {
		go func(n int) {
			sym := "BTCUSDT"
			mgr.GetOrCreate(sym).Append(trade.Trade{ID: string(rune('0' + n%10))})
			done <- struct{}{}
		}(i)
	}

	for i := 0; i < 100; i++ {
		<-done
	}

	buf := mgr.GetOrCreate("BTCUSDT")
	if buf.Len() > 100 {
		t.Fatalf("buffer len %d exceeds safe bound", buf.Len())
	}
	// Len should be exactly capped at 100
	if buf.Len() != 100 {
		t.Fatalf("expected len 100 (cap), got %d", buf.Len())
	}
}

// TestTradeBuffer_Flush_ReleasesBackingArray measures what TestTradeBuffer_Flush_Clears does
// not.
//
// That test checks len() after the flush and passes with `b.trades = b.trades[:0]`, which
// reduces the LENGTH and leaves the backing array untouched. Every stale slot keeps holding
// a trade.Trade, and trade.Trade carries THREE strings (ID, Symbol, Side) — live references,
// reachable by the GC, until a later append overwrites that index.
//
// Per-buffer retention is therefore the symbol's historical HIGH-WATER MARK, a value that only
// rises: a symbol that had a burst of 5,000 trades and now receives 10 per flush keeps 4,990
// slots of live strings forever. With trade_buffer_size=5000 and ~500 symbols the worst case
// is 2.5 M stale trades holding up to 7.5 M string objects.
//
// This matters because live heap is the target the GC pacer pursues: heap that does not die
// becomes repeated scanning, and live-heap growth is what crossed GOMEMLIMIT in the GC-thrash
// incident.
//
// The probe rereads the array BEYOND the length — b.trades[:3] after truncating to 0 is legal
// in Go and returns exactly the slots `[:0]` left behind. It is the only way to distinguish
// "emptied" from "hid".
func TestTradeBuffer_Flush_ReleasesBackingArray(t *testing.T) {
	buf := NewTradeBuffer("SOLUSDT", 8)
	buf.Append(trade.Trade{ID: "id-1", Symbol: "SOL/USDT", Side: "buy"})
	buf.Append(trade.Trade{ID: "id-2", Symbol: "SOL/USDT", Side: "sell"})
	buf.Append(trade.Trade{ID: "id-3", Symbol: "SOL/USDT", Side: "buy"})

	capBefore := cap(buf.trades)

	out := buf.Flush()
	if len(out) != 3 {
		t.Fatalf("expected 3 drained trades, got %d", len(out))
	}

	// No slot of the backing array may hold a reference to what was drained.
	retained := buf.trades[:3]
	for i, tr := range retained {
		if tr != (trade.Trade{}) {
			t.Errorf("slot %d retained after flush: %+v (expected a zeroed trade.Trade)", i, tr)
		}
	}

	// The pre-allocated capacity survives the flush — no reallocating 5,000 slots per cycle.
	if got := cap(buf.trades); got != capBefore {
		t.Errorf("capacity changed on flush: was %d, became %d", capBefore, got)
	}

	// What was returned to the caller is an independent copy and remains intact.
	if out[0].ID != "id-1" || out[2].Side != "buy" {
		t.Errorf("flush corrupted the returned copy: %+v", out)
	}
}

// TestTradeBuffer_Flush_EmptyIsNoOp pins that an empty-buffer flush returns nil and does not
// touch the buffer.
func TestTradeBuffer_Flush_EmptyIsNoOp(t *testing.T) {
	buf := NewTradeBuffer("ADAUSDT", 4)
	capBefore := cap(buf.trades)

	if out := buf.Flush(); out != nil {
		t.Errorf("expected nil on an empty buffer, got %#v", out)
	}
	if got := cap(buf.trades); got != capBefore {
		t.Errorf("capacity changed on an empty flush: was %d, became %d", capBefore, got)
	}
	if buf.Len() != 0 {
		t.Errorf("expected Len()==0, got %d", buf.Len())
	}
}
