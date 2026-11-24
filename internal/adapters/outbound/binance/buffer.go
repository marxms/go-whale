package binancesdk

import (
	"sync"
	"time"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/trade"
)

// TradeBuffer holds trades for a single symbol with cooperative flush.
type TradeBuffer struct {
	symbol  string
	trades  []trade.Trade
	cap     int
	mu      sync.Mutex
	lastAdd time.Time
	maxLen  int
	lastDrain time.Time
}

// NewTradeBuffer creates a new buffer for the given symbol and capacity.
//
// Only the first tradeInitialPrealloc slots are pre-allocated. The 2026-11-23
// production tuning raised the cap to 12000 for a gap-recovery burst; allocating
// the full cap for all 500 symbols pushed the GC headroom into the degraded
// band. The slice still grows to cap, so the burst guarantee is unchanged.
func NewTradeBuffer(symbol string, capacity int) *TradeBuffer {
	initial := capacity
	if initial > tradeInitialPrealloc {
		initial = tradeInitialPrealloc
	}
	return &TradeBuffer{symbol: symbol, cap: capacity, trades: make([]trade.Trade, 0, initial)}
}

const tradeInitialPrealloc = 500

// Append adds a trade and reports whether THIS trade was DROPPED.
//
// The caller spends this value as "the trade was lost": it increments trades_dropped_total
// and logs "buffer overflow". Those are not the same event. The append that takes the
// buffer from cap-1 to cap STORES its trade, so returning at-capacity there would charge
// one phantom drop against a trade sitting in the slice — TestTradeBuffer_Append_HonorsCap
// asserts full==true and Len()==3 on that very call.
//
// A drop counter that fires on a trade it did not lose is worse than no counter: it makes
// "we ingested fewer trades" indistinguishable from "a buffer filled once", which is the
// only discrimination trades_dropped_total exists to provide.
//
// Drop BEHAVIOR: at capacity the TRIGGERING (newest) trade is discarded — drop-newest, not
// a ring buffer. In production the cap is large (default 30000) and the buffer drains every
// scheduler tick, so a true drop is rare (~299 in 7d).
func (b *TradeBuffer) Append(trade trade.Trade) (dropped bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.trades) >= b.cap {
		return true
	}

	b.trades = append(b.trades, trade)
	// Max fill is the high-water mark since creation, not the current length:
	// a buffer that burst and drained must keep the evidence of the burst.
	if len(b.trades) > b.maxLen {
		b.maxLen = len(b.trades)
	}
	b.lastAdd = time.Now()
	return false
}

// Flush returns and clears all buffered trades. It returns nil (not an empty
// slice) when the buffer is empty to skip the make+copy allocation on the hot
// path — this method runs ~10k/s across hundreds of symbols and the common case
// is an empty buffer. Callers (HybridManager.processClient, BufferManager.FlushAll)
// already guard on len() == 0, so a nil result is handled identically to empty.
func (b *TradeBuffer) Flush() []trade.Trade {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.trades) == 0 {
		return nil
	}
	out := make([]trade.Trade, len(b.trades))
	copy(out, b.trades)
	// The drain clock is set only when trades actually left the buffer. An
	// empty poll must not refresh the age and hide a stalled drain.
	b.lastDrain = time.Now()

	// clear BEFORE truncating, and that order is the whole point.
	//
	// `b.trades[:0]` alone reduces the LENGTH and leaves the backing array untouched: every
	// drained slot keeps holding a trade.Trade, and trade.Trade carries three strings (ID,
	// Symbol, Side). They remain reachable by the GC until a later append overwrites that
	// index, so per-buffer retention is the symbol's historical HIGH-WATER MARK — a value
	// that only rises. A symbol that had a burst of 5,000 trades and now receives 10 per
	// flush kept 4,990 slots of live strings indefinitely.
	//
	// `clear` operates on 0..len-1, so truncating first would clear zero elements. After the
	// clear the truncation is only bookkeeping, and the pre-allocated capacity survives — no
	// reallocating 5,000 slots per flush cycle.
	clear(b.trades)
	b.trades = b.trades[:0]
	return out
}

// Len returns the current number of buffered trades.
func (b *TradeBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.trades)
}

// TimeSinceLastAdd returns duration since the last trade was added.
func (b *TradeBuffer) TimeSinceLastAdd() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return time.Since(b.lastAdd)
}

// stats returns the locked snapshot the sampler publishes.
func (b *TradeBuffer) stats() streaming.BufferStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return streaming.BufferStats{
		Current:   len(b.trades),
		Max:       b.maxLen,
		Capacity:  b.cap,
		LastDrain: b.lastDrain,
	}
}

// BufferManager manages per-symbol TradeBuffers.
type BufferManager struct {
	buffers map[string]*TradeBuffer
	cap     int
	mu      sync.RWMutex
}

// NewBufferManager creates a manager with the given per-symbol capacity.
func NewBufferManager(capacity int) *BufferManager {
	return &BufferManager{buffers: make(map[string]*TradeBuffer), cap: capacity}
}

// GetOrCreate returns the buffer for a symbol, creating it if needed.
func (m *BufferManager) GetOrCreate(symbol string) *TradeBuffer {
	m.mu.RLock()
	b, ok := m.buffers[symbol]
	m.mu.RUnlock()
	if ok {
		return b
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if b, ok := m.buffers[symbol]; ok {
		return b
	}
	b = NewTradeBuffer(symbol, m.cap)
	m.buffers[symbol] = b
	return b
}

// Remove drops the buffer for a symbol.
func (m *BufferManager) Remove(symbol string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.buffers, symbol)
}

// GetBufferStats returns the exact snapshot for a symbol. false means the
// symbol has no buffer in this manager, which the sampler reads as "not
// measured here" rather than as an empty buffer.
func (m *BufferManager) GetBufferStats(symbol string) (streaming.BufferStats, bool) {
	m.mu.RLock()
	buffer, ok := m.buffers[symbol]
	m.mu.RUnlock()
	if !ok {
		return streaming.BufferStats{}, false
	}
	return buffer.stats(), true
}

// FlushAll returns a map of symbol → trades for all non-empty buffers.
func (m *BufferManager) FlushAll() map[string][]trade.Trade {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][]trade.Trade)
	for sym, buf := range m.buffers {
		trades := buf.Flush()
		if len(trades) > 0 {
			out[sym] = trades
		}
	}
	return out
}

// Symbols returns all currently tracked symbols.
func (m *BufferManager) Symbols() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.buffers))
	for sym := range m.buffers {
		out = append(out, sym)
	}
	return out
}
