package engine

import (
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

const unionStep = int64(5 * 60 * 1000)

func unionBase() int64 {
	return time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC).UnixMilli()
}

// TestSeedCandles_FresherBatchDoesNotDestroyLiveDetail guards against a replacement branch
// destroying richer data.
//
// A batch that REPLACES the buffer overwrites every shared bucket with the batch's version, and
// the two versions are not equivalent: the live buffer holds trade-by-trade accumulation (the
// taker split, per-bucket order flow), while a REST kline carries a coarser total and may carry
// no taker split at all. A repair meant to ADD the buckets the stream missed would also SUBTRACT
// detail from the buckets it did not miss.
//
// THE BATCH HERE CARRIES NO ORDER FLOW, and that is why it still must not replace even though an
// authoritative exchange record CAN settle a closed bucket. The seeder degrades to plain
// FetchOHLCV under rate limits, so a "total" can arrive with a volume and no taker split.
// Settling with it would move the volume while leaving the accumulated pair in place, and if the
// volume moves down the bucket ends up claiming more classified flow than it had trades. A
// record that did not measure the order flow cannot settle the order flow — see
// isAuthoritativeClosedTotal.
func TestSeedCandles_FresherBatchDoesNotDestroyLiveDetail(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "BTC/USDT"
		tf       = "5m"
	)
	base := unionBase()
	ce := NewCandleEngine(map[string]int{"default": 16})

	// The live buffer: four buckets built from actual trades.
	live := make([]candle.Candle, 0, 4)
	for i := range 4 {
		live = append(live, candle.Candle{
			Timestamp: base + int64(i)*unionStep,
			Open:      100, High: 110, Low: 90, Close: 105,
			Volume:          100,
			TakerBuyVolume:  70,
			TakerSellVolume: 30,
			Source:          string(sourceWSDelta),
		})
	}
	ce.SeedCandles(exchange, market, symbol, tf, live)

	// The repair: FRESHER (reaches base+5), overlapping the live buckets base+2 and base+3
	// with the coarser REST view — lower volume, no taker split, no absolute CVD level.
	batch := make([]candle.Candle, 0, 4)
	for i := 2; i < 6; i++ {
		batch = append(batch, candle.Candle{
			Timestamp: base + int64(i)*unionStep,
			Open:      100, High: 108, Low: 92, Close: 104,
			Volume: 10,
			Source: string(sourceRESTTotal),
		})
	}
	ce.SeedCandles(exchange, market, symbol, tf, batch)

	// The overlapped bucket must keep every field the live path had and the batch lacks.
	overlapped := candleAt(ce, exchange, market, symbol, tf, base+2*unionStep)
	if overlapped == nil {
		t.Fatal("the overlapped bucket vanished entirely")
	}
	if overlapped.Volume != 100 {
		t.Errorf("Volume = %v, want 100 — the live accumulation was overwritten by the coarser "+
			"REST total, so the bucket now under-reports the volume that actually traded",
			overlapped.Volume)
	}
	if overlapped.TakerBuyVolume != 70 || overlapped.TakerSellVolume != 30 {
		t.Errorf("taker split = %v/%v, want 70/30 — order flow was destroyed by the repair, which "+
			"blanks CVD and aggression for a bucket that had them",
			overlapped.TakerBuyVolume, overlapped.TakerSellVolume)
	}

	// And the repair must still have delivered what it was for.
	for i := 4; i < 6; i++ {
		if candleAt(ce, exchange, market, symbol, tf, base+int64(i)*unionStep) == nil {
			t.Errorf("the fresher bucket at index %d was not admitted — the repair lost its purpose", i)
		}
	}
}

// TestSeedCandles_FresherBatchKeepsOlderBucketsOutsideItsSpan pins the second half of the
// same defect: replacement does not only coarsen the overlap, it TRUNCATES the history.
//
// A batch that starts later than the buffer does (every gap-recovery window does) silently
// dropped every bucket older than its first — the hot window shrank, and consumers reading
// depth got a shorter series with no log saying why.
func TestSeedCandles_FresherBatchKeepsOlderBucketsOutsideItsSpan(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "ETH/USDT"
		tf       = "5m"
	)
	base := unionBase()
	ce := NewCandleEngine(map[string]int{"default": 16})

	live := make([]candle.Candle, 0, 4)
	for i := range 4 {
		live = append(live, candle.Candle{
			Timestamp: base + int64(i)*unionStep,
			Close:     100, Volume: 50,
		})
	}
	ce.SeedCandles(exchange, market, symbol, tf, live)

	batch := make([]candle.Candle, 0, 4)
	for i := 2; i < 6; i++ {
		batch = append(batch, candle.Candle{
			Timestamp: base + int64(i)*unionStep,
			Close:     101, Volume: 60,
			Source: string(sourceRESTTotal),
		})
	}
	ce.SeedCandles(exchange, market, symbol, tf, batch)

	for i := range 2 {
		if candleAt(ce, exchange, market, symbol, tf, base+int64(i)*unionStep) == nil {
			t.Errorf("the bucket at index %d was dropped — it lies before the batch's span, so the "+
				"repair truncated history it was never meant to touch", i)
		}
	}
}

// TestSeedCandles_BatchTotalsNeverInflateVolume pins the direction of the union rule.
//
// mergeLateIntoClosed dispatches on the DECLARED source, and a candle read back from Redis
// on a warm boot carries whatever source was persisted — including ws_delta. Routing a seed
// batch through that dispatch would take the ADDITIVE branch and sum a whole-bucket total
// into an already-complete bucket, doubling it.
//
// A seed batch is by construction a set of bucket TOTALS (REST klines or persisted
// snapshots), never increments, so the batch union MAX-merges unconditionally. This follows
// the asymmetry the live merge already documents: an under-count is recoverable, an
// inflation is not.
func TestSeedCandles_BatchTotalsNeverInflateVolume(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "SOL/USDT"
		tf       = "5m"
	)
	base := unionBase()
	ce := NewCandleEngine(map[string]int{"default": 16})

	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{Timestamp: base, Close: 100, Volume: 80, TakerBuyVolume: 50, TakerSellVolume: 30},
		{Timestamp: base + unionStep, Close: 100, Volume: 80, TakerBuyVolume: 50, TakerSellVolume: 30},
	})

	// Same buckets, mislabelled as deltas, plus a fresher one so the batch takes the
	// replacement branch rather than the repair branch.
	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{Timestamp: base, Close: 100, Volume: 80, TakerBuyVolume: 50, TakerSellVolume: 30, Source: string(sourceWSDelta)},
		{Timestamp: base + unionStep, Close: 100, Volume: 80, TakerBuyVolume: 50, TakerSellVolume: 30, Source: string(sourceWSDelta)},
		{Timestamp: base + 2*unionStep, Close: 101, Volume: 5, Source: string(sourceWSDelta)},
	})

	got := candleAt(ce, exchange, market, symbol, tf, base)
	if got == nil {
		t.Fatal("bucket vanished")
	}
	if got.Volume != 80 {
		t.Errorf("Volume = %v, want 80 — the batch was treated as an increment and summed into a "+
			"bucket that already held the same total, inflating it", got.Volume)
	}
	if got.TakerBuyVolume != 50 || got.TakerSellVolume != 30 {
		t.Errorf("taker split = %v/%v, want 50/30 — order flow was inflated by the same double-count",
			got.TakerBuyVolume, got.TakerSellVolume)
	}
}

// TestSeedCandles_RepairIsNotCountedAsALateTrade keeps the late-update signal honest.
//
// LateTradeCount and candle_late_update_total exist for ONE purpose: to make the silent
// discard of genuinely late TRADES visible. A bulk seed merge is not a late trade — it is a
// repair — and folding thousands of them into the same counter would restore, in the
// opposite direction, exactly the blindness that counter was added to remove: an alert on
// late updates would fire on every boot and be tuned out.
func TestSeedCandles_RepairIsNotCountedAsALateTrade(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "ADA/USDT"
		tf       = "5m"
	)
	base := unionBase()
	ce := NewCandleEngine(map[string]int{"default": 16})

	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{Timestamp: base, Close: 100, Volume: 10},
		{Timestamp: base + unionStep, Close: 100, Volume: 10},
	})
	ce.SeedCandles(exchange, market, symbol, tf, []candle.Candle{
		{Timestamp: base, Close: 100, Volume: 12, Source: string(sourceRESTTotal)},
		{Timestamp: base + unionStep, Close: 100, Volume: 12, Source: string(sourceRESTTotal)},
		{Timestamp: base + 2*unionStep, Close: 101, Volume: 3, Source: string(sourceRESTTotal)},
	})

	got := candleAt(ce, exchange, market, symbol, tf, base)
	if got == nil {
		t.Fatal("bucket vanished")
	}
	if got.LateTradeCount != 0 {
		t.Errorf("LateTradeCount = %d, want 0 — a seed repair was recorded as a late trade, which "+
			"makes the late-update signal fire on every boot and stop meaning anything",
			got.LateTradeCount)
	}
}

// TestSeedCandles_WarmBootIntoEmptyEngineStaysVerbatim protects the contract the union must
// not break: on an empty key the batch lands EXACTLY as it came off redisstore. The seed is not
// allowed to recompute, normalise or "improve" a measurement the live path already made.
func TestSeedCandles_WarmBootIntoEmptyEngineStaysVerbatim(t *testing.T) {
	t.Parallel()

	const (
		exchange = "binance"
		market   = "future"
		symbol   = "XRP/USDT"
		tf       = "5m"
	)
	base := unionBase()
	ce := NewCandleEngine(map[string]int{"default": 16})

	warm := []candle.Candle{
		{Timestamp: base, Close: 100, Volume: 10, TakerBuyVolume: 7, TakerSellVolume: 3},
		// Fully buy-side: the zero must survive the warm path, not be reconstructed.
		{Timestamp: base + unionStep, Close: 101, Volume: 12, TakerBuyVolume: 12, TakerSellVolume: 0},
	}
	ce.SeedCandles(exchange, market, symbol, tf, warm)

	for i, want := range [][2]float64{{7, 3}, {12, 0}} {
		got := candleAt(ce, exchange, market, symbol, tf, base+int64(i)*unionStep)
		if got == nil {
			t.Fatalf("warm bucket %d vanished", i)
		}
		if got.TakerBuyVolume != want[0] || got.TakerSellVolume != want[1] {
			t.Errorf("bucket %d taker pair = %v/%v, want %v/%v — the warm path must stay verbatim",
				i, got.TakerBuyVolume, got.TakerSellVolume, want[0], want[1])
		}
	}
}
