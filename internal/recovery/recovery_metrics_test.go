package recovery

import (
	"strings"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

// TestEmptyCycleMetric pins REC-06/SD-03's empty-cycle half: a cycle whose fill returned no
// candles at all is counted with the exchange and internal market type, and a cycle that
// never attempted a fetch (materiality skip) is not.
func TestEmptyCycleMetric(t *testing.T) {
	w := newRecordingWriter()
	m := newTestManager(fakeFiller{}, w, staleProvider{ts: time.Now().Add(-time.Hour).UnixMilli()})
	m.retry.(*MemoryRetryScheduler).pending.Store(true)

	type sample struct{ exchange, marketType string }
	var got []sample
	m.SetMetrics(Metrics{EmptyCycleTotal: func(exchange, marketType string) {
		got = append(got, sample{exchange: exchange, marketType: marketType})
	}})

	m.process(t.Context(), 0, RecoveryJob{
		ExchangeID: "binance", MarketType: "swap", Symbol: "BTC/USDT",
		DisconnectTime: time.Now().Add(-time.Hour), ReconnectTime: time.Now(),
	})
	if len(got) != 1 || got[0].exchange != "binance" || got[0].marketType != "future" {
		t.Fatalf("empty-cycle metric = %+v, want one labelled binance/future sample", got)
	}

	// A fresh head with a zero-length silence is skipped by the materiality gate: no fetch,
	// so nothing was attempted and nothing is empty.
	m.SetTimestampProvider(staleProvider{ts: candle.BucketTimestamp(time.Now().UnixMilli(), "1m")})
	silent := time.Now()
	m.process(t.Context(), 0, RecoveryJob{
		ExchangeID: "binance", MarketType: "swap", Symbol: "ETH/USDT",
		DisconnectTime: silent, ReconnectTime: silent,
	})
	if len(got) != 1 {
		t.Fatalf("materiality skip counted as an empty cycle: %+v", got)
	}
}

// TestRejectedCandleMetric pins SD-03's rejection half: a candle that fails validation is
// dropped with its reason, never written, and is not confused with an empty fill.
func TestRejectedCandleMetric(t *testing.T) {
	type rejection struct{ exchange, marketType, timeframe, reason string }

	t.Run("historical candle", func(t *testing.T) {
		w := newRecordingWriter()
		// Open is non-positive: normalization cannot repair it, so the candle stays
		// impossible at the write boundary and is rejected.
		invalid := candle.Candle{
			Timestamp: candle.BucketTimestamp(time.Now().Add(-time.Hour).UnixMilli(), "1m"),
			Open:      0, High: 1, Low: 0.5, Close: 1, Volume: 1,
		}
		m := newTestManager(fakeFiller{historical: []candle.Candle{invalid}}, w,
			staleProvider{ts: time.Now().Add(-time.Hour).UnixMilli()})
		m.retry.(*MemoryRetryScheduler).pending.Store(true)
		var got []rejection
		var empty int
		m.SetMetrics(Metrics{
			RejectedCandles: func(exchange, marketType, timeframe, reason string) {
				got = append(got, rejection{exchange, marketType, timeframe, reason})
			},
			EmptyCycleTotal: func(string, string) { empty++ },
		})

		m.process(t.Context(), 0, RecoveryJob{
			ExchangeID: "binance", MarketType: "swap", Symbol: "BTC/USDT",
			DisconnectTime: time.Now().Add(-time.Hour), ReconnectTime: time.Now(),
		})

		if len(got) != 1 {
			t.Fatalf("rejected-candle metric = %+v, want one sample", got)
		}
		if got[0].exchange != "binance" || got[0].marketType != "future" || got[0].timeframe != "1m" {
			t.Fatalf("rejected-candle labels = %+v", got[0])
		}
		if !strings.Contains(got[0].reason, "open") {
			t.Fatalf("reason = %q, want the violated OHLC field", got[0].reason)
		}
		if w.seededBatch != 0 || len(w.seededCur) != 0 {
			t.Fatalf("rejected candle reached the store: historical=%d current=%d", w.seededBatch, len(w.seededCur))
		}
		if empty != 0 {
			t.Fatalf("a rejected candle counted as an empty fill: %d", empty)
		}
		if len(m.retry.(*MemoryRetryScheduler).queue) != 0 {
			t.Fatalf("rejected candle scheduled a retry: %+v", m.retry.(*MemoryRetryScheduler).queue)
		}
	})

	t.Run("current bucket candle", func(t *testing.T) {
		w := newRecordingWriter()
		historical := candle.Candle{
			Timestamp: candle.BucketTimestamp(time.Now().Add(-2*time.Minute).UnixMilli(), "1m"),
			Open:      1, High: 1, Low: 1, Close: 1, Volume: 1,
		}
		invalid := candle.Candle{
			Timestamp: candle.BucketTimestamp(time.Now().UnixMilli(), "1m"),
			Open:      0, High: 1, Low: 0.5, Close: 1, Volume: 1,
		}
		m := newTestManager(fakeFiller{historical: []candle.Candle{historical}, current: &invalid}, w,
			staleProvider{ts: time.Now().Add(-time.Hour).UnixMilli()})
		m.retry.(*MemoryRetryScheduler).pending.Store(true)
		var got []rejection
		m.SetMetrics(Metrics{RejectedCandles: func(exchange, marketType, timeframe, reason string) {
			got = append(got, rejection{exchange, marketType, timeframe, reason})
		}})

		m.process(t.Context(), 0, RecoveryJob{
			ExchangeID: "binance", MarketType: "swap", Symbol: "BTC/USDT",
			DisconnectTime: time.Now().Add(-time.Hour), ReconnectTime: time.Now(),
		})

		if len(got) != 1 || got[0].timeframe != "1m" || !strings.Contains(got[0].reason, "open") {
			t.Fatalf("current-candle rejection = %+v, want one labelled reason", got)
		}
		if w.seededBatch != 1 {
			t.Fatalf("valid historical candle not persisted alongside the rejection: %d", w.seededBatch)
		}
		if len(w.seededCur) != 0 {
			t.Fatalf("rejected current candle reached the store: %+v", w.seededCur)
		}
	})
}
