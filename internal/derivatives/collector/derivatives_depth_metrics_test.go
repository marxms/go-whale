package collector

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
)

// ─────────────────────────────────────────────────────────────────────────────
// the depth gauges, held in place.
//
// `collected_points` and `collected_span_seconds` are the only observability the
// change has for "how deep did the bootstrap actually get", and recordCollectedDepth
// carries a decision nothing verified: it does NOT publish a span for fewer than two
// points, because one lonely reading spans no time and a published 0 would be
// indistinguishable from many readings all stamped at the same instant.
//
// These tests do not run in parallel, and that is deliberate: each case gets its own
// in-memory recorder, so isolation comes from the recorder rather than from process-wide
// collector state.
// ─────────────────────────────────────────────────────────────────────────────

// droppingStore refuses one write in every `refuseEvery`, so a walk ends with more
// points fetched than persisted — the gap a Redis outage opens, and the gap that tells
// "count of rows the provider sent" apart from "depth the series actually reached".
type droppingStore struct {
	recordingStore
	refuseEvery int
	attempts    int
}

func (s *droppingStore) SaveSeriesOnly(ctx context.Context, snapshot *derivatives.DerivativesSnapshot, timeframe string) error {
	s.attempts++
	if s.refuseEvery > 0 && s.attempts%s.refuseEvery == 0 {
		return errRefusedWrite
	}
	return s.recordingStore.SaveSeriesOnly(ctx, snapshot, timeframe)
}

var errRefusedWrite = errors.New("store refused the write")

func TestRecordCollectedDepth(t *testing.T) {
	const minuteMs = 60 * 1000

	tests := []struct {
		name string
		// The (oldest, newest) pair as the collectors see it: milliseconds.
		points        int
		oldestMs      int64
		newestMs      int64
		wantPoints    float64
		wantSpanSecs  float64
		wantSpanGauge bool
	}{
		{
			name:          "a full walk publishes both the count and the span it covers",
			points:        288,
			oldestMs:      1_754_000_000_000,
			newestMs:      1_754_000_000_000 + 287*5*minuteMs,
			wantPoints:    288,
			wantSpanSecs:  287 * 5 * 60,
			wantSpanGauge: true,
		},
		{
			name:          "two points are enough for a span",
			points:        2,
			oldestMs:      1_754_000_000_000,
			newestMs:      1_754_000_000_000 + 5*minuteMs,
			wantPoints:    2,
			wantSpanSecs:  300,
			wantSpanGauge: true,
		},
		{
			// The decision this test exists for. A single reading spans no time; a
			// published 0 would read as "many readings, all at the same instant".
			name:          "a single reading publishes its count and NO span",
			points:        1,
			oldestMs:      1_754_000_000_000,
			newestMs:      1_754_000_000_000,
			wantPoints:    1,
			wantSpanGauge: false,
		},
		{
			// The count is the authority, not the endpoints. A caller that reports one
			// point while handing over two distinct instants is contradicting itself,
			// and the span is the half that cannot be true.
			name:          "one point with two endpoints still publishes no span",
			points:        1,
			oldestMs:      1_754_000_000_000,
			newestMs:      1_754_000_000_000 + 5*minuteMs,
			wantPoints:    1,
			wantSpanGauge: false,
		},
		{
			// Zero collected points is a real, publishable measurement — the count is
			// the answer to "how deep did we get", and the answer is "not at all".
			name:          "an empty collection publishes zero points and no span",
			points:        0,
			wantPoints:    0,
			wantSpanGauge: false,
		},
		{
			// A walk that never learned its own endpoints must not invent a span out
			// of the zero value; the count still stands on its own.
			name:          "points without timestamps publish no span",
			points:        50,
			oldestMs:      0,
			newestMs:      0,
			wantPoints:    50,
			wantSpanGauge: false,
		},
		{
			// Endpoints in the wrong order is a bug upstream, not a negative span.
			name:          "an inverted pair of endpoints publishes no span",
			points:        10,
			oldestMs:      1_754_000_000_000 + 5*minuteMs,
			newestMs:      1_754_000_000_000,
			wantPoints:    10,
			wantSpanGauge: false,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := newCollectorTestMetrics()

			// A distinct symbol per case, so a leftover child from another case could
			// never be mistaken for this case's answer.
			symbol := fmt.Sprintf("DEPTH%d/USDT", i)
			recordCollectedDepth(rec.port(), "binance", feedOpenInterest, symbol, "5m", tt.points, tt.oldestMs, tt.newestMs)

			got, ok := rec.points[ckey("binance", string(feedOpenInterest), symbol, "5m")]
			if !ok {
				t.Fatalf("collected_points was not published at all for %s", symbol)
			}
			if got != tt.wantPoints {
				t.Errorf("collected_points = %v, want %v", got, tt.wantPoints)
			}

			span, spanOK := rec.spans[ckey("binance", string(feedOpenInterest), symbol, "5m")]
			if spanOK != tt.wantSpanGauge {
				t.Fatalf("collected_span_seconds published = %v (value %v), want published = %v",
					spanOK, span, tt.wantSpanGauge)
			}
			if tt.wantSpanGauge && span != tt.wantSpanSecs {
				t.Errorf("collected_span_seconds = %v, want %v — the span is newest minus oldest, in seconds",
					span, tt.wantSpanSecs)
			}
		})
	}
}

// TestCollectPaginatedHistoryPublishesTheDepthItPersisted is the other half of 0.2:
// the gauge has to report what reached the STORE, not what the provider handed over.
// A page that was fetched and then dropped on the way to Redis leaves a hole that is
// invisible from the endpoints of the series, and a points gauge counting fetched
// rows would report a plausible number over that hole.
func TestCollectPaginatedHistoryPublishesTheDepthItPersisted(t *testing.T) {
	const timeframe = "5m"
	periodMs, _ := candle.ParseTimeframeMs(timeframe)

	rec := newCollectorTestMetrics()

	// Every third write is refused, so the number FETCHED and the number PERSISTED
	// differ by construction. Without that gap the assertion below would pass on
	// either definition and prove nothing.
	store := &droppingStore{refuseEvery: 3}
	scope := SymbolScope{ExchangeID: "binance", MarketType: "future", Symbol: "DEPTHWALK/USDT"}

	saved := collectPaginatedHistory(
		context.Background(), rec.port(), store, scope, feedOpenInterest, timeframe,
		600, fixedNowMs,
		func(page historyPage) ([]int64, error) {
			return providerPage(page, periodMs, historyPageLimit), nil
		},
		func(ts int64) int64 { return ts },
		func(ts int64) *derivatives.DerivativesSnapshot {
			contracts := 1234.5
			return &derivatives.DerivativesSnapshot{
				Exchange:         scope.ExchangeID,
				MarketType:       scope.MarketType,
				Symbol:           scope.Symbol,
				Timestamp:        ts,
				OpenInterestBase: &contracts,
			}
		},
	)

	if saved == 0 || saved != store.count() {
		t.Fatalf("saved = %d, store holds %d — the fixture is not exercising the walk", saved, store.count())
	}
	if store.attempts <= saved {
		t.Fatalf("attempts = %d, saved = %d — no write was refused, so this test cannot tell "+
			"a fetched-row count from a persisted-point count", store.attempts, saved)
	}

	points, ok := rec.points[ckey("binance", string(feedOpenInterest), scope.Symbol, timeframe)]
	if !ok {
		t.Fatal("the walk persisted points and published no depth gauge")
	}
	if int(points) != store.count() {
		t.Errorf("collected_points = %v, store holds %d — the gauge is not counting what was persisted",
			points, store.count())
	}

	// The span the walk actually reached, computed from the members the store holds,
	// so the assertion does not restate the implementation's own arithmetic.
	var oldest, newest int64
	for _, snapshot := range store.saved {
		if oldest == 0 || snapshot.Timestamp < oldest {
			oldest = snapshot.Timestamp
		}
		if snapshot.Timestamp > newest {
			newest = snapshot.Timestamp
		}
	}

	span, ok := rec.spans[ckey("binance", string(feedOpenInterest), scope.Symbol, timeframe)]
	if !ok {
		t.Fatal("a multi-point walk published no span")
	}
	if want := float64(newest-oldest) / 1000; span != want {
		t.Errorf("collected_span_seconds = %v, want %v — newest minus oldest of the persisted members", span, want)
	}
}
