package httpapi

import (
	"context"
	"encoding/json"
	"math"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"
)

// The reading does not score a bar behind the warm tier's reference.
//
// MEASURED on the `1d` fleet of 100 symbols: zero lag on 93, nine on five, ten
// on one and THIRTEEN on one. The baseline had been recomposed five minutes earlier and the
// tail had not advanced — it is not cadence, it is the durable series' SOURCE that stopped.
// The correct datum sits in the warm ZSET inches away, and nothing compared that bar with the
// current one.
//
// The fixture writes both series through the PRODUCTION paths — `RecomputeBaseline` for the
// canonical ruler and `SaveSeriesOnly` for the warm ZSET — and the clock comes from the store
// itself. The warm tier enters only as an INSTANT: none of its values touches the ruler or the
// score.

const (
	dayMs  int64 = 24 * 60 * 60 * 1000
	weekMs int64 = 7 * dayMs
)

// integrityAxis are the fields of one axis this case observes.
//
// `n` and `nMin` are here because the SCORE refusal has to be judged AGAINST them: a seal that
// labels a ruler with `n` above the minimum as `immature` contradicts itself in the same
// payload.
type integrityAxis struct {
	Value  *float64 `json:"value"`
	Z      *float64 `json:"z"`
	State  string   `json:"state"`
	N      int      `json:"n"`
	NMin   int      `json:"nMin"`
	Reason string   `json:"reason"`
}

type integrityReading struct {
	Count int `json:"count"`
	Data  []struct {
		Timestamp int64 `json:"timestamp"`
		Snapshot  struct {
			Source string `json:"source"`
		} `json:"snapshot"`
	} `json:"data"`
	Normalized struct {
		Timestamp      int64         `json:"timestamp"`
		OpenInterest   integrityAxis `json:"zOpenInterest"`
		LongShortRatio integrityAxis `json:"zLongShortRatio"`
		Price          integrityAxis `json:"zPrice"`
	} `json:"normalized"`
}

// integrityAxes returns the three named axes, so the cases can walk them together.
func (r integrityReading) integrityAxes() []struct {
	nome string
	axis integrityAxis
} {
	return []struct {
		nome string
		axis integrityAxis
	}{
		{"zOpenInterest", r.Normalized.OpenInterest},
		{"zLongShortRatio", r.Normalized.LongShortRatio},
		{"zPrice", r.Normalized.Price},
	}
}

// coarseSeriesOf builds `count` canonical points ending at `head`, in the order the durable
// reader returns (newest first) and with real dispersion, so the ruler reaches `mature`.
func coarseSeriesOf(tf string, head int64, count int) []*derivatives.DerivativesSnapshot {
	step := candle.ParseTimeframeToSeconds(tf) * 1000
	out := make([]*derivatives.DerivativesSnapshot, 0, count)
	for i := 0; i < count; i++ {
		oi := 1000 * math.Exp(float64(i)*0.001+math.Sin(float64(i))*0.01)
		ls := 1.2 * math.Exp(math.Cos(float64(i))*0.02)
		out = append(out, &derivatives.DerivativesSnapshot{
			Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
			Timestamp: head - int64(i)*step, Source: derivatives.SourceCoinalyze,
			OpenInterestBase: &oi, LongShortRatio: &ls,
		})
	}
	return out
}

// warmSnapshot is a point of the WARM ZSET: the venue's, with the bucket label.
func warmSnapshot(ts int64, oi float64) *derivatives.DerivativesSnapshot {
	return &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT",
		Timestamp: ts, Source: derivatives.SourceVenue, OpenInterestBase: &oi,
	}
}

// newWarmFixture builds the two series in a miniredis, through the production paths.
//
// `closeEnd` is the instant of the last seeded candle; ZERO means "no candles", which is the
// case of a symbol whose price axis has no ruler yet.
func newWarmFixture(t *testing.T, tf string, coarse, warm []*derivatives.DerivativesSnapshot, closeEnd int64) *MarketDataHandler {
	t.Helper()
	ctx := context.Background()
	m := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: m.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	tfReg := candle.NewTimeframeRegistry([]string{tf})
	derivStore := redisstore.NewDerivativesRedisStore(client, config.DerivativesConfig{MaxSeriesSize: 400}, tfReg)

	if closeEnd > 0 {
		candleStore, err := redisstore.NewRedisStore(t.Context(), config.Config{RedisAddr: m.Addr()}, 400, tfReg)
		if err != nil {
			t.Fatalf("candle store: %v", err)
		}
		step := candle.ParseTimeframeToSeconds(tf) * 1000
		const n = 291
		moves := []float64{1.02, 1.0 / 1.02, 1.005, 1.0 / 1.005}
		level := 60_000.0
		start := closeEnd - int64(n-1)*step
		bars := make([]candle.Candle, 0, n)
		for i := 0; i < n; i++ {
			ts := start + int64(i)*step
			bars = append(bars, candle.Candle{Timestamp: ts, Open: level, High: level, Low: level, Close: level})
			level *= moves[i%len(moves)]
		}
		if err := candleStore.SeedCandlesBatch(ctx, "binance", "future", "BTCUSDT", tf, bars); err != nil {
			t.Fatalf("seed candles: %v", err)
		}
	}

	if len(coarse) > 0 {
		derivStore.SetCoarseSeriesReader(func(context.Context, string, string, string, string, int) ([]*derivatives.DerivativesSnapshot, error) {
			return coarse, nil
		})
		if _, err := derivStore.RecomputeBaseline(ctx, "binance", "future", "BTCUSDT", tf); err != nil {
			t.Fatalf("RecomputeBaseline: %v", err)
		}
	}
	for _, snap := range warm {
		if err := derivStore.SaveSeriesOnly(ctx, snap, tf); err != nil {
			t.Fatalf("SaveSeriesOnly %d: %v", snap.Timestamp, err)
		}
	}

	return newMarketDataHandlerWith(derivStore, "binance", "future", tfReg)
}

// warmReading reads the singular route with the canonical route's limit.
func warmReading(t *testing.T, h *MarketDataHandler, tf string) integrityReading {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/derivatives/BTCUSDT/series?timeframe="+tf+"&limit=3", nil)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTCUSDT")))
	rec := httptest.NewRecorder()
	h.GetDerivativesSeries(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var got integrityReading
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return got
}

// newestClosedWarmBar is the case's reference bar: the newest CLOSED one between the two the
// warm ZSET receives. It uses the producer's SAME closure predicate —
// `redisstore.BucketClosed` — instead of a second arithmetic in the test.
func newestClosedWarmBar(now, tfMs int64, candidates ...int64) int64 {
	newest := int64(0)
	for _, ts := range candidates {
		if redisstore.BucketClosed(ts, tfMs, now) && ts > newest {
			newest = ts
		}
	}
	return newest
}

func TestDerivativesReading_RefusesTheScoreBehindTheWarmReference(t *testing.T) {
	t.Run("the seal refuses all three scores and names both bars", func(t *testing.T) {
		now := time.Now().UnixMilli()
		today := candle.BucketTimestamp(now, "1d")
		reference := newestClosedWarmBar(now, dayMs, today, today-dayMs)
		head := today - 13*dayMs // the head thirteen bars back, like API3/USDT

		coarse := coarseSeriesOf("1d", head, 291)
		warm := []*derivatives.DerivativesSnapshot{warmSnapshot(today, 120), warmSnapshot(reference, 110)}
		h := newWarmFixture(t, "1d", coarse, warm, head)

		got := warmReading(t, h, "1d")

		if got.Normalized.Timestamp != head {
			t.Fatalf("scored bar = %d, want %d — the fixture does not reproduce the lag",
				got.Normalized.Timestamp, head)
		}
		if reference <= head {
			t.Fatalf("reference %d is not ahead of the head %d", reference, head)
		}
		for _, eixo := range got.integrityAxes() {
			if eixo.axis.Z != nil {
				t.Fatalf("%s: the score of a bar behind the reference was SERVED (z=%v) — the "+
					"lag census must be zero or the axis refused with a reason",
					eixo.nome, *eixo.axis.Z)
			}
			if !strings.Contains(eixo.axis.Reason, barInstant(head)) ||
				!strings.Contains(eixo.axis.Reason, barInstant(reference)) {
				t.Fatalf("%s: reason = %q, expected both bars (%s and %s) to be named",
					eixo.nome, eixo.axis.Reason, barInstant(head), barInstant(reference))
			}
			// THE SCORE REFUSAL DOES NOT RENAME THE RULER.
			//
			// `state` describes the RULER, and here it has hundreds of observations against
			// `nMin`. Forcing `immature` — which means "observations are missing" — produces a
			// payload that contradicts itself (`state=immature` with `n` above the minimum) and
			// a FALSE sentence on screen: `baselineSealOf` only converts `immature` to
			// `unavailable` when `n === 0`, so the sidebar prints "baseline immature
			// (n < n_min)" BESIDE the real `n`. Re-adding that line fails here, and this is the
			// only instrument that catches it.
			if eixo.axis.State == string(derivatives.BaselineImmature) && eixo.axis.N >= eixo.axis.NMin {
				t.Fatalf("%s: state=%q with n=%d against nMin=%d — the score refusal renamed the "+
					"ruler; `immature` means 'observations are missing' and the observations exist",
					eixo.nome, eixo.axis.State, eixo.axis.N, eixo.axis.NMin)
			}
		}

		// THE DATA KEEPS BEING SERVED: the seal refuses the SCORE, not the points.
		if got.Count != 3 {
			t.Fatalf("points served = %d, want 3 — the seal refused the data", got.Count)
		}
		for _, point := range got.Data {
			if point.Snapshot.Source != derivatives.SourceCoinalyze {
				t.Fatalf("point served with source %q, want %q", point.Snapshot.Source, derivatives.SourceCoinalyze)
			}
		}
	})

	t.Run("control: the head at the reference keeps all three scores", func(t *testing.T) {
		now := time.Now().UnixMilli()
		today := candle.BucketTimestamp(now, "1d")
		reference := newestClosedWarmBar(now, dayMs, today, today-dayMs)

		coarse := coarseSeriesOf("1d", reference, 291)
		warm := []*derivatives.DerivativesSnapshot{warmSnapshot(today, 120), warmSnapshot(reference, 110)}
		h := newWarmFixture(t, "1d", coarse, warm, reference)

		got := warmReading(t, h, "1d")

		if got.Normalized.Timestamp != reference {
			t.Fatalf("scored bar = %d, want %d", got.Normalized.Timestamp, reference)
		}
		for _, eixo := range got.integrityAxes() {
			if eixo.axis.Z == nil {
				t.Fatalf("%s: score refused on a reading that is AT the reference bar "+
					"(reason=%q) — the seal fired too early",
					eixo.nome, eixo.axis.Reason)
			}
			if strings.Contains(eixo.axis.Reason, "warm series") {
				t.Fatalf("%s: lag reason on a fresh reading: %q", eixo.nome, eixo.axis.Reason)
			}
		}
	})

	t.Run("control: immature 1w with a fresh bar preserves today's reason", func(t *testing.T) {
		now := time.Now().UnixMilli()
		week := candle.BucketTimestamp(now, "1w")
		reference := newestClosedWarmBar(now, weekMs, week, week-weekMs)

		// YOUNG ruler: six increments against nMin=30. It is a physical limitation, not a
		// defect — and the seal's rule is about the BAR, never about `n`.
		coarse := coarseSeriesOf("1w", reference, 8)
		warm := []*derivatives.DerivativesSnapshot{warmSnapshot(week, 120), warmSnapshot(reference, 110)}
		h := newWarmFixture(t, "1w", coarse, warm, 0)

		got := warmReading(t, h, "1w")

		if got.Normalized.Timestamp != reference {
			t.Fatalf("scored bar = %d, want %d", got.Normalized.Timestamp, reference)
		}
		const imaturo = "the baseline holds 6 of the 30 observations a score needs"
		for _, eixo := range got.integrityAxes() {
			if eixo.axis.Z != nil {
				t.Fatalf("%s: a ruler of 6 observations produced a score (z=%v)", eixo.nome, *eixo.axis.Z)
			}
			if strings.Contains(eixo.axis.Reason, "warm series") {
				t.Fatalf("%s: the lag became a NEW ALARM on a young symbol: %q", eixo.nome, eixo.axis.Reason)
			}
		}
		for _, eixo := range []struct {
			nome string
			axis integrityAxis
		}{{"zOpenInterest", got.Normalized.OpenInterest}, {"zLongShortRatio", got.Normalized.LongShortRatio}} {
			if !strings.Contains(eixo.axis.Reason, imaturo) {
				t.Fatalf("%s: reason = %q, want today's (%q)", eixo.nome, eixo.axis.Reason, imaturo)
			}
		}
	})

	t.Run("control: without a reference the seal is inert", func(t *testing.T) {
		now := time.Now().UnixMilli()
		today := candle.BucketTimestamp(now, "1d")
		head := today - 13*dayMs

		// EMPTY warm ZSET: the clock has nothing to answer.
		h := newWarmFixture(t, "1d", coarseSeriesOf("1d", head, 291), nil, head)

		got := warmReading(t, h, "1d")

		if got.Normalized.Timestamp != head {
			t.Fatalf("scored bar = %d, want %d", got.Normalized.Timestamp, head)
		}
		for _, eixo := range got.integrityAxes() {
			if eixo.axis.Z == nil {
				t.Fatalf("%s: without a clock the gate must be INERT, and the score was refused "+
					"(reason=%q)", eixo.nome, eixo.axis.Reason)
			}
		}
	})

	t.Run("control: the store without the clock is also inert", func(t *testing.T) {
		now := time.Now().UnixMilli()
		today := candle.BucketTimestamp(now, "1d")
		head := today - 13*dayMs
		price := 0.01
		points := oiSeriesDesc(head, dayMs, 110, 100, 90)
		store := &canonicalRouteStore{
			mockDerivativesStore: mockDerivativesStore{priceIncrementValue: &price},
			baseline: &derivatives.DerivativesBaseline{
				Source: derivatives.SourceCoinalyze, SeriesTail: points, WindowN: 288, NMin: 30,
				OpenInterest: &derivatives.Baseline{Median: 0, MAD: 0.01, N: 288},
			},
		}
		h := newMarketDataHandlerWith(store, "binance", "future", candle.NewTimeframeRegistry(nil))
		h.nowMs = func() int64 { return now }

		got := warmReading(t, h, "1d")

		if got.Normalized.Timestamp != head {
			t.Fatalf("scored bar = %d, want %d", got.Normalized.Timestamp, head)
		}
		if got.Normalized.OpenInterest.Z == nil {
			t.Fatalf("a store without the clock must not refuse: reason=%q", got.Normalized.OpenInterest.Reason)
		}
	})

	t.Run("the residual-lag limitation is declared", func(t *testing.T) {
		now := time.Now().UnixMilli()
		today := candle.BucketTimestamp(now, "1d")
		head := today - 13*dayMs
		h := newWarmFixture(t, "1d", coarseSeriesOf("1d", head, 291), nil, head)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/derivatives/BTCUSDT/series?timeframe=1d&limit=3", nil)
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, attachSymbolParam("BTCUSDT")))
		rec := httptest.NewRecorder()
		h.GetDerivativesSeries(rec, req)

		var got struct {
			Measurement struct {
				Limitations []string `json:"limitations"`
			} `json:"measurement"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		// THE ANCHOR IS THE STABLE PART. The declaration asserts the residual lag of ONE bar,
		// which is the case that in fact escapes (there being no clock). What it discriminates
		// is the limitation having been REMOVED — the mutation that deletes the `append` fails
		// here.
		achou := false
		for _, limitacao := range got.Measurement.Limitations {
			if strings.Contains(limitacao, "one bar behind the cold series") {
				achou = true
			}
		}
		if !achou {
			t.Fatalf("the residual lag was not declared: %v", got.Measurement.Limitations)
		}
	})
}
