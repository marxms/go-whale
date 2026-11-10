package httpapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
)

// the read that crosses the ZSET cap, measured at the HANDLER.
//
// The whole deep-window feature was unreachable from `main`: the window reader existed, had unit tests
// and was never constructed, so the route kept serving only the ZSET cap. What these cases
// assert is the WIRING — that the route reaches the durable tier, with the scope the durable
// query needs, and that a failure there does not erase the recent part.

// windowHot is the hot side of the join. It does NOT declare a horizon: the warm coverage is
// measured on what it returns, and it was precisely confusing capacity with coverage that
// opened the 601-day hole.
type windowHot struct {
	series []*derivatives.DerivativesSnapshot
	calls  int
}

func (h *windowHot) GetSnapshotSeries(_ context.Context, _, _, _, _ string, _ int) ([]*derivatives.DerivativesSnapshot, error) {
	h.calls++
	return h.series, nil
}

// windowCold records the scope it was queried with — it is what proves the provenance term.
type windowCold struct {
	scopes []derivatives.DerivativesScope
	froms  []time.Time
	tos    []time.Time
	series []*derivatives.DerivativesSnapshot
	err    error
}

func (c *windowCold) QueryDerivativesRange(_ context.Context, scope derivatives.DerivativesScope, from, to time.Time) ([]*derivatives.DerivativesSnapshot, error) {
	c.scopes = append(c.scopes, scope)
	c.froms = append(c.froms, from)
	c.tos = append(c.tos, to)
	if c.err != nil {
		return nil, c.err
	}
	return c.series, nil
}

func derivWindowPoint(ts int64, source string, oi float64) *derivatives.DerivativesSnapshot {
	v := oi
	return &derivatives.DerivativesSnapshot{
		Exchange: "binance", MarketType: "future", Symbol: "BTC/USDT",
		Timestamp: ts, Source: source, OpenInterestBase: &v,
	}
}

// dailySeries builds `n` ascending daily buckets ending at `end`.
func dailySeries(end time.Time, n int, source string) []*derivatives.DerivativesSnapshot {
	out := make([]*derivatives.DerivativesSnapshot, 0, n)
	for i := n - 1; i >= 0; i-- {
		out = append(out, derivWindowPoint(end.Add(-time.Duration(i)*24*time.Hour).UnixMilli(), source, 1000+float64(i)))
	}
	return out
}

// handlerWithWindow builds the handler wired to a window reader over the two doubles.
func handlerWithWindow(hot *windowHot, cold *windowCold, zset []*derivatives.DerivativesSnapshot) *MarketDataHandler {
	// The `reader` is the DEGRADATION path: it is what the read falls back to when the
	// durable tier fails, and a nil here would turn the fallback assertion into a panic.
	mds := &mockDerivativesStore{series: zset}
	h := &MarketDataHandler{reader: mds, baselines: mds, prices: mds}
	return h.WithDerivativesWindow(derivatives.NewDerivativesWindowReader(hot, cold))
}

// TestDurableRead_ScopeCarriesTheSource is the missing wiring, at the exact point where it
// fails silently: `QueryDerivativesRange` filters by `source`, and a scope with an empty
// source matches ZERO rows and returns success. The deep window would come back empty, with
// 200 on the response and no log — silent truncation again, now dressed as the durable tier.
func TestDurableRead_ScopeCarriesTheSource(t *testing.T) {
	now := time.Now().UTC()
	hot := &windowHot{}
	cold := &windowCold{series: []*derivatives.DerivativesSnapshot{
		derivWindowPoint(now.Add(-300*24*time.Hour).UnixMilli(), derivatives.SourceCoinalyze, 1000),
	}}

	// 365 daily buckets = far past the ZSET's 5-day horizon.
	if _, err := handlerWithWindow(hot, cold, hot.series).derivativesSeries(
		context.Background(), "binance", "future", "BTC/USDT", "1d", 365,
	); err != nil {
		t.Fatalf("read returned an error: %v", err)
	}

	if len(cold.scopes) != 1 {
		t.Fatalf("the durable tier was queried %d time(s), expected 1", len(cold.scopes))
	}
	if got := cold.scopes[0].Source; got != derivatives.SourceCoinalyze {
		t.Errorf("durable scope with source %q, expected %q — an empty source matches zero rows",
			got, derivatives.SourceCoinalyze)
	}
	// The other terms are filters too; an empty one zeroes the result the same way.
	if s := cold.scopes[0]; s.Exchange != "binance" || s.MarketType != "future" ||
		s.Symbol != "BTC/USDT" || s.Timeframe != "1d" {
		t.Errorf("incomplete durable scope: %+v", s)
	}
}

// TestDurableRead_DeepWindowReachesTheColdTier asserts the RESULT, not the call: the point
// from 300 days ago reaches the handler's output. It is the requirement's independent test
// reduced to what the handler controls.
func TestDurableRead_DeepWindowReachesTheColdTier(t *testing.T) {
	now := time.Now().UTC()
	deepTs := now.Add(-300 * 24 * time.Hour).UnixMilli()
	hot := &windowHot{series: dailySeries(now, 5, "binance")}
	cold := &windowCold{series: []*derivatives.DerivativesSnapshot{
		derivWindowPoint(deepTs, derivatives.SourceCoinalyze, 1000),
	}}

	out, err := handlerWithWindow(hot, cold, hot.series).derivativesSeries(
		context.Background(), "binance", "future", "BTC/USDT", "1d", 365,
	)
	if err != nil {
		t.Fatalf("read returned an error: %v", err)
	}

	var sawDeep bool
	for _, p := range out {
		if p.Timestamp == deepTs {
			sawDeep = true
			if p.Source != derivatives.SourceCoinalyze {
				t.Errorf("deep point with source %q, expected %q", p.Source, derivatives.SourceCoinalyze)
			}
		}
	}
	if !sawDeep {
		t.Fatalf("the point from 300 days ago did not reach the output (%d points served) — the window stopped at the ZSET cap", len(out))
	}
}

// TestDurableRead_ShallowWindowSkipsTheColdTier is the other half of the decision: a window
// that fits in the warm horizon cannot pay the price of opening partitions.
func TestDurableRead_ShallowWindowSkipsTheColdTier(t *testing.T) {
	now := time.Now().UTC()
	// THE WARM TIER REALLY COVERS the 7 requested days. A double that declares a 200-day
	// horizon and delivers ONE point makes the read believe the declaration; that is the
	// defect.
	hot := &windowHot{series: dailySeries(now, 10, "binance")}
	cold := &windowCold{}

	if _, err := handlerWithWindow(hot, cold, hot.series).derivativesSeries(
		context.Background(), "binance", "future", "BTC/USDT", "1d", 7,
	); err != nil {
		t.Fatalf("read returned an error: %v", err)
	}
	if len(cold.scopes) != 0 {
		t.Errorf("the durable tier was queried %d time(s) for a 7-day window under a 200-day horizon", len(cold.scopes))
	}
}

// TestDurableRead_ColdFailureFallsBackToHot asserts the declared degradation: a durable
// query that does not return is a reason to serve LESS history, never to serve nothing.
func TestDurableRead_ColdFailureFallsBackToHot(t *testing.T) {
	now := time.Now().UTC()
	hot := &windowHot{series: dailySeries(now, 5, "binance")}
	cold := &windowCold{err: errors.New("questdb fora do ar")}

	// The ZSET receives a MARKED point. Counting calls would not distinguish degrading from
	// failing silently; what proves the degradation is the ZSET point appearing in the output.
	markerTs := now.Add(-2 * 24 * time.Hour).UnixMilli()
	zset := []*derivatives.DerivativesSnapshot{derivWindowPoint(markerTs, "binance", 4242)}

	out, err := handlerWithWindow(hot, cold, zset).derivativesSeries(
		context.Background(), "binance", "future", "BTC/USDT", "1d", 365,
	)
	if err != nil {
		t.Fatalf("the durable failure propagated an error instead of degrading: %v", err)
	}
	if len(out) != 1 || out[0].Timestamp != markerTs {
		t.Fatalf("the durable failure did not fall back to the ZSET: %d point(s) served, expected the marker at %d",
			len(out), markerTs)
	}
}

// TestDurableRead_NilWindowServesTheZsetAlone: without QuestDB, the route keeps answering.
//
// Checking only a zero field value — "the handler starts with a nil derivWindow" — asserts a
// property of Go, not of the product. What must be asserted is the DEGRADATION: without a
// durable tier the read serves the ZSET, with its cap, instead of failing.
func TestDurableRead_NilWindowServesTheZsetAlone(t *testing.T) {
	now := time.Now().UTC()
	zset := dailySeries(now, 30, "venue")
	mds := &mockDerivativesStore{series: zset}
	h := &MarketDataHandler{reader: mds, baselines: mds, prices: mds}

	out, err := h.derivativesSeries(context.Background(), "binance", "future", "BTC/USDT", "1d", 365)
	if err != nil {
		t.Fatalf("without a durable tier the read failed: %v", err)
	}
	if len(out) != len(zset) {
		t.Fatalf("served %d point(s), expected the ZSET's %d", len(out), len(zset))
	}
	// AND IT DOES NOT INVENT DEPTH: asking for 365 buckets with 30 in the ZSET returns 30,
	// not 365.
	if len(out) == 365 {
		t.Error("the degradation invented coverage")
	}
}

// THE DEFECT MEASURED IN PRODUCTION, PINNED HERE.
//
// LAN: the declared cap of `1d` was 800 and the ZSET had 200 members. The read
// concluded that the warm tier covered the whole range, asked the durable tier nothing in it,
// and served the series with a 601-DAY HOLE in the middle — no error, no log,
// with `count` matching and both ends continuous. Nothing in the response denounced it.
//
// Capacity is an UPPER BOUND of coverage. Using it as if it were the coverage is always
// wrong while the ZSET has not filled up, and a freshly created ZSET has never filled up.
func TestDurableRead_SparseHotTierStillConsultsTheColdTier(t *testing.T) {
	now := time.Now().UTC()
	// The warm tier delivers only the 200 days it HAS, even though the cap declares 800.
	hot := &windowHot{series: dailySeries(now, 200, "binance")}
	deepTs := now.Add(-600 * 24 * time.Hour).UnixMilli()
	cold := &windowCold{series: []*derivatives.DerivativesSnapshot{
		derivWindowPoint(deepTs, derivatives.SourceCoinalyze, 1000),
	}}

	out, err := handlerWithWindow(hot, cold, hot.series).derivativesSeries(
		context.Background(), "binance", "future", "BTC/USDT", "1d", 800,
	)
	if err != nil {
		t.Fatalf("read returned an error: %v", err)
	}
	if len(cold.scopes) != 1 {
		t.Fatalf("the durable tier was queried %d time(s); with the warm tier covering 200 of the 800 requested, it had to be 1", len(cold.scopes))
	}
	// AND THE COLD WINDOW COVERS THE WHOLE REQUEST. Stopping at the oldest warm point leaves
	// the durable tier unasked precisely for the buckets where the warm tier HAS the row and
	// does not have the number — the 156 days of null open interest measured on the LAN.
	oldestHot := time.UnixMilli(hot.series[0].Timestamp).UTC()
	if got := cold.tos[0]; got.Before(oldestHot) {
		t.Errorf("the durable tier was queried back to %s, before the oldest warm point (%s) — the middle buckets are left with no fill source",
			got.Format("2006-01-02"), oldestHot.Format("2006-01-02"))
	}
	var sawDeep bool
	for _, p := range out {
		if p.Timestamp == deepTs {
			sawDeep = true
		}
	}
	if !sawDeep {
		t.Errorf("the point from 600 days ago did not reach the output")
	}
}

// THE DURABLE READ'S DEADLINE — the difference between serving less and serving nothing.
//
// MEASURED on the LAN: the first deep read after a restart took 24.4 s
// (opening the daily partitions of 5.5 years) and the client received ZERO bytes, with the
// connection dropped. The server's `WriteTimeout` is 15 s and counts from the start of the
// request, so it expired in the middle of writing the body. The following calls took 0.13 s:
// it is warm-up, not load. But "once per restart" is when someone opens the chart after a
// deploy.
func TestDurableRead_ASlowDurableTierDegradesInsteadOfHanging(t *testing.T) {
	now := time.Now().UTC()
	hot := &windowHot{series: dailySeries(now, 5, "binance")}
	// The durable tier blocks until the context is canceled — the shape of a query that
	// opens too many partitions.
	cold := &windowColdBlocking{}

	markerTs := now.Add(-2 * 24 * time.Hour).UnixMilli()
	zset := []*derivatives.DerivativesSnapshot{derivWindowPoint(markerTs, "binance", 4242)}
	mds := &mockDerivativesStore{series: zset}
	h := (&MarketDataHandler{
		reader:        mds,
		baselines:     mds,
		prices:        mds,
		durableBudget: 40 * time.Millisecond,
	}).WithDerivativesWindow(derivatives.NewDerivativesWindowReader(hot, cold))

	inicio := time.Now()
	out, err := h.derivativesSeries(context.Background(), "binance", "future", "BTC/USDT", "1d", 365)
	decorrido := time.Since(inicio)

	if err != nil {
		t.Fatalf("the slow read propagated an error instead of degrading: %v", err)
	}
	if len(out) != 1 || out[0].Timestamp != markerTs {
		t.Fatalf("the degradation did not serve the ZSET: %d point(s)", len(out))
	}
	// AND IT GAVE UP WITHIN THE BUDGET, with slack to write the response.
	if decorrido >= 2*time.Second {
		t.Errorf("the read took %s; it had to give up at the budget (40ms)", decorrido)
	}
	if !cold.observouCancelamento {
		t.Error("the durable context carried no deadline — nothing would interrupt it")
	}
}

// AND THE DEFAULT BUDGET FITS BEFORE THE SERVER'S WRITE DEADLINE, with slack to serialize
// the degradation response. A budget larger than the `WriteTimeout` would reproduce exactly
// the measured defect: 200 sent, body cut, client with nothing.
func TestDurableRead_TheDefaultBudgetFitsInsideTheServerWriteTimeout(t *testing.T) {
	// THE REAL VALUE, not a copy. Comparing the budget with a hand-written
	// `15 * time.Second` guards the copy, not the server: lowering the real `WriteTimeout`
	// reproduces the `i/o timeout` measured on the LAN with nothing complaining.
	if derivativesDurableReadBudget >= ServerWriteTimeout {
		t.Fatalf("durable budget %s does not fit inside WriteTimeout %s", derivativesDurableReadBudget, ServerWriteTimeout)
	}
	if folga := ServerWriteTimeout - derivativesDurableReadBudget; folga < 3*time.Second {
		t.Errorf("only %s of slack to serialize and write the degradation; that is too little", folga)
	}
}

// windowColdBlocking blocks until the context is canceled.
type windowColdBlocking struct{ observouCancelamento bool }

func (c *windowColdBlocking) QueryDerivativesRange(ctx context.Context, _ derivatives.DerivativesScope, _, _ time.Time) ([]*derivatives.DerivativesSnapshot, error) {
	<-ctx.Done()
	c.observouCancelamento = true
	return nil, ctx.Err()
}
