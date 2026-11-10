package collector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/coinalyze"
	"github.com/marxms/go-whale/internal/derivatives"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// The responses below are the ones MEASURED against the real API, for the same daily bar
// (t=1787270400 = 2026-08-21T00:00:00Z) of Binance's BTCUSDT perpetual. The existing
// source's values on the same bar, also measured, are:
//
//	/fapi/v1/openInterest            openInterest    107025.033   (base asset)
//	/fapi/v1/fundingRate             fundingRate     0.00010000   (fraction)
//	futures/data/globalLongShort…    longAccount     0.4886       (fraction; written x100)
//	futures/data/globalLongShort…    longShortRatio  0.9554
//
// The unit assertions in this file compare against those numbers.
const (
	coinalyzeFixtureTsSec = int64(1787270400)
	coinalyzeFixtureTsMs  = coinalyzeFixtureTsSec * 1000

	coinalyzeMarketsBody = `[
		{"symbol":"BTCUSDT_PERP.A","exchange":"A","symbol_on_exchange":"BTCUSDT","oi_lq_vol_denominated_in":"BASE_ASSET"},
		{"symbol":"BTCUSD_PERP.A","exchange":"A","symbol_on_exchange":"BTCUSD_PERP","oi_lq_vol_denominated_in":"QUOTE_ASSET"}
	]`

	coinalyzeOpenInterestBody = `[{"symbol":"BTCUSDT_PERP.A","history":[
		{"t":1787270400,"o":107640.019,"h":111039.103,"l":105678.351,"c":106117.954},
		{"t":1787356800,"o":106114.438,"h":106721.101,"l":105303.538,"c":105834.173}
	]}]`

	coinalyzeFundingBody = `[{"symbol":"BTCUSDT_PERP.A","history":[
		{"t":1787270400,"o":0.004964,"h":0.01,"l":0.004964,"c":0.01}
	]}]`

	coinalyzeLongShortBody = `[{"symbol":"BTCUSDT_PERP.A","history":[
		{"t":1787270400,"r":0.9554,"l":48.86,"s":51.14}
	]}]`

	// A pair that does NOT close at 100: the two halves came from different instants.
	coinalyzeLongShortBrokenBody = `[{"symbol":"BTCUSDT_PERP.A","history":[
		{"t":1787270400,"r":0.9554,"l":48.86,"s":90.0}
	]}]`
)

// --- doubles ------------------------------------------------------------------

type recordedWrite struct {
	snapshot  *derivatives.DerivativesSnapshot
	timeframe string
	label     derivatives.LabelConvention
}

type recordingCoinalyzeStore struct {
	mu      sync.Mutex
	writes  []recordedWrite
	batches []derivatives.DerivativesBatch
	err     error
}

// WriteBatch receives the BATCH the cycle produced and unfolds it into writes, so that the
// existing cases — which assert point by point — keep asserting the same thing.
//
// The double speaks in batches because the cycle does: calling the series write directly
// leaves the two-tier writer with no caller, so the durable table never receives a row. The
// double follows the real object, and no assertion was loosened.
func (s *recordingCoinalyzeStore) WriteBatch(_ context.Context, batch derivatives.DerivativesBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, snapshot := range batch.Snapshots {
		s.writes = append(s.writes, recordedWrite{
			snapshot: snapshot, timeframe: batch.Timeframe, label: batch.Label,
		})
		s.batches = append(s.batches, batch)
	}
	return s.err
}

func (s *recordingCoinalyzeStore) all() []recordedWrite {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedWrite(nil), s.writes...)
}

// recordingSpender records FOR HOW MANY symbols each call was charged and executes it.
type recordingSpender struct {
	mu      sync.Mutex
	charges [][]string
}

func (b *recordingSpender) Do(ctx context.Context, symbols []string, call func(context.Context) error) error {
	b.mu.Lock()
	b.charges = append(b.charges, append([]string(nil), symbols...))
	b.mu.Unlock()
	return call(ctx)
}

func (b *recordingSpender) all() [][]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([][]string(nil), b.charges...)
}

// coinalyzeFake serves the four paths and stores every query received.
type coinalyzeFake struct {
	mu       sync.Mutex
	requests []*url.URL
	bodies   map[string]string
}

func newCoinalyzeFake(t *testing.T, overrides map[string]string) (*coinalyzeFake, *coinalyze.CoinalyzeClient) {
	t.Helper()
	fake := &coinalyzeFake{bodies: map[string]string{
		"/future-markets":           coinalyzeMarketsBody,
		"/open-interest-history":    coinalyzeOpenInterestBody,
		"/funding-rate-history":     coinalyzeFundingBody,
		"/long-short-ratio-history": coinalyzeLongShortBody,
	}}
	for path, body := range overrides {
		fake.bodies[path] = body
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		captured := *r.URL
		fake.requests = append(fake.requests, &captured)
		body, ok := fake.bodies[r.URL.Path]
		fake.mu.Unlock()

		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	return fake, coinalyze.NewCoinalyzeClient("chave-de-teste", coinalyze.WithCoinalyzeBaseURL(server.URL))
}

func (f *coinalyzeFake) queriesFor(path string) []url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []url.Values
	for _, u := range f.requests {
		if u.Path == path {
			out = append(out, u.Query())
		}
	}
	return out
}

func newCoinalyzeHarness(t *testing.T, timeframes []string, overrides map[string]string) (
	*CoinalyzePoller, *coinalyzeFake, *recordingCoinalyzeStore, *recordingSpender, *logger.Sink,
) {
	t.Helper()
	return newCoinalyzeHarnessWithProfiles(t, timeframes, overrides, nil)
}

// newCoinalyzeHarnessWithProfiles assembles the same cycle with a declared settlement
// interval registry.
//
// The registry is a PARAMETER and not a default because it is the evidence that decides
// whether the 8h equivalent comes out at all: a harness that always injected a measured
// profile would make the "no profile" case pass by construction, and that case is precisely
// what keeps a unitless number from becoming comparable.
func newCoinalyzeHarnessWithProfiles(
	t *testing.T, timeframes []string, overrides map[string]string, profiles *FundingProfileRegistry,
) (*CoinalyzePoller, *coinalyzeFake, *recordingCoinalyzeStore, *recordingSpender, *logger.Sink) {
	t.Helper()

	log, sink := logger.NewCapture(logger.LevelDebug)
	fake, client := newCoinalyzeFake(t, overrides)
	store := &recordingCoinalyzeStore{}
	spender := &recordingSpender{}

	catalog := coinalyze.NewCoinalyzeSymbolCatalog(coinalyze.CoinalyzeSymbolCatalogOptions{Client: client, Log: log})
	poller := NewCoinalyzePoller(CoinalyzePollerOptions{
		Client:          client,
		Catalog:         catalog,
		Budget:          spender,
		Store:           store,
		Exchange:        "binance",
		MarketType:      "future",
		Symbols:         []string{"BTC/USDT"},
		Timeframes:      timeframes,
		Lookback:        72 * time.Hour,
		FundingProfiles: profiles,
		Now:             func() time.Time { return time.Unix(1787465684, 0).UTC() },
		Log:             log,
	})
	// THE DOUBLE FOLLOWS THE OBJECT, and no assertion in this file changes.
	//
	// The cycle paces batches; the cases below measure UNIT, WINDOW and SEEDING, and one of
	// them (two window groups in the same cycle) admits two batches. With the production
	// source — a `time.Ticker` at the derived pace — they would wait minutes for a tick that
	// is not what they measure. The pace is measured by `coinalyze_pacing_test.go`, which
	// replaces this source with its own.
	poller.newPacer = unpacedTicks
	return poller, fake, store, spender, sink
}

// unpacedTicks is the admission source for cases that do NOT measure the pace: a closed
// channel, which releases every batch immediately.
func unpacedTicks(time.Duration) (<-chan time.Time, func()) {
	ch := make(chan time.Time)
	close(ch)
	return ch, func() {}
}

func firstWriteAt(t *testing.T, store *recordingCoinalyzeStore, tsMs int64) *derivatives.DerivativesSnapshot {
	t.Helper()
	for _, w := range store.all() {
		if w.snapshot.Timestamp == tsMs {
			return w.snapshot
		}
	}
	t.Fatalf("no write in bucket %d; writes=%d", tsMs, len(store.all()))
	return nil
}

// --- tests ------------------------------------------------------------------

// TestCoinalyzePoller_OpenInterestIsRequestedWithoutUsdConversion is the first criterion:
// the conversion to USD stays OFF.
//
// With it on the provider returns notional, and notional is contracts x price — the decision
// axis would become ordered by price under another name.
func TestCoinalyzePoller_OpenInterestIsRequestedWithoutUsdConversion(t *testing.T) {
	poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	queries := fake.queriesFor("/open-interest-history")
	if len(queries) != 1 {
		t.Fatalf("open interest requested %d time(s), wanted 1", len(queries))
	}
	if got := queries[0].Get("convert_to_usd"); got != "false" {
		t.Fatalf("convert_to_usd=%q, wanted \"false\"", got)
	}
}

// TestCoinalyzePoller_OpenInterestLandsInTheContractField proves that the value returned
// with conversion off goes into the CONTRACTS field.
//
// THE VALUE IS THE BUCKET'S OPEN, and changing `c` to `o` fixes a one-bucket displacement
// between the two provenances — MEASURED against Binance's own `/futures/data/openInterestHist`,
// 30 daily buckets:
//
//	                  D-1       D        D+1
//	coinalyze `.o`  1.265%   0.018%    1.259%
//	coinalyze `.c`  0.018%   1.232%    1.639%
//	our `venue`     1.263%   0.030%    1.274%
//
// Binance reports open interest AT instant `t`; Coinalyze reports the OHLC of the interval
// that STARTS at `t`. Writing the close under an interval-start label put the long history
// one day ahead of the recent part of the SAME series. See the block at `coinalyzeOpen`.
//
// The notional field stays empty: writing it here would declare a unit nobody measured.
func TestCoinalyzePoller_OpenInterestLandsInTheContractField(t *testing.T) {
	poller, _, store, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	snap := firstWriteAt(t, store, coinalyzeFixtureTsMs)
	if snap.OpenInterestBase == nil {
		t.Fatal("OpenInterestBase missing — open interest did not reach the decision field")
	}
	if got := *snap.OpenInterestBase; got != 107640.019 {
		t.Fatalf("OpenInterestBase=%v, wanted 107640.019 (the bucket's `o` — the sample at the label instant)", got)
	}
	// AND NOT THE CLOSE, which is the sample at the FOLLOWING instant and put the series one
	// bucket ahead of the venue.
	if got := *snap.OpenInterestBase; got == 106117.954 {
		t.Fatal("wrote the `c`: the second provenance is one bucket ahead of the first again")
	}
	if snap.OpenInterest != nil {
		t.Fatalf("OpenInterest (notional) = %v; with convert_to_usd=false there is no notional to declare", *snap.OpenInterest)
	}
}

// TestCoinalyzePoller_FundingIsScaledFromPercentToTheStoredFraction is the second criterion:
// funding is PERCENT, already scaled.
//
// Measured on the same bar: `0.01` at Coinalyze is Binance's `0.00010000`. Writing the raw
// number would multiply funding by 100.
func TestCoinalyzePoller_FundingIsScaledFromPercentToTheStoredFraction(t *testing.T) {
	poller, _, store, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	snap := firstWriteAt(t, store, coinalyzeFixtureTsMs)
	if snap.FundingRate == nil {
		t.Fatal("FundingRate missing")
	}
	if got := *snap.FundingRate; got != 0.0001 {
		t.Fatalf("FundingRate=%v, wanted 0.0001 — the value the existing source writes on the same bar", got)
	}
}

// TestCoinalyzePoller_LongShortSharesMatchTheUnitTheExistingSourceStores is the third
// criterion: long/short arrives in the SAME UNIT as the existing source.
//
// The reference unit is the WRITTEN one, not the published one: Binance publishes 0.4886 and
// the existing poller writes 48.86 (parseLSRRow multiplies by 100). Coinalyze already
// publishes 48.86, so the correct conversion is the identity — applying the documentation's
// "factor 100" would write 4886.
func TestCoinalyzePoller_LongShortSharesMatchTheUnitTheExistingSourceStores(t *testing.T) {
	poller, _, store, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	snap := firstWriteAt(t, store, coinalyzeFixtureTsMs)

	// What the existing source would write from what Binance published on this bar.
	const binanceLongFraction, binanceShortFraction = 0.4886, 0.5114
	wantLong, wantShort := binanceLongFraction*100, binanceShortFraction*100

	if snap.LSLongPct == nil || snap.LSShortPct == nil {
		t.Fatal("shares missing — the measured pair closes at 100 and should have been accepted")
	}
	if got := *snap.LSLongPct; got != wantLong {
		t.Fatalf("LSLongPct=%v, wanted %v (what the existing source writes for 0.4886)", got, wantLong)
	}
	if got := *snap.LSShortPct; got != wantShort {
		t.Fatalf("LSShortPct=%v, wanted %v", got, wantShort)
	}
	if snap.LongShortRatio == nil || *snap.LongShortRatio != 0.9554 {
		t.Fatalf("LongShortRatio=%v, wanted 0.9554 — both APIs publish the ratio in the same unit", snap.LongShortRatio)
	}
}

// TestCoinalyzePoller_SharesThatDoNotCloseAreRefusedAndTheRatioSurvives mirrors the rule the
// existing source already applies: the pair is validated as a pair, and the ratio is not
// sacrificed by a defect in two sibling fields.
func TestCoinalyzePoller_SharesThatDoNotCloseAreRefusedAndTheRatioSurvives(t *testing.T) {
	poller, _, store, _, _ := newCoinalyzeHarness(t, []string{"1d"}, map[string]string{
		"/long-short-ratio-history": coinalyzeLongShortBrokenBody,
	})

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	snap := firstWriteAt(t, store, coinalyzeFixtureTsMs)
	if snap.LSLongPct != nil || snap.LSShortPct != nil {
		t.Fatalf("shares written (long=%v short=%v) despite 48.86+90 not closing at 100",
			snap.LSLongPct, snap.LSShortPct)
	}
	if snap.LongShortRatio == nil || *snap.LongShortRatio != 0.9554 {
		t.Fatalf("LongShortRatio=%v — the ratio is published in its own right and does not fall with the shares",
			snap.LongShortRatio)
	}
}

// THE 0.1 PERCENTAGE-POINT TOLERANCE, ASSERTED ON BOTH SIDES OF THE EDGE.
//
// The two cases above bound the rule from outside, not from inside: the acceptance one uses
// ZERO deviation (48.86+51.14) and the refusal one uses ~38 (48.86+90). Any tolerance fits
// between 0 and 38 — the `0.1 → 1.0` mutant passed the whole suite green, and with it a pair
// taken from two different instants would enter the series as if it were one snapshot.
//
// The literals are the spec's: sum 100.05 accepted (deviation 0.05) and sum 100.5 refused
// (deviation 0.5). They bound 0.1 from BOTH sides — `1.0` kills the refusal line, `0.01`
// kills the acceptance one. Neither sits AT 0.1: the comparison is floating point, and a
// case exactly on the edge would assert the rounding mode, not the rule.
func TestCoinalyzePoller_ShareToleranceIsDiscriminatedAtTheEdge(t *testing.T) {
	for _, tc := range []struct {
		nome        string
		long, short float64
		aceita      bool
	}{
		{nome: "sum 100.05 — deviation of 0.05 pp, within tolerance", long: 50.02, short: 50.03, aceita: true},
		{nome: "sum 100.5 — deviation of 0.5 pp, outside tolerance", long: 50.20, short: 50.30, aceita: false},
	} {
		t.Run(tc.nome, func(t *testing.T) {
			body := fmt.Sprintf(`[{"symbol":"BTCUSDT_PERP.A","history":[{"t":%d,"r":0.9554,"l":%v,"s":%v}]}]`,
				coinalyzeFixtureTsSec, tc.long, tc.short)
			poller, _, store, _, _ := newCoinalyzeHarness(t, []string{"1d"}, map[string]string{
				"/long-short-ratio-history": body,
			})

			if err := poller.PollOnce(context.Background()); err != nil {
				t.Fatalf("PollOnce: %v", err)
			}
			snap := firstWriteAt(t, store, coinalyzeFixtureTsMs)

			if !tc.aceita {
				if snap.LSLongPct != nil || snap.LSShortPct != nil {
					t.Fatalf("shares written (long=%v short=%v) with sum %v — the deviation exceeds 0.1 pp and the pair had to be refused",
						snap.LSLongPct, snap.LSShortPct, tc.long+tc.short)
				}
				return
			}
			if snap.LSLongPct == nil || snap.LSShortPct == nil {
				t.Fatalf("shares refused with sum %v — the deviation is 0.05 pp and fits the tolerance", tc.long+tc.short)
			}
			if *snap.LSLongPct != tc.long || *snap.LSShortPct != tc.short {
				t.Fatalf("shares written long=%v short=%v, wanted long=%v short=%v — acceptance must not alter the pair",
					*snap.LSLongPct, *snap.LSShortPct, tc.long, tc.short)
			}
		})
	}
}

// TestCoinalyzePoller_HistoryTimestampsAreReadAsSeconds is the fourth criterion.
//
// The history `t` comes in SECONDS (1787270400) while this same API's snapshot comes in
// milliseconds. Reading it as milliseconds puts the series in 1970-01-21: the bucket exists,
// the read answers, and the chart is born empty.
func TestCoinalyzePoller_HistoryTimestampsAreReadAsSeconds(t *testing.T) {
	poller, _, store, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	writes := store.all()
	if len(writes) == 0 {
		t.Fatal("no writes")
	}
	for _, w := range writes {
		if w.snapshot.Timestamp < 1_000_000_000_000 {
			t.Fatalf("Timestamp=%d is still in seconds — the whole series would fall in 1970", w.snapshot.Timestamp)
		}
	}
	if got := writes[0].snapshot.Timestamp; got != coinalyzeFixtureTsMs {
		t.Fatalf("first bucket=%d, wanted %d (the measured t=%d, in ms)",
			got, coinalyzeFixtureTsMs, coinalyzeFixtureTsSec)
	}
}

// TestCoinalyzePoller_TheWindowTravelsInSecondsToo: query `from`/`to` are in the same unit as
// the response's `t`. Sending them in milliseconds would request a window from the year
// 58,000 and return HTTP 200 with `[]` — indistinguishable from "there is no data".
func TestCoinalyzePoller_TheWindowTravelsInSecondsToo(t *testing.T) {
	poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	queries := fake.queriesFor("/open-interest-history")
	if len(queries) != 1 {
		t.Fatalf("open interest requested %d time(s)", len(queries))
	}
	if got := queries[0].Get("to"); got != "1787465684" {
		t.Fatalf("to=%q, wanted \"1787465684\" (the test clock in seconds)", got)
	}
	// `to` MINUS the seeding window: 2000 daily buckets = 172,800,000 s. The assertion here
	// is about the UNIT, and it only discriminates if `from` is checked against the window
	// the cycle actually requests.
	if got := queries[0].Get("from"); got != "1614665684" {
		t.Fatalf("from=%q, wanted \"1614665684\" (to minus the 2000 seeding buckets)", got)
	}
	// AND THE UNIT, WHICH IS THE POINT: in milliseconds these numbers would have 13 digits,
	// the requested window would be from the year 58,000 and the response would be HTTP 200
	// with `[]`.
	if len(queries[0].Get("to")) != 10 || len(queries[0].Get("from")) != 10 {
		t.Fatalf("the window is not in seconds: from=%q to=%q", queries[0].Get("from"), queries[0].Get("to"))
	}
}

// TestCoinalyzePoller_DeclaresTheStartLabelConvention: Coinalyze labels the START of the
// interval, and the declaration MUST cross all the way to the write — a convention that
// stays inside the poller aligns nothing.
func TestCoinalyzePoller_DeclaresTheStartLabelConvention(t *testing.T) {
	poller, _, store, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	writes := store.all()
	if len(writes) == 0 {
		t.Fatal("no writes")
	}
	for _, w := range writes {
		if w.label != derivatives.LabelIntervalStart {
			t.Fatalf("write with convention %s, wanted interval-start", w.label)
		}
	}
}

// TestCoinalyzePoller_UndeclaredConventionIsRefusedBeforeAnyWrite applies the rule to this
// feed: refusal happens BEFORE any write, not after contaminating storage.
func TestCoinalyzePoller_UndeclaredConventionIsRefusedBeforeAnyWrite(t *testing.T) {
	poller, fake, store, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)
	poller.feedLabel = derivatives.LabelUnspecified

	err := poller.PollOnce(context.Background())
	if err == nil {
		t.Fatal("PollOnce accepted a feed with no declared convention")
	}
	if len(store.all()) != 0 {
		t.Fatalf("%d write(s) despite the refusal", len(store.all()))
	}
	if len(fake.queriesFor("/open-interest-history")) != 0 {
		t.Fatal("the cycle requested data from a refused feed")
	}
}

// TestCoinalyzeInterval_MapsEveryConfiguredTimeframeOrRefusesIt pins the table against the
// list the API itself returns on a 400 — measured:
// `1min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 12hour, daily`.
func TestCoinalyzeInterval_MapsEveryConfiguredTimeframeOrRefusesIt(t *testing.T) {
	tests := []struct {
		timeframe string
		want      string
		wantErr   bool
	}{
		{timeframe: "5m", want: "5min"},
		{timeframe: "15m", want: "15min"},
		{timeframe: "30m", want: "30min"},
		{timeframe: "1h", want: "1hour"},
		{timeframe: "2h", want: "2hour"},
		{timeframe: "4h", want: "4hour"},
		{timeframe: "1d", want: "daily"},
		{timeframe: "1w", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.timeframe, func(t *testing.T) {
			got, err := CoinalyzeInterval(tc.timeframe)
			if tc.wantErr {
				if !errors.Is(err, ErrCoinalyzeTimeframeUnsupported) {
					t.Fatalf("CoinalyzeInterval(%q) err=%v, wanted ErrCoinalyzeTimeframeUnsupported", tc.timeframe, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("CoinalyzeInterval(%q): %v", tc.timeframe, err)
			}
			if got != tc.want {
				t.Fatalf("CoinalyzeInterval(%q)=%q, wanted %q", tc.timeframe, got, tc.want)
			}
		})
	}
}

// TestCoinalyzePoller_UnsupportedTimeframeIsSkippedAndTheCycleContinues: `1w` does not exist
// in the API, and a timeframe with no equivalent must not take down the ones that have one.
func TestCoinalyzePoller_UnsupportedTimeframeIsSkippedAndTheCycleContinues(t *testing.T) {
	poller, fake, store, _, _ := newCoinalyzeHarness(t, []string{"1w", "1d"}, nil)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	queries := fake.queriesFor("/open-interest-history")
	if len(queries) != 1 {
		t.Fatalf("open interest requested %d time(s), wanted 1 — only the supported timeframe", len(queries))
	}
	if got := queries[0].Get("interval"); got != "daily" {
		t.Fatalf("interval=%q, wanted \"daily\"", got)
	}
	for _, w := range store.all() {
		if w.timeframe != "1d" {
			t.Fatalf("write in timeframe %q, which the API does not serve", w.timeframe)
		}
	}
}

// TestCoinalyzePoller_BudgetIsChargedPerSymbolOnEveryCall: the cycle's three calls go through
// the budget, each declaring the symbols it consumes.
//
// The API charges ONE call per symbol of a multi-symbol request; a cycle that called the
// network outside the budget would spend without counting and would only discover the
// ceiling by hitting it — there is no rate-limit header on a 200.
func TestCoinalyzePoller_BudgetIsChargedPerSymbolOnEveryCall(t *testing.T) {
	poller, _, _, spender, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	charges := spender.all()
	if len(charges) != 3 {
		t.Fatalf("%d budget charge(s), wanted 3 (open interest, funding, long/short)", len(charges))
	}
	for i, symbols := range charges {
		if len(symbols) != 1 || symbols[0] != "BTCUSDT_PERP.A" {
			t.Fatalf("charge %d for %v, wanted [BTCUSDT_PERP.A] — the resolved id, one symbol", i, symbols)
		}
	}
}

// TestCoinalyzePoller_MarketRefusedByDenominationNeverReachesTheNetwork: a QUOTE_ASSET
// market is refused at the join, and the cycle continues without it.
func TestCoinalyzePoller_MarketRefusedByDenominationNeverReachesTheNetwork(t *testing.T) {
	fake, client := newCoinalyzeFake(t, nil)
	store := &recordingCoinalyzeStore{}
	spender := &recordingSpender{}
	catalog := coinalyze.NewCoinalyzeSymbolCatalog(coinalyze.CoinalyzeSymbolCatalogOptions{Client: client})

	poller := NewCoinalyzePoller(CoinalyzePollerOptions{
		Client:     client,
		Catalog:    catalog,
		Budget:     spender,
		Store:      store,
		Exchange:   "binance",
		MarketType: "future",
		Symbols:    []string{"BTCUSD_PERP", "BTC/USDT"},
		Timeframes: []string{"1d"},
		Lookback:   72 * time.Hour,
		Now:        func() time.Time { return time.Unix(1787465684, 0).UTC() },
	})

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	queries := fake.queriesFor("/open-interest-history")
	if len(queries) != 1 {
		t.Fatalf("%d open interest request(s), wanted 1", len(queries))
	}
	if got := queries[0].Get("symbols"); got != "BTCUSDT_PERP.A" {
		t.Fatalf("symbols=%q — the QUOTE_ASSET market entered the batch", got)
	}
}

// TestCoinalyzePoller_WritesTheBucketsInAscendingOrder: write order comes from the VALUES,
// not from map iteration, which is random in Go. Random order makes any write failure
// irreproducible.
func TestCoinalyzePoller_WritesTheBucketsInAscendingOrder(t *testing.T) {
	poller, _, store, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	writes := store.all()
	if len(writes) != 2 {
		t.Fatalf("%d write(s), wanted 2 (the fixture's two open interest buckets)", len(writes))
	}
	for i := 1; i < len(writes); i++ {
		if writes[i].snapshot.Timestamp <= writes[i-1].snapshot.Timestamp {
			t.Fatalf("bucket %d (%d) is not later than the previous one (%d)",
				i, writes[i].snapshot.Timestamp, writes[i-1].snapshot.Timestamp)
		}
	}
}

// TestCoinalyzePoller_SymbolIsWrittenUnderTheInternalSpelling: the provider id
// (BTCUSDT_PERP.A) is not this system's symbol, and writing the provider id would create a
// series no project read finds.
func TestCoinalyzePoller_SymbolIsWrittenUnderTheInternalSpelling(t *testing.T) {
	poller, _, store, _, _ := newCoinalyzeHarness(t, []string{"1d"}, nil)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	for _, w := range store.all() {
		if w.snapshot.Symbol != "BTC/USDT" {
			t.Fatalf("symbol written %q, wanted \"BTC/USDT\"", w.snapshot.Symbol)
		}
		if w.snapshot.Exchange != "binance" || w.snapshot.MarketType != "future" {
			t.Fatalf("scope written %s:%s, wanted binance:future", w.snapshot.Exchange, w.snapshot.MarketType)
		}
	}
}

// ============================================================================
// THE FUNDING UNIT ON THE COINALYZE PATH
// ============================================================================
//
// This source publishes PERCENT per settlement interval, and the interval is not the same
// across symbols: 8h, 4h and 1h coexist, with 42% of the measured fleet outside 8h. Two
// distinct errors live in the same pair of lines, and neither produces an exception:
//
//   - THE OPERAND. `applyFunding` converts percent into a fraction; both values are in scope
//     with similar names. Normalizing the PERCENT writes 100 × rate × 8/N.
//   - THE LAYER. `8/N` applied twice gives (8/N)²: 64x at N=1h, 16x at N=2h, 4x at N=4h and
//     1x at N=8h — that is, green on the majors and a lie in the tail.
//
// The three cases below anchor the operand, the origin of `N` and the silence in its absence.

// coinalyzeFundingHourlyBody is THREE buckets spaced ONE HOUR apart — the collection grid,
// not the contract's settlement cadence.
//
// The fixture exists for the mutant that derives `N` from the spacing of the buckets
// themselves: in a `1h` collection every symbol would measure N=1 and the 8h equivalent would
// come out 8x wrong, with `nStep` claiming "measured by spacing".
const coinalyzeFundingHourlyBody = `[{"symbol":"BTCUSDT_PERP.A","history":[
	{"t":1787270400,"o":0.01,"h":0.01,"l":0.01,"c":0.01},
	{"t":1787274000,"o":0.01,"h":0.01,"l":0.01,"c":0.01},
	{"t":1787277600,"o":0.01,"h":0.01,"l":0.01,"c":0.01}
]}]`

// mostraFloat prints a measurement pointer by VALUE, and its absence as a word.
//
// Without it a failure diagnostic prints the address, and the address does not say whether
// the number came out 100x larger or simply did not come out.
func mostraFloat(v *float64) string {
	if v == nil {
		return "missing"
	}
	return fmt.Sprintf("%v", *v)
}

// coinalyzeScope is the contract behind the provider id in this suite.
func coinalyzeScope() SymbolScope {
	return SymbolScope{ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT"}
}

// profilesMeasuring returns a registry that has already measured `hours` for this suite's
// contract over a window that STARTS at `observedFromMs`.
func profilesMeasuring(hours float64, observedFromMs int64) *FundingProfileRegistry {
	registry := NewFundingProfileRegistry()
	registry.setMeasured(Metrics{}, coinalyzeScope(), measuredCadence{Hours: hours, ObservedFromMs: observedFromMs})
	return registry
}

// TestCoinalyzePoller_TheEightHourEquivalentIsDerivedFromTheFractionNotThePercent ⭐ is the
// OPERAND case.
//
// Measured: Coinalyze's `0.01` is Binance's `0.00010000`. With N=4h — the cadence of 13 of the
// 31 symbols sampled on the LAN — the eight-hour equivalent is 0.0001 × 8/4 = 0.0002.
// Normalizing the percent would give 0.02, exactly 100x the right value, in an internally
// coherent row: the raw value stays 0.0001 and nothing complains.
func TestCoinalyzePoller_TheEightHourEquivalentIsDerivedFromTheFractionNotThePercent(t *testing.T) {
	// The observed window STARTS one day before the fixture bucket: it falls INSIDE it.
	profiles := profilesMeasuring(4, coinalyzeFixtureTsMs-24*60*60*1000)
	poller, _, store, _, _ := newCoinalyzeHarnessWithProfiles(t, []string{"1d"}, nil, profiles)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	snap := firstWriteAt(t, store, coinalyzeFixtureTsMs)
	if snap.FundingRate == nil || *snap.FundingRate != 0.0001 {
		t.Fatalf("FundingRate=%v, wanted 0.0001 (the fraction)", mostraFloat(snap.FundingRate))
	}
	if snap.FundingRate8h == nil {
		t.Fatal("FundingRate8h missing — the bucket falls inside the observed window and the eight-hour equivalent is required")
	}
	if got := *snap.FundingRate8h; got != 0.0002 {
		t.Fatalf("FundingRate8h=%v, wanted 0.0002 = 0.0001 × 8/4. "+
			"0.02 would be the normalized percent (100x); 0.0001 would be no normalization", got)
	}
	if snap.FundingIntervalHours == nil || *snap.FundingIntervalHours != 4 {
		t.Fatalf("FundingIntervalHours=%v, wanted 4 — the N that produced the number above", mostraFloat(snap.FundingIntervalHours))
	}
	if snap.NStep != derivatives.NFromMeasuredSpacing {
		t.Fatalf("NStep=%q, wanted %q — the interval's evidence travels with it", snap.NStep, derivatives.NFromMeasuredSpacing)
	}
}

// TestCoinalyzePoller_TheIntervalComesFromTheProfileAndNotFromTheBucketSpacing ⭐ is the case
// of the ORIGIN OF `N`.
//
// What this source delivers is the close of an OHLC bucket ALIGNED TO THE REQUESTED TIMEFRAME,
// not a settlement. Measuring spacing here measures OUR OWN grid: in a `1h` collection every
// contract would measure N=1, and an 8h contract would have its 8h inflated 8x under the
// "measured" stamp.
func TestCoinalyzePoller_TheIntervalComesFromTheProfileAndNotFromTheBucketSpacing(t *testing.T) {
	profiles := profilesMeasuring(8, coinalyzeFixtureTsMs-24*60*60*1000)
	poller, _, store, _, _ := newCoinalyzeHarnessWithProfiles(t, []string{"1h"},
		map[string]string{"/funding-rate-history": coinalyzeFundingHourlyBody}, profiles)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	snap := firstWriteAt(t, store, coinalyzeFixtureTsMs)
	if snap.FundingIntervalHours == nil || *snap.FundingIntervalHours != 8 {
		t.Fatalf("FundingIntervalHours=%v, wanted 8 — the CONTRACT's profile. "+
			"1 would be the bucket spacing, which is the collection grid", mostraFloat(snap.FundingIntervalHours))
	}
	if snap.FundingRate8h == nil || *snap.FundingRate8h != 0.0001 {
		t.Fatalf("FundingRate8h=%v, wanted 0.0001 = 0.0001 × 8/8. "+
			"0.0008 would be the N derived from one-hour spacing", mostraFloat(snap.FundingRate8h))
	}
}

// TestCoinalyzePoller_ABucketOlderThanTheObservationInheritsAndIsNotNormalized ⭐ delimits
// the TIME DOMAIN of the normalization, which is a WHERE clause and not an unconditional rule.
//
// Seeding from this source writes up to 2,000 retroactive buckets — in `1d`, back to 2021.
// The measured `N` describes the last sixteen settlements. Applying it back there is 2x to
// 8x of error stamped as evidence, and that is why normalized funding coverage TRAILS raw
// funding coverage in depth instead of matching 100%.
func TestCoinalyzePoller_ABucketOlderThanTheObservationInheritsAndIsNotNormalized(t *testing.T) {
	// The observed window STARTS one day AFTER the fixture bucket: it is outside it.
	profiles := profilesMeasuring(4, coinalyzeFixtureTsMs+24*60*60*1000)
	poller, _, store, _, _ := newCoinalyzeHarnessWithProfiles(t, []string{"1d"}, nil, profiles)

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	snap := firstWriteAt(t, store, coinalyzeFixtureTsMs)
	if snap.NStep != derivatives.NInheritedOutsideWindow {
		t.Fatalf("NStep=%q, wanted %q — the bucket precedes the window in which the interval was observed",
			snap.NStep, derivatives.NInheritedOutsideWindow)
	}
	if snap.FundingRate8h != nil {
		t.Fatalf("FundingRate8h=%v on a bucket outside the observed window — it is the normalization of 2021 "+
			"with today's N, which the time domain exists to prevent", *snap.FundingRate8h)
	}
	// The interval TRAVELS: it remains the best thing known about the contract, and the step
	// says it was not observed here.
	if snap.FundingIntervalHours == nil || *snap.FundingIntervalHours != 4 {
		t.Fatalf("FundingIntervalHours=%v, wanted 4 — the step declares inheritance, it does not erase the interval",
			mostraFloat(snap.FundingIntervalHours))
	}
	// And the RAW value survives: it is a real observation.
	if snap.FundingRate == nil || *snap.FundingRate != 0.0001 {
		t.Fatalf("FundingRate=%v, wanted 0.0001 — the raw value is observation and does not derive from N", mostraFloat(snap.FundingRate))
	}
}

// TestCoinalyzePoller_WithNoIntervalTheRateTravelsAndNothingIsNormalized: no step produced an
// interval. The raw value travels, the equivalent is NOT fabricated from a presumed cadence,
// and the step says so instead of staying blank.
func TestCoinalyzePoller_WithNoIntervalTheRateTravelsAndNothingIsNormalized(t *testing.T) {
	poller, _, store, _, _ := newCoinalyzeHarnessWithProfiles(t, []string{"1d"}, nil, NewFundingProfileRegistry())

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	snap := firstWriteAt(t, store, coinalyzeFixtureTsMs)
	if snap.NStep != derivatives.NUnavailable {
		t.Fatalf("NStep=%q, wanted %q — with no derived interval, the step declares unavailability",
			snap.NStep, derivatives.NUnavailable)
	}
	if snap.FundingRate8h != nil {
		t.Fatalf("FundingRate8h=%v with no interval at all — it is a number normalized by a presumed cadence",
			*snap.FundingRate8h)
	}
	if snap.FundingIntervalHours != nil {
		t.Fatalf("FundingIntervalHours=%v with no derived interval", *snap.FundingIntervalHours)
	}
	if snap.FundingRate == nil || *snap.FundingRate != 0.0001 {
		t.Fatalf("FundingRate=%v, wanted 0.0001 — the raw value is a real observation and travels even without N", mostraFloat(snap.FundingRate))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// THE SEEDED MARK SURVIVES RESTART AND DEPLOY.
//
// This collection cycle measures 1.87x its own ticker (`499 × 3 × 3 ÷ 40 = 112.3 min` against
// 60), and a process-map mark written at the END of the cycle preserves nothing across a deploy
// shorter than ~2 h. The cases below build a NEW poller, which is what a deploy produces, and
// measure the REQUESTED WINDOW: 2000 buckets is re-seeding, the tail is a resumption.
// ─────────────────────────────────────────────────────────────────────────────

// ledgerDuplo is the durable ledger, with the response declared per timeframe.
type ledgerDuplo struct {
	mu       sync.Mutex
	oldest   map[string]map[string]time.Time // timeframe → internal symbol → oldest
	err      error
	consulta []string
}

func (l *ledgerDuplo) OldestDerivativesBySymbol(
	_ context.Context, _, _, _, timeframe string,
) (map[string]time.Time, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.consulta = append(l.consulta, timeframe)
	if l.err != nil {
		return nil, l.err
	}
	return l.oldest[timeframe], nil
}

func (l *ledgerDuplo) consultados() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.consulta...)
}

// coinalyzeHarnessComLedger assembles this file's same cycle with a durable ledger.
func coinalyzeHarnessComLedger(
	t *testing.T, timeframes []string, ledger CoinalyzeSeedLedger,
) (*CoinalyzePoller, *coinalyzeFake) {
	t.Helper()
	poller, fake, _, _, _ := newCoinalyzeHarness(t, timeframes, nil)
	poller.seedLedger = ledger
	return poller, fake
}

// A NEW PROCESS DOES NOT RE-SEED WHAT THE DURABLE TIER ALREADY HOLDS.
//
// This is the discriminating case: without the ledger, a freshly built poller always requests
// the 2000 buckets, which is exactly the seeding a deploy burns.
func TestSeed_ANewProcessDoesNotReseedWhatTheDurableTierAlreadyHolds(t *testing.T) {
	agora := time.Unix(1787465684, 0).UTC()
	ledger := &ledgerDuplo{oldest: map[string]map[string]time.Time{
		// Depth of one seeding: 2000 daily buckets back.
		"1d": {"BTC/USDT": agora.Add(-2000 * 24 * time.Hour)},
	}}

	poller, fake := coinalyzeHarnessComLedger(t, []string{"1d"}, ledger)
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	pedido := windowSeconds(t, fake.queriesFor("/open-interest-history"))
	if pedido >= 2000*86400 {
		t.Fatalf("the new process requested %d s — it re-seeded what the durable tier already held", pedido)
	}
	if cauda := int64(poller.windowFor("1d", false) / time.Second); pedido != cauda {
		t.Fatalf("the new process requested %d s, expected the tail (%d s)", pedido, cauda)
	}
	if consultas := ledger.consultados(); len(consultas) != 1 || consultas[0] != "1d" {
		t.Fatalf("the ledger was queried as %v, expected one query per timeframe", consultas)
	}
}

// AND TAIL DEPTH DOES NOT COUNT AS SEEDING.
//
// The distinction is why the mark is derived from `min(timestamp)` and not from row counts:
// a symbol with 2000 tail cycles has 2000 rows and no deep history.
func TestSeed_TailDepthIsNotSeeding(t *testing.T) {
	agora := time.Unix(1787465684, 0).UTC()
	ledger := &ledgerDuplo{oldest: map[string]map[string]time.Time{
		// Within the tail: only what a shallow cycle would have written.
		"1d": {"BTC/USDT": agora.Add(-2 * 24 * time.Hour)},
	}}

	poller, fake := coinalyzeHarnessComLedger(t, []string{"1d"}, ledger)
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	if pedido := windowSeconds(t, fake.queriesFor("/open-interest-history")); pedido != 2000*86400 {
		t.Fatalf("the cycle requested %d s; tail depth is not seeding and it had to request 2000 buckets", pedido)
	}
}

// THE REQUIREMENT IS ON EVERY TIMEFRAME — and it is what closes the LINE hole measured in
// production: four batches of 20 died on 429 and left 40 symbols with `4h` full and ZERO `1h`.
func TestSeed_DepthInOneTimeframeDoesNotSeedTheOthers(t *testing.T) {
	agora := time.Unix(1787465684, 0).UTC()
	ledger := &ledgerDuplo{oldest: map[string]map[string]time.Time{
		"4h": {"BTC/USDT": agora.Add(-2000 * 4 * time.Hour)}, // full
		"1h": {},                                             // zero rows, the measured hole
	}}

	poller, fake := coinalyzeHarnessComLedger(t, []string{"1h", "4h"}, ledger)
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	queries := fake.queriesFor("/open-interest-history")
	if len(queries) != 2 {
		t.Fatalf("expected one request per timeframe, got %d", len(queries))
	}
	for i, q := range queries {
		from, _ := strconv.ParseInt(q.Get("from"), 10, 64)
		to, _ := strconv.ParseInt(q.Get("to"), 10, 64)
		if janela := to - from; janela < 2000*3600 {
			t.Fatalf("request %d asked for %d s: the symbol was taken as seeded with `1h` empty", i, janela)
		}
	}
}

// AND THE SYMMETRIC CASE ⭐, which is what closes the CONJUNCTION.
//
// The case above puts the hole in the FIRST timeframe, and an independent verification
// measured the consequence: `range p.timeframes[:1]` — a loop that looks only at the first —
// passes it. The requirement is not "some timeframe has depth"; it is EVERY timeframe, and a
// pair of cases that only pierces the head of the list does not distinguish the two readings.
//
// Here the hole is in the LAST one: `1h` full, `4h` empty. The `[:1]` mutant would take the
// symbol as seeded and both requests would go out shallow.
func TestSeed_DepthInTheFirstTimeframeDoesNotSeedALaterOne(t *testing.T) {
	agora := time.Unix(1787465684, 0).UTC()
	ledger := &ledgerDuplo{oldest: map[string]map[string]time.Time{
		"1h": {"BTC/USDT": agora.Add(-2000 * time.Hour)}, // full
		"4h": {},                                         // zero rows — the same hole, on the other side of the list
	}}

	poller, fake := coinalyzeHarnessComLedger(t, []string{"1h", "4h"}, ledger)
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	queries := fake.queriesFor("/open-interest-history")
	if len(queries) != 2 {
		t.Fatalf("expected one request per timeframe, got %d", len(queries))
	}
	for i, q := range queries {
		from, _ := strconv.ParseInt(q.Get("from"), 10, 64)
		to, _ := strconv.ParseInt(q.Get("to"), 10, 64)
		if janela := to - from; janela < 2000*3600 {
			t.Fatalf("request %d asked for %d s: the symbol was taken as seeded with `4h` empty — "+
				"the requirement is on EVERY timeframe, not the first", i, janela)
		}
	}
}

// A FAILING QUERY DECLARES NOBODY SEEDED — nor unmarks whoever already was.
//
// Not knowing is not zero, and it is not the opposite either: the cycle falls back to the
// process map, the behavior before the ledger existed.
func TestSeed_AnUnreadableLedgerFallsBackToTheProcessMark(t *testing.T) {
	ledger := &ledgerDuplo{err: errors.New("questdb down")}

	poller, fake := coinalyzeHarnessComLedger(t, []string{"1d"}, ledger)
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("first cycle: %v", err)
	}
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("second cycle: %v", err)
	}

	queries := fake.queriesFor("/open-interest-history")
	if len(queries) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(queries))
	}
	primeira := windowSeconds(t, queries[:1])
	segunda := windowSeconds(t, queries[1:])
	if primeira != 2000*86400 {
		t.Fatalf("with an unreadable ledger the first cycle had to seed, requested %d s", primeira)
	}
	if segunda >= primeira {
		t.Fatalf("the process mark did not hold: the second cycle requested %d s", segunda)
	}
}

// ============================================================================
// REPEATED FAILURE DOES NOT AMPLIFY THE REQUEST
//
// THE DEFECT: a failing symbol does not enter the seeded mark — and it must not, otherwise its
// deep history never arrives — but that made the next cycle request the deep window AGAIN, at
// the same pace, indefinitely. The plan is deterministic (`planCycle` builds everything before
// the first fetch), so two consecutive cycles emitted the same 2000-bucket request per symbol,
// per timeframe, forever.
//
// THE POLICY: on failure the symbol enters BACKOFF — its deep pass waits, and this cycle
// requests the tail (which is what the chart reads). The wait is counted in CYCLES and grows
// with the failure streak (1, 2, 4 and 8); when the symbol writes again, the wait ADVANCES one
// cycle instead of restarting, and the deep pass returns. A symbol in backoff is also not
// marked as seeded for having written the tail: tail depth is not seeding, and marking it
// there would lose the deep history instead of postponing it.
// ============================================================================

// CLOCK TESTING: none. The wait is counted in cycles, and a cycle is a call to `PollOnce` — the
// case measures the policy, not time.

// TestCoinalyzeSeedBackoff_GrowsWithTheStreak — the wait is a function of the failure STREAK,
// and it has a ceiling.
func TestCoinalyzeSeedBackoff_GrowsWithTheStreak(t *testing.T) {
	for _, tc := range []struct {
		streak int
		ciclos int
	}{
		{streak: 0, ciclos: 0},
		{streak: 1, ciclos: 1},
		{streak: 2, ciclos: 2},
		{streak: 3, ciclos: 4},
		{streak: 4, ciclos: 8},
		// THE CEILING: eight cycles. The wait does not grow without bound — it limits
		// pressure, not the data.
		{streak: 9, ciclos: 8},
		{streak: 100, ciclos: 8},
	} {
		if got := coinalyzeSeedBackoffCycles(tc.streak); got != tc.ciclos {
			t.Errorf("streak of %d failure(s) returned %d cycle(s) of wait, wanted %d",
				tc.streak, got, tc.ciclos)
		}
	}
}

// TestCoinalyzePoller_ASymbolThatFailedAsksForLessThanTheDeepWindow ⭐ is the mandatory
// counter-example: a symbol that failed in the previous cycle asks for LESS than the 2000
// buckets — and the symbol that wrote keeps requesting the tail, without being dragged back.
func TestCoinalyzePoller_ASymbolThatFailedAsksForLessThanTheDeepWindow(t *testing.T) {
	poller, fake, _, _, _ := newCoinalyzeHarness(t, []string{"1d"}, map[string]string{
		"/future-markets":           coinalyzeDoisMercadosBody,
		"/open-interest-history":    coinalyzeDoisOpenInterestBody,
		"/funding-rate-history":     coinalyzeDoisFundingBody,
		"/long-short-ratio-history": coinalyzeDoisLongShortBody,
	})
	poller.symbolsFunc = func(context.Context) []string { return []string{"BTC/USDT", "ETH/USDT"} }
	store := &storeQueFalhaPorSimbolo{falhaEm: "BTC/USDT", err: errors.New("durable tier unavailable")}
	poller.store = store

	const profunda = int64(2000 * 86400)

	// Cycle 1 — nobody seeded: one deep request for both.
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 1: %v", err)
	}
	cicloUm := janelasPorSimbolo(t, fake.queriesFor("/open-interest-history"))
	if got := cicloUm["BTCUSDT_PERP.A,ETHUSDT_PERP.A"]; got != profunda {
		t.Fatalf("cycle 1 requested %d s for both symbols, expected the deep pass (%d s)", got, profunda)
	}

	// Storage returns — the failure belonged to CYCLE 1, and that is what backoff handles.
	store.paraDeFalhar()

	// Cycle 2 — the symbol that failed in the previous cycle does NOT re-request the 2000
	// buckets: it is in backoff, and this cycle requests the tail.
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 2: %v", err)
	}
	todas := fake.queriesFor("/open-interest-history")
	cicloDois := janelasPorSimbolo(t, todas[1:])
	if len(cicloDois) == 0 {
		t.Fatal("cycle 2 requested nothing — both symbols had to keep updating the tail")
	}
	for pedido, janela := range cicloDois {
		if janela >= profunda {
			t.Errorf("the cycle after the failure requested the deep window (%d s) in %q — repeated "+
				"failure amplifies the request at the same pace", janela, pedido)
		}
	}
	// AND THE FLEET WAS NOT DRAGGED: with BTC in backoff, both go in the SAME tail request —
	// no new symbol entered seeding, and the cycle did not split into two requests because of
	// one failure.
	if len(cicloDois) != 1 {
		t.Errorf("the cycle after the failure emitted %d request(s); with the failing symbol in "+
			"backoff both go on the same tail request: %v", len(cicloDois), cicloDois)
	}
	juntos := false
	for pedido := range cicloDois {
		if strings.Contains(pedido, "BTCUSDT_PERP.A") && strings.Contains(pedido, "ETHUSDT_PERP.A") {
			juntos = true
		}
	}
	if !juntos {
		t.Errorf("one symbol's failure split the fleet: %v", cicloDois)
	}

	// Cycle 3 — the wait ADVANCED (one cycle, because it wrote the tail): the symbol that never
	// received the deep pass requests it again, ALONE, and the one that wrote stays on the tail.
	// This half is what keeps backoff from becoming abandonment of the deep history.
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 3: %v", err)
	}
	todas = fake.queriesFor("/open-interest-history")
	cicloTres := janelasPorSimbolo(t, todas[len(todas)-2:])
	if got := cicloTres["BTCUSDT_PERP.A"]; got != profunda {
		t.Errorf("after the wait, the symbol that never received the deep pass requested %d s; it "+
			"was not marked as seeded by the tail and must request the deep window again "+
			"(%d s): %v", got, profunda, cicloTres)
	}
	if got, pediu := cicloTres["ETHUSDT_PERP.A"]; !pediu || got >= profunda {
		t.Errorf("the symbol that wrote requested %d s (present=%v) after the other's wait: it left "+
			"seeding and had to go on the tail", got, pediu)
	}
	// AND THE MARK DID NOT ABSOLVE IT: whoever wrote the TAIL did not receive the deep pass.
	poller.seededMu.Lock()
	_, marcado := poller.seededSymbols["BTC/USDT"]
	poller.seededMu.Unlock()
	if marcado {
		t.Fatal("the tail write marked the symbol as seeded — the deep window would never be " +
			"requested again, and its history would never arrive")
	}
	// AND THE ONE THAT WROTE DOES NOT CARRY THE OTHER'S BACKOFF: ETH stayed on the tail in every
	// cycle.
	if _, emRecuo := poller.seedBackoff["ETH/USDT"]; emRecuo {
		t.Error("the symbol that wrote entered the other's backoff — backoff is per symbol")
	}
}

// TestCoinalyzePoller_TheBackoffIsAnnouncedByNameAndLevel — pressure reduction stays VISIBLE.
//
// Without the line, backoff is indistinguishable from silence: the operator sees the batch
// fail (the warning already exists) and then sees deep history for that symbol not arrive,
// with nothing connecting the two. The level matters and is not decoration: the process
// default is WARN, and below it the line goes nowhere.
func TestCoinalyzePoller_TheBackoffIsAnnouncedByNameAndLevel(t *testing.T) {
	poller, _, _, _, sink := newCoinalyzeHarness(t, []string{"1d"}, map[string]string{
		"/future-markets":           coinalyzeDoisMercadosBody,
		"/open-interest-history":    coinalyzeDoisOpenInterestBody,
		"/funding-rate-history":     coinalyzeDoisFundingBody,
		"/long-short-ratio-history": coinalyzeDoisLongShortBody,
	})
	poller.symbolsFunc = func(context.Context) []string { return []string{"BTC/USDT", "ETH/USDT"} }
	poller.store = &storeQueFalhaPorSimbolo{falhaEm: "BTC/USDT", err: errors.New("durable tier unavailable")}

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 1: %v", err)
	}
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("cycle 2: %v", err)
	}

	level, achou := sink.LevelOf("symbols in backoff")
	if !achou {
		t.Fatalf("backoff was not announced; captured: %v", sink.Records())
	}
	if level != logger.LevelWarn {
		t.Errorf("backoff came out at %s; below WARN it does not appear at the process default level", level)
	}
	for _, r := range sink.Records() {
		if !strings.Contains(r.Text(), "symbols in backoff") {
			continue
		}
		// THE SYMBOL NAME, because the operator's question is "why does THIS one lack deep
		// history" — and only it: the one that wrote did not enter backoff.
		if !strings.Contains(r.Text(), "BTCUSDT_PERP.A") {
			t.Errorf("the backoff line does not name the symbol that failed: %q", r.Text())
		}
		if strings.Contains(r.Text(), "ETHUSDT_PERP.A") {
			t.Errorf("the backoff line names a symbol that did not fail: %q", r.Text())
		}
	}
}
