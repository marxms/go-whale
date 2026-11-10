package collector

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"
)

// fixedNowMs is an arbitrary but FIXED instant. The walk is reproducible only if the
// clock is an input, and a pagination bug that appears at one wall-clock offset and
// not another is exactly the kind that reaches production.
const fixedNowMs = int64(1_754_600_000_000) // 2025-08-07T21:33:20Z

// recordingStore accepts every write and remembers it. It implements only the persistence
// surface the poll loop uses — the writer plus the previous-snapshot read — so the fake
// proves the consumer port is minimal.
type recordingStore struct {
	mu    sync.Mutex
	saved []*derivatives.DerivativesSnapshot
	tfs   []string
}

func (s *recordingStore) SaveSnapshot(context.Context, *derivatives.DerivativesSnapshot) error {
	return nil
}

func (s *recordingStore) SaveSeriesOnly(_ context.Context, snapshot *derivatives.DerivativesSnapshot, timeframe string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved = append(s.saved, snapshot)
	s.tfs = append(s.tfs, timeframe)
	return nil
}

func (s *recordingStore) GetLatestSnapshot(context.Context, string, string, string) (*derivatives.DerivativesSnapshot, error) {
	return nil, nil
}

func (s *recordingStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.saved)
}

func testScope() SymbolScope {
	return SymbolScope{ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT"}
}

// providerPage mimics the measured behavior of the endpoint: it answers with the LAST
// `limit` points of the requested window, oldest-first. That is the behavior that makes
// a forward-advancing cursor produce a truncated series which looks complete.
func providerPage(page historyPage, periodMs int64, limit int) []int64 {
	var newestFirst []int64
	for ts := page.EndMs - (page.EndMs % periodMs); ts >= page.StartMs; ts -= periodMs {
		newestFirst = append(newestFirst, ts)
		if len(newestFirst) == limit {
			break
		}
	}
	out := make([]int64, 0, len(newestFirst))
	for i := len(newestFirst) - 1; i >= 0; i-- {
		out = append(out, newestFirst[i])
	}
	return out
}

func TestHistoryDepthPoints(t *testing.T) {
	tests := []struct {
		name      string
		timeframe string
		seriesCap int
		want      int
		wantOK    bool
	}{
		// The declared table: 288 points of the pair's own timeframe, truncated by the
		// provider's thirty-day retention.
		{"five minutes reaches the declared baseline", "5m", 200, 288, true},
		{"fifteen minutes reaches the declared baseline", "15m", 200, 288, true},
		{"one hour reaches the declared baseline", "1h", 200, 288, true},
		{"four hours is truncated by retention", "4h", 200, 180, true},
		{"one day is born at the declared minimum", "1d", 200, 30, true},
		{"a larger store cap deepens the collection", "5m", 8640, 8640, true},
		{"a store cap beyond retention is still truncated", "1h", 5000, 720, true},
		{"an unrecognized timeframe has no declared depth", "banana", 200, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := historyDepthPoints(tt.timeframe, tt.seriesCap)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Errorf("points = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestHistoryWalkerAdvancesBackwardWithoutRepeating(t *testing.T) {
	const timeframe = "5m"
	periodMs, _ := candle.ParseTimeframeMs(timeframe)

	walker, ok := newHistoryWalker(fixedNowMs, timeframe, 1500)
	if !ok {
		t.Fatal("newHistoryWalker refused a supported timeframe")
	}

	var pages []historyPage
	for {
		page, more := walker.next()
		if !more {
			break
		}
		pages = append(pages, page)
		walker.observe(page, page.StartMs, historyPageLimit)
	}

	if len(pages) != 3 {
		t.Fatalf("pages = %d, want 3 for a 1500-point target at limit %d", len(pages), historyPageLimit)
	}
	if pages[0].EndMs != fixedNowMs {
		t.Errorf("first page ends at %d, want the present instant %d", pages[0].EndMs, fixedNowMs)
	}

	for i := 1; i < len(pages); i++ {
		prev, curr := pages[i-1], pages[i]
		if curr.EndMs >= prev.EndMs {
			t.Errorf("page %d ends at %d, not strictly before the previous end %d — the cursor is not walking backward",
				i, curr.EndMs, prev.EndMs)
		}
		if curr.EndMs >= prev.StartMs {
			t.Errorf("page %d [%d,%d] overlaps the previous window [%d,%d] — the newest page is being re-fetched",
				i, curr.StartMs, curr.EndMs, prev.StartMs, prev.EndMs)
		}
		if curr.StartMs > curr.EndMs {
			t.Errorf("page %d is inverted: [%d,%d]", i, curr.StartMs, curr.EndMs)
		}
	}

	// Each full page must span the buckets it asked for; a shorter span would silently
	// skip the interior, which is the defect the backward walk exists to prevent.
	wantSpan := (historyPageLimit - 1) * periodMs
	if got := pages[0].EndMs - pages[0].StartMs; got != wantSpan {
		t.Errorf("first page spans %d ms, want %d", got, wantSpan)
	}
}

func TestHistoryWalkerNeverRewindsToTheRequestedEnd(t *testing.T) {
	walker, ok := newHistoryWalker(fixedNowMs, "5m", 1500)
	if !ok {
		t.Fatal("newHistoryWalker refused a supported timeframe")
	}

	page, more := walker.next()
	if !more {
		t.Fatal("the walk produced no first page")
	}
	// A sparse window: the provider held a single point, sitting at the newest edge.
	// The cursor must still clear the whole window it just asked for — anchoring it on
	// what came back would re-request the same span forever.
	walker.observe(page, page.EndMs, 1)

	second, more := walker.next()
	if !more {
		t.Fatal("the walk stopped after a page that returned data")
	}
	if second.EndMs != page.StartMs-1 {
		t.Errorf("second page ends at %d, want %d — the cursor did not clear the requested window",
			second.EndMs, page.StartMs-1)
	}
}

func TestHistoryWalkerClampsAtTheRetentionBoundary(t *testing.T) {
	const timeframe = "5m"

	// A target far deeper than retention: the clamp, not the target, has to stop it.
	walker, ok := newHistoryWalker(fixedNowMs, timeframe, 100_000)
	if !ok {
		t.Fatal("newHistoryWalker refused a supported timeframe")
	}
	boundary := fixedNowMs - (derivativesRetention - retentionGuardBand).Milliseconds()

	pages := 0
	var last historyPage
	for {
		page, more := walker.next()
		if !more {
			break
		}
		pages++
		if pages > 64 {
			t.Fatal("the walk did not terminate — the cursor is not strictly decreasing")
		}
		if page.StartMs < boundary {
			t.Fatalf("page %d starts at %d, %d ms beyond the retention boundary %d — this is the request that returns HTTP 400 -1130",
				pages, page.StartMs, boundary-page.StartMs, boundary)
		}
		last = page
		walker.observe(page, page.StartMs, historyPageLimit)
	}

	if last.StartMs != boundary {
		t.Errorf("the deepest page starts at %d, want the boundary %d — retention is not being used in full", last.StartMs, boundary)
	}
	if pages < 2 {
		t.Errorf("pages = %d, want a multi-page walk for a 30-day 5m target", pages)
	}
}

func TestHistoryWalkerStopsOnAnEmptyPage(t *testing.T) {
	walker, ok := newHistoryWalker(fixedNowMs, "5m", 1500)
	if !ok {
		t.Fatal("newHistoryWalker refused a supported timeframe")
	}

	page, more := walker.next()
	if !more {
		t.Fatal("the walk produced no first page")
	}
	// Inside retention, an empty window means the series does not reach that far back;
	// every older window is emptier still, so paying for them is pure waste.
	walker.observe(page, 0, 0)

	if _, more := walker.next(); more {
		t.Error("the walk continued past an empty page")
	}
}

func TestCollectPaginatedHistoryWalksBackwardAndPersists(t *testing.T) {
	const timeframe = "5m"
	periodMs, _ := candle.ParseTimeframeMs(timeframe)
	store := &recordingStore{}

	var requested []historyPage
	saved := collectPaginatedHistory(
		context.Background(), Metrics{}, store, testScope(), feedOpenInterest, timeframe,
		1200, fixedNowMs,
		func(page historyPage) ([]int64, error) {
			requested = append(requested, page)
			return providerPage(page, periodMs, historyPageLimit), nil
		},
		func(ts int64) int64 { return ts },
		func(ts int64) *derivatives.DerivativesSnapshot {
			contracts := 1234.5
			return &derivatives.DerivativesSnapshot{
				Exchange:         "binance",
				MarketType:       "future",
				Symbol:           "BTC/USDT",
				Timestamp:        ts,
				OpenInterestBase: &contracts,
			}
		},
	)

	if len(requested) < 3 {
		t.Fatalf("requests = %d, want at least 3 for a 1200-point target", len(requested))
	}
	for i := 1; i < len(requested); i++ {
		if requested[i].EndMs >= requested[i-1].StartMs {
			t.Fatalf("request %d [%d,%d] re-enters the previous window [%d,%d]",
				i, requested[i].StartMs, requested[i].EndMs, requested[i-1].StartMs, requested[i-1].EndMs)
		}
	}
	if saved != store.count() {
		t.Errorf("reported %d saved, store holds %d", saved, store.count())
	}
	if saved < 1000 {
		t.Errorf("saved = %d, want the walk to assemble close to its 1200-point target", saved)
	}

	// Every persisted point carries contracts, never a notional.
	seen := make(map[int64]bool, saved)
	for _, snapshot := range store.saved {
		if snapshot.OpenInterestBase == nil {
			t.Fatal("a persisted point carries no base-unit open interest")
		}
		if snapshot.OpenInterest != nil {
			t.Fatal("the bootstrap persisted a notional into the decision series")
		}
		if seen[snapshot.Timestamp] {
			t.Fatalf("timestamp %d was collected twice — a page was re-fetched", snapshot.Timestamp)
		}
		seen[snapshot.Timestamp] = true
	}
}

func TestCollectPaginatedHistorySkipsAnUnsupportedPeriod(t *testing.T) {
	store := &recordingStore{}
	calls := 0

	// 1w is served by neither open interest nor long/short ratio, and the provider says
	// so with HTTP 200 and an empty array — indistinguishable, in a log, from a symbol
	// with no data. The matrix answers without spending the request.
	saved := collectPaginatedHistory(
		context.Background(), Metrics{}, store, testScope(), feedOpenInterest, "1w",
		200, fixedNowMs,
		func(historyPage) ([]int64, error) {
			calls++
			return nil, nil
		},
		func(ts int64) int64 { return ts },
		func(int64) *derivatives.DerivativesSnapshot { return nil },
	)

	if calls != 0 {
		t.Errorf("issued %d requests for a period the provider does not serve, want 0", calls)
	}
	if saved != 0 || store.count() != 0 {
		t.Errorf("saved = %d / stored = %d, want 0", saved, store.count())
	}
}

func TestCollectPaginatedHistoryRefusesAnUnrecognisedTimeframe(t *testing.T) {
	store := &recordingStore{}
	calls := 0

	saved := collectPaginatedHistory(
		context.Background(), Metrics{}, store, testScope(), feedOpenInterest, "banana",
		200, fixedNowMs,
		func(historyPage) ([]int64, error) {
			calls++
			return nil, nil
		},
		func(ts int64) int64 { return ts },
		func(int64) *derivatives.DerivativesSnapshot { return nil },
	)

	if calls != 0 || saved != 0 {
		t.Errorf("calls = %d, saved = %d — an unrecognized timeframe must not produce a request", calls, saved)
	}
}

func TestBootstrapLSRRequestsTheDailyPeriodAsItself(t *testing.T) {
	store := &recordingStore{}
	periodMs, _ := candle.ParseTimeframeMs("1d")

	var requestedPeriods []string
	var requestedWindows []historyPage

	poller := &DerivativesPoller{
		cfg:             config.DerivativesConfig{Enabled: true, MaxSeriesSize: 200},
		store:           store,
		tfReg:           candle.NewTimeframeRegistry([]string{"1d"}),
		fundingProfiles: NewFundingProfileRegistry(),
	}
	client := &fakeRestClient{lsrHistory: func(_, timeframe string, _ int64, sinceMs, endMs int64) []derivatives.LongShortRatioPoint {
		requestedPeriods = append(requestedPeriods, timeframe)
		requestedWindows = append(requestedWindows, historyPage{StartMs: sinceMs, EndMs: endMs})

		var points []derivatives.LongShortRatioPoint
		for _, ts := range providerPage(historyPage{StartMs: sinceMs, EndMs: endMs}, periodMs, historyPageLimit) {
			points = append(points, derivatives.LongShortRatioPoint{
				Timestamp: ts, Ratio: 1.2, LongPct: 54.5, ShortPct: 45.5, HasShares: true,
			})
		}
		return points
	}}

	saved := poller.bootstrapLSRForTimeframe(context.Background(), client, testScope(), "1d")

	if len(requestedPeriods) == 0 {
		t.Fatal("the daily bootstrap issued no request")
	}
	for i, period := range requestedPeriods {
		// The previous code remapped 1d to 4h on the strength of a false comment, which
		// filled the daily ZSET with 4h readings aligned to daily buckets.
		if period != "1d" {
			t.Errorf("request %d used period %q, want %q", i, period, "1d")
		}
	}
	for i, window := range requestedWindows {
		if window.StartMs <= 0 || window.EndMs <= window.StartMs {
			t.Errorf("request %d carried an incomplete window [%d,%d]; startTime alone returns the present-day tail",
				i, window.StartMs, window.EndMs)
		}
	}
	if saved == 0 || store.count() != saved {
		t.Errorf("saved = %d, stored = %d", saved, store.count())
	}
	for _, timeframe := range store.tfs {
		if timeframe != "1d" {
			t.Errorf("a daily bootstrap wrote into the %q series", timeframe)
		}
	}
}

func TestRetentionBoundaryStaysInsideTheMeasuredBracket(t *testing.T) {
	// Measured: requests at 25 days answer, requests at 35 days return HTTP 400 (-1130).
	// The declared retention plus its guard band must sit strictly inside that bracket,
	// otherwise the clamp protects nothing.
	effective := derivativesRetention - retentionGuardBand
	if effective <= 25*24*time.Hour {
		t.Errorf("effective horizon %s does not even reach the 25-day window measured as working", effective)
	}
	if effective >= 35*24*time.Hour {
		t.Errorf("effective horizon %s reaches the 35-day window measured as failing", effective)
	}
}
