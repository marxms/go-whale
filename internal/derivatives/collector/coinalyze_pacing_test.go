package collector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/coinalyze"
)

// ─────────────────────────────────────────────────────────────────────────────
// THE COINALYZE CYCLE IS PACED
//
// The defect these cases hold: the Coinalyze path called the network in a nested loop, as fast
// as the token bucket would release, with no spacing and no notion of its own period. And
// `PollOnce` runs SYNCHRONOUSLY inside the select of the ticker that schedules it
// (`runCoinalyzeCycles`), so a cycle longer than the interval does not run late: the runtime
// DISCARDS the ticks that fall inside it.
//
// The arithmetic is the spec's, not a choice: 500 symbols ÷ 20 per request = 25 batches, × 3
// timeframes = 75 batches, × 3 endpoints × 20 symbols = 4,500 charged calls, against the 40 per
// minute the provider serves — 112 minutes against a 60-minute ticker.
//
// The venue path already had the mechanism (`admissionInterval` + drain reserve + floor report).
// These cases assert it was PORTED, not reinvented: same reserve, same second return saying the
// cadence is unreachable, and the same admission rendezvous — the source belongs to the test and
// the send only completes when the loop is parked in receive, which makes "no batch entered
// early" an exact statement instead of a timing guess.
// ─────────────────────────────────────────────────────────────────────────────

// announcingCoinalyzeFake serves the four paths and ANNOUNCES every history request at the instant
// it arrives.
//
// The announcement is what separates "the batch entered" from "the batch finished": a double that
// only stored requests would force the test to wait on the clock to conclude nothing else entered,
// and waiting on the clock is how a pacing test passes on both ends.
type announcingCoinalyzeFake struct {
	mu       sync.Mutex
	requests []*url.URL
	arrivals chan url.Values
}

func newAnnouncingCoinalyzeFake(t *testing.T) (*announcingCoinalyzeFake, *coinalyze.CoinalyzeClient) {
	t.Helper()

	fake := &announcingCoinalyzeFake{arrivals: make(chan url.Values, 64)}
	bodies := map[string]string{
		"/future-markets":           coinalyzeMarketsBody,
		"/open-interest-history":    coinalyzeOpenInterestBody,
		"/funding-rate-history":     coinalyzeFundingBody,
		"/long-short-ratio-history": coinalyzeLongShortBody,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := bodies[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		fake.mu.Lock()
		captured := *r.URL
		fake.requests = append(fake.requests, &captured)
		fake.mu.Unlock()

		// The catalog is NOT an admission: it runs once, before the plan, and counting it as a
		// batch would measure symbol resolution instead of the pace.
		if r.URL.Path != "/future-markets" {
			fake.arrivals <- r.URL.Query()
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	return fake, coinalyze.NewCoinalyzeClient("chave-de-teste", coinalyze.WithCoinalyzeBaseURL(server.URL))
}

// takeBatch receives a batch's three requests and returns the `interval` they all carry, proving
// WHICH admission unit they belong to.
func (f *announcingCoinalyzeFake) takeBatch(t *testing.T, ordinal int) string {
	t.Helper()

	interval := ""
	for i := 0; i < CoinalyzeCallsPerBatch; i++ {
		select {
		case query := <-f.arrivals:
			got := query.Get("interval")
			if interval == "" {
				interval = got
			}
			if got != interval {
				t.Fatalf("batch %d: request %d came with interval=%q and the previous ones with %q — "+
					"two admission units mixed in the same batch", ordinal, i, got, interval)
			}
		case <-time.After(testDeadline):
			t.Fatalf("batch %d: only %d of the %d requests arrived within %s",
				ordinal, i, CoinalyzeCallsPerBatch, testDeadline)
		}
	}
	return interval
}

// ─────────────────────────────────────────────────────────────────────────────
// The arithmetic
// ─────────────────────────────────────────────────────────────────────────────

func TestCoinalyzeAdmissionInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cycle       time.Duration
		batches     int
		want        time.Duration
		wantFloored bool
	}{
		{
			// THE PRODUCTION CASE. 75 batches on a one-hour ticker want one admission every
			// 45.6s; the budget only funds one full batch every 90s. The floor decides, and the
			// second return makes that audible.
			name:        "the production fleet does not fit the one-hour ticker, and the floor says so",
			cycle:       time.Hour,
			batches:     75,
			want:        minCoinalyzeAdmission,
			wantFloored: true,
		},
		{
			// The reserve is what separates this from a cycle that overruns by one latency:
			// without it the pace would be 3600s/2 and the last batch would enter with nothing
			// ahead of it.
			name:    "a small fleet keeps the same one-twentieth reserve",
			cycle:   time.Hour,
			batches: 2,
			want:    (time.Hour - time.Hour/coinalyzeCycleDrainFraction) / 2,
		},
		{
			// The exact boundary: 3420s ÷ 38 gives 90s, which IS the floor. It must not bind
			// here, otherwise the unreachable-cadence report accuses a cycle that fits.
			name:    "a cycle landing exactly on the floor is not reported as unreachable",
			cycle:   time.Hour,
			batches: 38,
			want:    minCoinalyzeAdmission,
		},
		{
			// An undeclared interval must not become a zero-period ticker, which panics — nor
			// "no spacing", which is the original defect.
			name:        "an undeclared interval falls to the floor, not to zero",
			cycle:       0,
			batches:     2,
			want:        minCoinalyzeAdmission,
			wantFloored: true,
		},
		{
			name:    "a cycle with no batches has nothing to pace",
			cycle:   time.Hour,
			batches: 0,
			want:    minCoinalyzeAdmission,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, floored := coinalyzeAdmissionInterval(tt.cycle, tt.batches)
			if got != tt.want {
				t.Errorf("coinalyzeAdmissionInterval(%s, %d) = %s, wanted %s", tt.cycle, tt.batches, got, tt.want)
			}
			if floored != tt.wantFloored {
				t.Errorf("coinalyzeAdmissionInterval(%s, %d) floored = %v, wanted %v",
					tt.cycle, tt.batches, floored, tt.wantFloored)
			}
			if got < minCoinalyzeAdmission {
				t.Errorf("coinalyzeAdmissionInterval(%s, %d) = %s, faster than the %s that the "+
					"budget of %d calls/min funds — that is a 429, not a performance choice",
					tt.cycle, tt.batches, got, minCoinalyzeAdmission, coinalyze.CoinalyzeCallsPerMinute)
			}
		})
	}
}

// TestCoinalyzePace_ReproducesTheCycleThatDoesNotFitItsTicker ties the mechanism to the spec's
// measurement: the derived pace must reproduce the 112 minutes, not an invented number.
func TestCoinalyzePace_ReproducesTheCycleThatDoesNotFitItsTicker(t *testing.T) {
	t.Parallel()

	const (
		fleet          = 500
		timeframes     = 3
		cycleInterval  = time.Hour
		medidoEmMinuto = 112*time.Minute + 30*time.Second // 4.500 ÷ 40 por minuto
	)

	batches := (fleet / coinalyze.CoinalyzeMaxSymbolsPerRequest) * timeframes
	calls := batches * CoinalyzeCallsPerBatch * coinalyze.CoinalyzeMaxSymbolsPerRequest
	if calls != 4500 {
		t.Fatalf("the production fleet costs %d symbol-calls, and the spec measures 4,500 — "+
			"the admission unit stopped being the batch", calls)
	}

	admission, floored := coinalyzeAdmissionInterval(cycleInterval, batches)
	if !floored {
		t.Fatalf("a cycle of %d batches in %s was reported as fitting the cadence; it costs %s",
			batches, cycleInterval, medidoEmMinuto)
	}

	span := admission * time.Duration(batches)
	if span != medidoEmMinuto {
		t.Errorf("the derived pace spreads the cycle over %s; the budget of %d calls/min puts it "+
			"at %s", span, coinalyze.CoinalyzeCallsPerMinute, medidoEmMinuto)
	}
	if span <= cycleInterval {
		t.Errorf("the derived cycle (%s) fits the %s interval — the spec's measurement says the opposite",
			span, cycleInterval)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The mechanism
// ─────────────────────────────────────────────────────────────────────────────

// TestCoinalyzePollOnce_AdmitsOneBatchPerTickAndNeverInABurst is the case that kills the mutant
// removing the spacing.
//
// Without pacing, the cycle fires both batches back to back and NEVER reaches admission receive —
// `pacer.tick` then fails on deadline, because the send is a rendezvous on an unbuffered channel.
// No clock wait decides anything here.
func TestCoinalyzePollOnce_AdmitsOneBatchPerTickAndNeverInABurst(t *testing.T) {
	fake, client := newAnnouncingCoinalyzeFake(t)
	pacer := newManualPacer()

	// TWO TIMEFRAMES = TWO BATCHES, the smallest cycle in which spacing is observable.
	poller := NewCoinalyzePoller(CoinalyzePollerOptions{
		Client:        client,
		Catalog:       coinalyze.NewCoinalyzeSymbolCatalog(coinalyze.CoinalyzeSymbolCatalogOptions{Client: client}),
		Budget:        &recordingSpender{},
		Store:         &recordingCoinalyzeStore{},
		Exchange:      "binance",
		MarketType:    "future",
		Symbols:       []string{"BTC/USDT"},
		Timeframes:    []string{"1h", "1d"},
		CycleInterval: time.Hour,
		Lookback:      72 * time.Hour,
		Now:           func() time.Time { return time.Unix(1787465684, 0).UTC() },
	})
	poller.newPacer = pacer.build

	done := make(chan error, 1)
	go func() { done <- poller.PollOnce(context.Background()) }()

	// THE FIRST BATCH ENTERS WITHOUT A TICK — the `1h` three quantities.
	if got := fake.takeBatch(t, 1); got != "1hour" {
		t.Fatalf("the first batch requested interval=%q, wanted \"1hour\" — the plan order changed", got)
	}
	// Nothing else may be waiting. A burst would have queued the second batch here.
	if extra := len(fake.arrivals); extra != 0 {
		t.Fatalf("after one admitted batch, %d more request(s) had already entered — "+
			"the cycle is bursting, not pacing", extra)
	}

	// THE SECOND BATCH ONLY ENTERS WITH THE TICK.
	pacer.tick(t)

	if got := fake.takeBatch(t, 2); got != "daily" {
		t.Fatalf("the second batch requested interval=%q, wanted \"daily\"", got)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("PollOnce: %v", err)
		}
	case <-time.After(testDeadline):
		t.Fatal("the cycle did not finish after the last batch")
	}

	// The mechanism is tied to the arithmetic: the pace requested is the derived one, not a number
	// the loop invented.
	wantPace, _ := coinalyzeAdmissionInterval(time.Hour, 2)
	if got := pacer.pace(); got != wantPace {
		t.Errorf("the cycle paced at %s, wanted the derived %s", got, wantPace)
	}
}

// TestCoinalyzePollOnce_CancellationStopsAdmissionWithoutBurningTheSeeding: cancellation is
// visible from inside the cycle, and it does NOT mark as seeded whoever was not collected.
//
// A cycle spans the whole period by design, so leaving the loop instead of returning the error
// would fall into `markSeeded` — and the symbol that never received the deep pass would be marked
// as if it had, the seeding burn through the new door.
func TestCoinalyzePollOnce_CancellationStopsAdmissionWithoutBurningTheSeeding(t *testing.T) {
	fake, client := newAnnouncingCoinalyzeFake(t)
	pacer := newManualPacer()

	poller := NewCoinalyzePoller(CoinalyzePollerOptions{
		Client:        client,
		Catalog:       coinalyze.NewCoinalyzeSymbolCatalog(coinalyze.CoinalyzeSymbolCatalogOptions{Client: client}),
		Budget:        &recordingSpender{},
		Store:         &recordingCoinalyzeStore{},
		Exchange:      "binance",
		MarketType:    "future",
		Symbols:       []string{"BTC/USDT"},
		Timeframes:    []string{"1h", "1d"},
		CycleInterval: time.Hour,
		Lookback:      72 * time.Hour,
		Now:           func() time.Time { return time.Unix(1787465684, 0).UTC() },
	})
	poller.newPacer = pacer.build

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- poller.PollOnce(ctx) }()

	fake.takeBatch(t, 1)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the cycle canceled during admission returned nil; it must return the cancellation")
		}
	case <-time.After(testDeadline):
		t.Fatal("cancellation was not seen from inside the cycle — the admission wait ignores it")
	}

	// Without a durable ledger (nil in this case), the new arguments are inert: the question falls
	// entirely to the process map, which is what this case measures.
	porSemear, jaSemeados := poller.splitBySeeding(
		context.Background(), nil, []string{"BTCUSDT_PERP.A"}, poller.now())
	if len(jaSemeados) != 0 {
		t.Fatalf("the symbol was marked as seeded in a cycle interrupted at the second batch: %v", jaSemeados)
	}
	if len(porSemear) != 1 {
		t.Fatalf("the symbol left the seeding queue: porSemear=%v", porSemear)
	}
}
