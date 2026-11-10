//go:build integration

// Gate G1 — the live mirror check.
//
//	go test -tags=integration -run Parity ./internal/parity/...
//
// Configured entirely by environment so the same binary runs against a local stack or
// against production read-only:
//
//	PARITY_STREAMER_URL   base URL of the streamer HTTP API   (default http://localhost:8080)
//	PARITY_BINANCE_URL    base URL of the Binance futures API (default https://fapi.binance.com)
//	PARITY_SYMBOLS        comma-separated, unified            (default BTC/USDT,ETH/USDT,SOL/USDT)
//	PARITY_TIMEFRAMES     comma-separated                     (default: all eight configured)
//	PARITY_LIMIT          buckets per pair                    (default 50)
//
// It is a READ-ONLY check: it issues GETs against a public exchange endpoint and our own
// read API, and writes nothing.
package parity

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// sampleFleet draws the symbols under test at random from the fleet the streamer is actually
// serving, and prints the seed so any failure it finds is reproducible.
//
// Deterministic-per-run rather than fixed: a fixed list stops finding new defects the moment
// it passes once, while a fresh draw each run turns the gate into a standing search. The seed
// is printed and PARITY_SEED overrides it, so a red run can be replayed exactly.
//
// Falls back to the three majors only when the fleet cannot be read — a harness that cannot
// enumerate the fleet must still run, but it must say that it is testing less.
func sampleFleet(streamerURL string) []string {
	if explicit := os.Getenv("PARITY_SYMBOLS"); explicit != "" {
		return envList("PARITY_SYMBOLS", "")
	}

	size, err := strconv.Atoi(envOr("PARITY_SAMPLE", "20"))
	if err != nil || size <= 0 {
		size = 20
	}
	seed, err := strconv.ParseInt(envOr("PARITY_SEED", "0"), 10, 64)
	if err != nil || seed == 0 {
		seed = time.Now().UnixNano()
	}

	fleet, err := fetchFleet(streamerURL)
	if err != nil || len(fleet) == 0 {
		fmt.Printf("PARITY: fleet unreadable (%v) — falling back to 3 majors, which tests the "+
			"EASIEST symbols in the fleet and must not be read as fleet coverage\n", err)
		return []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}
	}

	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(fleet), func(i, j int) { fleet[i], fleet[j] = fleet[j], fleet[i] })
	if size > len(fleet) {
		size = len(fleet)
	}
	drawn := fleet[:size]
	fmt.Printf("PARITY: sampled %d of %d fleet symbols, seed=%d (replay with PARITY_SEED=%d)\n",
		len(drawn), len(fleet), seed, seed)
	return drawn
}

// fetchFleet reads the symbols the streamer reports it is subscribed to.
//
// # THE ENVELOPE IS NESTED, AND GUESSING IT WRONG FAILS SILENTLY
//
// A flat `{symbols:[...]}` / `{subscriptions:[...]}` shape is not what the endpoint serves. It
// serves
//
//	{"total":503,"exchanges":{"binance":{"markets":{"future":{"symbols":[...]}}}}}
//
// and json.Unmarshal SUCCEEDS on a struct whose fields are all absent — unknown keys are
// ignored by default. So it returned an empty slice with a nil error, sampleFleet took the
// fallback, and the harness printed "fleet unreadable (<nil>)" while quietly testing the three
// majors. PARITY_SEED and PARITY_SAMPLE became knobs that did nothing against production.
//
// That is the repo's most-repeated defect — a path that reports success without doing the work —
// committed inside the gate that exists to catch it. Hence the explicit emptiness check below:
// parsing that yields no symbols is an ERROR, not an empty fleet.
func fetchFleet(streamerURL string) ([]string, error) {
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Get(streamerURL + "/api/subscriptions")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("subscriptions returned %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return parseFleet(raw)
}

// parseFleet extracts every subscribed symbol from the subscriptions envelope.
//
// Split from the HTTP call so the shape can be tested against a literal body — the defect it
// replaces was a parsing defect, and a test that needs a live streamer would never have run.
func parseFleet(raw []byte) ([]string, error) {
	var body struct {
		Exchanges map[string]struct {
			Markets map[string]struct {
				Symbols []string `json:"symbols"`
			} `json:"markets"`
		} `json:"exchanges"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}

	seen := make(map[string]struct{})
	var out []string
	for _, ex := range body.Exchanges {
		for _, mkt := range ex.Markets {
			for _, sym := range mkt.Symbols {
				if sym == "" {
					continue
				}
				if _, dup := seen[sym]; dup {
					continue
				}
				seen[sym] = struct{}{}
				out = append(out, sym)
			}
		}
	}
	if len(out) == 0 {
		// An envelope that parses to nothing is a SHAPE MISMATCH, not an empty fleet. Reporting it
		// as success would make the harness disappear in silence.
		return nil, fmt.Errorf("subscriptions parsed to zero symbols (envelope shape changed?)")
	}

	// Map iteration is randomised, so sort before returning: the draw must be a pure function of
	// (fleet, seed) or PARITY_SEED cannot replay a red run, which is its only purpose.
	sort.Strings(out)
	return out, nil
}

func envList(key, def string) []string {
	parts := strings.Split(envOr(key, def), ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// rawSymbol converts a unified symbol ("BTC/USDT" or "BTC/USDT:USDT") to the Binance id.
func rawSymbol(unified string) string {
	s := unified
	if i := strings.IndexByte(s, ':'); i >= 0 {
		s = s[:i]
	}
	return strings.ReplaceAll(s, "/", "")
}

// TestParity_MirrorsBinance is gate G1.
//
// THE VERDICT IS |WINDOW SUM| PLUS THE UNPAIRED BAR, NEVER A PER-BAR DELTA. See the criterion
// block at the top of parity.go: we reconstruct klines from aggTrades, an aggTrade carries one
// timestamp for a group of fills, and the group that crosses a bucket boundary is split by the
// exchange and not by us. ~4,2% of boundaries, adjacent pairs summing to zero, zero bias. A
// per-bar test therefore fails forever on a HEALTHY system, and a gate that always fails gets
// muted — which is how a real defect walks past it.
//
// It compares every configured symbol x timeframe and reports EVERY failing pair before
// failing, rather than stopping at the first: a partial mirror (one timeframe adrift) is a
// different diagnosis from a systemic one, and bailing early hides which it is.
func TestParity_MirrorsBinance(t *testing.T) {
	var (
		streamerURL = envOr("PARITY_STREAMER_URL", "http://localhost:8080")
		binanceURL  = envOr("PARITY_BINANCE_URL", "https://fapi.binance.com")
		// RANDOMLY DRAWN FROM THE LIVE FLEET, not hand-picked. The default was "BTC,ETH,SOL"
		// and I ran the gate with ten majors: 80/80, three times in a row, and reported that
		// as if it spoke about the system. A random sample of twenty symbols from the same
		// fleet, in the same minute, failed 4 of 40 pairs. Ten out of five hundred is 2% —
		// and it is the 2% with the most liquidity, most trades per bucket and least
		// reordering, that is, precisely where this defect has the least chance of appearing.
		//
		// It is the SAME mistake that the timeframe comment just below documents, made on the
		// other axis. Choosing the sample is choosing the result; the only defense is not choosing.
		//
		// PARITY_SYMBOLS is still honored when explicit — that is how a failure is reproduced
		// after the draw finds it.
		symbols = sampleFleet(envOr("PARITY_STREAMER_URL", "http://localhost:8080"))
		// ALL eight production timeframes — and "all" here is literal, verified against
		// config.yaml and against what /api/timeseries serves.
		//
		// The default was "5m,15m,1h": three out of eight. The gate passed 9/9 while 2h and
		// 4h failed 16/16 against the same production in the same minute. The first
		// correction raised it to seven and wrote "ALL" in the comment — leaving 1w out and
		// the claim false. A gate that chooses where to look reports the sample, not the
		// system; a comment that overstates coverage makes the reader stop checking.
		timeframes = envList("PARITY_TIMEFRAMES", "5m,15m,30m,1h,2h,4h,1d,1w")
	)
	limit, err := strconv.Atoi(envOr("PARITY_LIMIT", "50"))
	if err != nil || limit <= 0 {
		t.Fatalf("PARITY_LIMIT must be a positive integer, got %q", os.Getenv("PARITY_LIMIT"))
	}

	client := &http.Client{Timeout: 20 * time.Second}

	// THE BOOT INSTANT COMES FROM THE STREAMER, NOT FROM CONFIGURATION.
	//
	// It unlocks exactly one bounded exemption (see parity.CompareAt), and an exemption whose
	// scope a caller can dial is not an exemption, it is a switch for hiding whatever bar is
	// inconvenient today. Deriving it from the process's own uptime means the harness can only
	// excuse the bucket the process actually booted in.
	//
	// A streamer that cannot report its uptime yields a zero instant, which DISABLES the
	// exemption rather than widening it. Failing closed is the whole point: the strict rule is
	// the default and the exemption is the thing that must be earned.
	bootAt, err := FetchBootInstant(client, streamerURL)
	if err != nil {
		t.Logf("could not read the streamer's boot instant (%v) — the boot-bucket exemption is "+
			"DISABLED for this run; a bar in the boot bucket will fail like any other", err)
	}

	var failures []string
	// Counted with atomics: subtests may run in parallel, and a racy tally would make the
	// coverage line — the one thing that says whether the run measured anything — unreliable.
	var graded, inconclusive int64
	for _, sym := range symbols {
		for _, tf := range timeframes {
			t.Run(fmt.Sprintf("%s_%s", strings.ReplaceAll(sym, "/", ""), tf), func(t *testing.T) {
				theirs, err := FetchBinanceKlines(client, binanceURL, rawSymbol(sym), tf, limit)
				if err != nil {
					t.Fatalf("fetch binance: %v", err)
				}
				if len(theirs) == 0 {
					t.Fatalf("binance returned no closed klines for %s %s — the sample is empty, which is a harness failure, not a pass", sym, tf)
				}

				ours, err := FetchOurs(client, streamerURL, sym, tf, limit*2)
				if err != nil {
					t.Fatalf("fetch ours: %v", err)
				}

				rep := CompareAt(sym, tf, ours, theirs, bootAt)
				// Logged on PASS too. The straddle means a healthy window normally carries a
				// handful of divergent-but-paired bars; seeing that count is what tells an
				// operator the pairing is doing work rather than the window being trivially
				// quiet.
				t.Logf("%s %s: %s", sym, tf, rep.Summary())

				// INCONCLUSIVE IS NOT A VERDICT. A pair whose every bucket was seeded or excluded
				// says nothing about the system; counting it as a failure buried the real signal
				// (measured: 374 of 480 "failures" had compared=0 while the gradeable ones were
				// nearly clean), and counting it as a pass is how gate A6 published a fabricated
				// floor from an empty fetch.
				if rep.Inconclusive() {
					atomic.AddInt64(&inconclusive, 1)
					return
				}
				atomic.AddInt64(&graded, 1)

				if rep.Pass() {
					return
				}
				failures = append(failures, fmt.Sprintf("%s %s (%s)", sym, tf, rep.Summary()))
				for _, line := range rep.Failures() {
					t.Error(line)
				}
			})
		}
	}

	g, inc := atomic.LoadInt64(&graded), atomic.LoadInt64(&inconclusive)
	t.Logf("COVERAGE: %d pair(s) graded, %d inconclusive (nothing to compare)", g, inc)

	// A RUN THAT GRADED NOTHING REPORTS THAT, AND DOES NOT PRETEND EITHER WAY.
	//
	// Failing the run outright would be a PERMANENT red for 1d and 1w: their buckets only become
	// gradeable after the venue has built two of them, which is two days and two WEEKS of uptime. A
	// gate that can never go green gets muted, and a muted gate is not a gate — the same reasoning
	// that rewrote the per-bar criterion around the straddle.
	//
	// So a run whose pairs were all inconclusive SKIPS with the reason stated. A run that graded
	// nothing and had no inconclusive pairs either is a genuine harness failure — nothing was even
	// attempted — and still fails.
	if g == 0 {
		if inc > 0 {
			t.Skipf("nothing gradeable: all %d pair(s) hold only seeded buckets. The venue has not "+
				"yet built two buckets at this timeframe — 1d needs ~2 days of uptime and 1w ~2 "+
				"weeks. This is a COVERAGE fact, not a verdict about the system", inc)
		}
		t.Fatal("the run graded nothing AND had no inconclusive pairs — nothing was attempted, " +
			"which is a harness failure and must never report success")
	}

	if len(failures) > 0 {
		t.Errorf("G1 FAILED for %d pair(s):\n  %s", len(failures), strings.Join(failures, "\n  "))
	}
}
