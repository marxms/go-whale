//go:build integration

// Gate A6 — the tape-conservation check.
//
//	TAPE_SYMBOLS="BTC/USDT,ETH/USDT" TAPE_TIMEFRAME=5m TAPE_BUCKETS=6 \
//	  go test -tags=integration -count=1 -run TapeConservation -v ./internal/parity/
//
// It reads two PUBLIC Binance endpoints and nothing of ours. Our streamer need not be running.
package parity

import (
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestTapeConservation_BucketAssignmentAgreesWithBinance decides a question this codebase has
// been ASSUMING the answer to since the G1 criterion was written.
//
// The package doc asserts that an aggTrade group straddling a bucket boundary is displaced whole,
// that it happens on ~4,2% of boundaries, and that "no amount of code removes it; it is the
// resolution limit of the input". WindowTolerance exists because of that assertion. It has never
// been measured directly — and an untested tolerance is indistinguishable from an off-by-one that
// nobody looked at.
//
// Here both sides of the comparison come from Binance:
//
//	ours-by-rule := Σ q over the raw tape, bucketed with candles.BucketTimestamp
//	theirs       := the kline Binance publishes for that same bucket
//
// A difference cannot be our ingestion, because our ingestion is not in the comparison. It can
// only be the ASSIGNMENT RULE. So:
//
//	all buckets equal → the straddle premise is FALSE. Binance buckets by trade time exactly as
//	                    we do, no group is displaced, and every G1 divergence is our own defect
//	                    and therefore fixable inside our code.
//	some differ       → the premise is TRUE and now MEASURED rather than assumed. The residual is
//	                    the real floor, and a digit-exact mirror is unreachable by aggregating the
//	                    tape ourselves at any level of effort.
//
// Both outcomes are valuable and the test reports either without failing the build: it is an
// INSTRUMENT, not a gate. Failing here would say "Binance disagrees with itself", which is not a
// defect anyone can act on.
func TestTapeConservation_BucketAssignmentAgreesWithBinance(t *testing.T) {
	var (
		binanceURL = envOr("PARITY_BINANCE_URL", "https://fapi.binance.com")
		timeframe  = envOr("TAPE_TIMEFRAME", "5m")
		symbols    = envList("TAPE_SYMBOLS", "BTC/USDT,ETH/USDT,SOL/USDT")
	)
	nBuckets, err := strconv.Atoi(envOr("TAPE_BUCKETS", "6"))
	if err != nil || nBuckets <= 0 {
		t.Fatalf("TAPE_BUCKETS must be a positive integer, got %q", os.Getenv("TAPE_BUCKETS"))
	}
	client := &http.Client{Timeout: 30 * time.Second}

	var agree, differ int
	var maxRel float64
	for _, symbol := range symbols {
		raw := strings.ReplaceAll(symbol, "/", "")

		// Skip the newest bucket: it is still open, so its kline is a partial the tape would
		// legitimately outrun. Comparing an open bucket would manufacture a disagreement.
		klines, err := FetchBinanceKlines(client, binanceURL, raw, timeframe, nBuckets+2)
		if err != nil {
			t.Fatalf("%s klines: %v", symbol, err)
		}
		if len(klines) < 2 {
			t.Fatalf("%s: need at least 2 klines, got %d", symbol, len(klines))
		}
		klines = klines[:len(klines)-1]
		if len(klines) > nBuckets {
			klines = klines[len(klines)-nBuckets:]
		}

		step := bucketStep(klineTimestamps(klines))
		if step <= 0 {
			t.Fatalf("%s: could not infer bucket width", symbol)
		}
		from, to := klines[0].Timestamp, klines[len(klines)-1].Timestamp+step

		trades, err := FetchAggTrades(client, binanceURL, raw, from, to, 400)
		if err != nil {
			t.Fatalf("%s tape: %v", symbol, err)
		}

		// THE PRECONDITION, ASSERTED AGAINST THE EXCHANGE'S OWN ANSWER.
		//
		// An empty tape is only legitimate when the exchange also says nothing traded. Before this
		// check, a fetch that found nothing produced a full report — every bucket "displaced", a
		// displacement rate, and an irreducible floor of 0 — and the test PASSED. Both headline
		// outputs of this instrument were fabricable by a fetch artifact.
		var tradedBuckets int
		for _, k := range klines {
			if k.Volume > 0 {
				tradedBuckets++
			}
		}
		if len(trades) == 0 {
			if tradedBuckets > 0 {
				t.Fatalf("%s: the tape came back EMPTY while the exchange reports volume in %d of "+
					"%d buckets. That is a FETCH ARTEFACT, not a measurement — comparing it would "+
					"fabricate a displacement rate and a floor of zero", symbol, tradedBuckets, len(klines))
			}
			t.Logf("%s %s: nothing traded in the window on either side — skipped", symbol, timeframe)
			continue
		}
		if ok, missing := TapeIsContiguous(trades); !ok {
			t.Fatalf("%s: fetched tape has a hole at id %d — the MEASUREMENT is incomplete and "+
				"comparing it would report a fetch artifact as a convention disagreement", symbol, missing)
		}
		t.Logf("%s %s: %d aggTrades over %d buckets [%s .. %s]", symbol, timeframe, len(trades),
			len(klines), time.UnixMilli(from).UTC().Format(time.RFC3339),
			time.UnixMilli(to).UTC().Format(time.RFC3339))

		folded := SumTapeByBucket(trades, timeframe)
		for _, k := range klines {
			if k.Volume == 0 {
				continue // untraded interval: nothing to compare, and not a displacement
			}
			b, seen := folded[k.Timestamp]
			if !seen {
				// The exchange says this bucket traded and our fetch of ITS OWN tape produced
				// nothing for it. That is incompleteness in the measurement, never a fact about
				// bucket assignment, and counting it as displacement is how the floor got faked.
				t.Fatalf("%s bucket %s: the exchange reports volume %.8f and the tape folded "+
					"NOTHING into it — the fetch is incomplete, not the assignment rule",
					symbol, time.UnixMilli(k.Timestamp).UTC().Format(time.RFC3339), k.Volume)
			}
			// The tape is the sum of individual quantities; the kline is Binance's own total.
			// Only float summation noise is tolerated — nothing structural.
			//
			// LOGGED, NOT FAILED. A disagreement here is Binance disagreeing with Binance about
			// where a boundary quantity belongs; no change to our code makes it go away, so
			// failing on it would produce a permanently red check, and a permanently red check
			// gets muted. The MAGNITUDE is the output that matters — see the verdict below.
			if rel := relErr(b.Volume, k.Volume); rel > 1e-9 {
				differ++
				maxRel = math.Max(maxRel, rel)
				t.Logf("%s bucket %s: DISPLACED  tape=%.10f kline=%.10f  delta=%+.10f rel=%.3g "+
					"(trades %d, ids %d..%d)",
					symbol, time.UnixMilli(k.Timestamp).UTC().Format(time.RFC3339),
					b.Volume, k.Volume, b.Volume-k.Volume, rel, b.Trades, b.FirstID, b.LastID)
				continue
			}
			agree++
			if rel := relErr(b.TakerBuy, k.TakerBuyVolume); rel > 1e-9 {
				t.Logf("%s bucket %s: volume agrees but the TAKER SPLIT does not — "+
					"tapeBuy=%.10f klineBuy=%.10f rel=%.3g (the maker-flag convention, not assignment)",
					symbol, time.UnixMilli(k.Timestamp).UTC().Format(time.RFC3339),
					b.TakerBuy, k.TakerBuyVolume, rel)
			}
		}
	}

	total := agree + differ
	if total == 0 {
		t.Fatal("no buckets compared — the run proves nothing")
	}
	t.Logf("VERDICT: %d/%d buckets agree with Binance's own assignment (%.1f%% displaced), "+
		"worst displacement %.3g relative",
		agree, total, 100*float64(differ)/float64(total), maxRel)

	// THE NUMBER THIS TEST EXISTS TO PRODUCE.
	//
	// The displacement rate alone settles nothing — what decides whether our own accounting can
	// ever be exact is its SIZE. Anything G1 reports well above this floor is ours.
	if differ == 0 {
		t.Logf("No displacement on this sample: on this evidence every G1 divergence is OURS.")
		return
	}
	t.Logf("IRREDUCIBLE FLOOR = %.3g relative. A G1 divergence materially larger than this "+
		"CANNOT be the boundary convention and is therefore ours to fix.", maxRel)
}

func klineTimestamps(cs []Candle) []int64 {
	out := make([]int64, len(cs))
	for i, c := range cs {
		out[i] = c.Timestamp
	}
	return out
}

func relErr(a, b float64) float64 {
	if b == 0 {
		if a == 0 {
			return 0
		}
		return math.Inf(1)
	}
	return math.Abs(a-b) / math.Abs(b)
}
