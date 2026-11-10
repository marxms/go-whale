//go:build integration

// Integrity checks I1, I2, I3 and I6 from the derivatives change, run against the
// live venue.
//
// They are tagged `integration` because they need the real endpoint: what they verify
// is not our arithmetic — that is covered by unit tests — but the CONTRACT between our
// assumptions and what the venue actually serves. A fixture cannot fail when the venue
// changes its behavior, and these exist precisely to notice that.
//
//	go test -tags=integration -run TestI[0-9] ./internal/derivatives/collector/...
//
// Rate limit: the IP budget is 2400 weight/minute and a previous sweep tripped it with
// a few dozen rapid calls. Every request here is paced, and the samples are small on
// purpose — an honest result over three symbols beats a 429 over sixty.
package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"testing"
	"time"
)

const (
	fapiBase = "https://fapi.binance.com"
	// pacing keeps the sweep well inside the weight budget.
	pacing = 400 * time.Millisecond
)

// integritySymbols is a small, fixed cohort spanning liquidity tiers. Fixed rather
// than sampled: a check that fails on a different symbol each run cannot be triaged.
var integritySymbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}

type oiHistPoint struct {
	SumOpenInterest      string `json:"sumOpenInterest"`
	SumOpenInterestValue string `json:"sumOpenInterestValue"`
	Timestamp            int64  `json:"timestamp"`
}

func (p oiHistPoint) contracts() (float64, error) { return parseFloat(p.SumOpenInterest) }
func (p oiHistPoint) notional() (float64, error)  { return parseFloat(p.SumOpenInterestValue) }

func parseFloat(s string) (float64, error) {
	var f float64
	if _, err := fmt.Sscanf(s, "%g", &f); err != nil {
		return 0, fmt.Errorf("parsing %q: %w", s, err)
	}
	return f, nil
}

// kline is the subset of the venue's candle we need: open time, close price, and the
// base-asset volume that the accounting invariant is measured against.
type kline struct {
	OpenTimeMs int64
	Close      float64
	BaseVolume float64
}

func getJSON(ctx context.Context, t *testing.T, path string, params url.Values, out any) {
	t.Helper()
	time.Sleep(pacing)

	endpoint := fapiBase + path + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatalf("building request for %s: %v", path, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Skipf("venue unreachable (%s): %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s returned %d: %s", path, resp.StatusCode, string(body))
	}
	if err := json.Unmarshal(body, out); err != nil {
		t.Fatalf("decoding %s: %v", path, err)
	}
}

func fetchOIHistory(ctx context.Context, t *testing.T, symbol, period string, limit int) []oiHistPoint {
	t.Helper()
	var out []oiHistPoint
	getJSON(ctx, t, "/futures/data/openInterestHist", url.Values{
		"symbol": {symbol}, "period": {period}, "limit": {fmt.Sprint(limit)},
	}, &out)
	return out
}

func fetchKlines(ctx context.Context, t *testing.T, symbol, interval string, limit int) []kline {
	t.Helper()
	var raw [][]any
	getJSON(ctx, t, "/fapi/v1/klines", url.Values{
		"symbol": {symbol}, "interval": {interval}, "limit": {fmt.Sprint(limit)},
	}, &raw)

	out := make([]kline, 0, len(raw))
	for _, row := range raw {
		if len(row) < 6 {
			continue
		}
		openTime, ok := row[0].(float64)
		if !ok {
			continue
		}
		closeStr, ok := row[4].(string)
		if !ok {
			continue
		}
		volStr, ok := row[5].(string)
		if !ok {
			continue
		}
		closePx, err := parseFloat(closeStr)
		if err != nil {
			continue
		}
		vol, err := parseFloat(volStr)
		if err != nil {
			continue
		}
		out = append(out, kline{OpenTimeMs: int64(openTime), Close: closePx, BaseVolume: vol})
	}
	return out
}

// TestI1_OpenInterestJoinsTheCandleThatClosedAtItsTimestamp is integrity check I1.
//
// It decides the join WITHOUT a correlation argument, which is what made the earlier
// attempt circular. The notional field is contracts times price by identity, so
// dividing the two recovers the price the venue used — and that implied price is a
// CLOCK: whichever candle's close it matches is the candle the record belongs to.
//
// Criterion from the spec: the median relative error at the correct lag must be at
// most 1e-4, and at least ten times smaller than at the neighboring lag.
func TestI1_OpenInterestJoinsTheCandleThatClosedAtItsTimestamp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const period = "5m"

	for _, symbol := range integritySymbols {
		t.Run(symbol, func(t *testing.T) {
			oi := fetchOIHistory(ctx, t, symbol, period, 60)
			candles := fetchKlines(ctx, t, symbol, period, 200)
			if len(oi) < 10 || len(candles) < 10 {
				t.Skipf("not enough data returned (oi=%d candles=%d)", len(oi), len(candles))
			}

			closeByOpenTime := make(map[int64]float64, len(candles))
			periodMs := int64(5 * 60 * 1000)
			for _, c := range candles {
				closeByOpenTime[c.OpenTimeMs] = c.Close
			}

			// lag -1 is the candle that CLOSED at the record's timestamp; lag 0 is the
			// candle that OPENED at it, which is the join the spec forbids.
			errorsByLag := map[int][]float64{}
			for _, point := range oi {
				contracts, err := point.contracts()
				if err != nil || contracts <= 0 {
					continue
				}
				notional, err := point.notional()
				if err != nil {
					continue
				}
				implied := notional / contracts

				for _, lag := range []int{-2, -1, 0, 1} {
					openTime := point.Timestamp + int64(lag)*periodMs
					closePx, ok := closeByOpenTime[openTime]
					if !ok || closePx <= 0 {
						continue
					}
					errorsByLag[lag] = append(errorsByLag[lag], math.Abs(implied-closePx)/closePx)
				}
			}

			correct, ok := medianOf(errorsByLag[-1])
			if !ok {
				t.Skip("no overlapping window between the two feeds")
			}
			t.Logf("median relative error by lag: %s", formatLagErrors(errorsByLag))

			if correct > 1e-4 {
				t.Errorf("join at the closing candle has median error %.2e, want <= 1e-4", correct)
			}
			for _, lag := range []int{-2, 0, 1} {
				neighbor, ok := medianOf(errorsByLag[lag])
				if !ok {
					continue
				}
				if neighbor < correct*10 {
					t.Errorf("lag %d is not clearly worse than the closing candle "+
						"(%.2e vs %.2e) — the join is not identifiable from the data",
						lag, neighbor, correct)
				}
			}
		})
	}
}

// TestI2_OpenInterestChangeNeverExceedsTradedVolume is integrity check I2.
//
// Every contract opened or closed passed through a trade, so the change in open
// interest cannot exceed the volume traded in the same bucket. It is a coarse alarm
// by design: it catches a unit swap and a collection hole, not a ten-percent error.
//
// The criterion is POOLED, and that is deliberate — healthy symbols do violate it
// individually at the margin, so a per-symbol gate would fail on correct data.
func TestI2_OpenInterestChangeNeverExceedsTradedVolume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const period = "5m"
	var pairs, violations int

	for _, symbol := range integritySymbols {
		oi := fetchOIHistory(ctx, t, symbol, period, 60)
		candles := fetchKlines(ctx, t, symbol, period, 200)
		if len(oi) < 10 || len(candles) < 10 {
			continue
		}

		volumeByOpenTime := make(map[int64]float64, len(candles))
		for _, c := range candles {
			volumeByOpenTime[c.OpenTimeMs] = c.BaseVolume
		}
		periodMs := int64(5 * 60 * 1000)

		sort.Slice(oi, func(i, j int) bool { return oi[i].Timestamp < oi[j].Timestamp })
		for i := 1; i < len(oi); i++ {
			prev, err := oi[i-1].contracts()
			if err != nil {
				continue
			}
			curr, err := oi[i].contracts()
			if err != nil {
				continue
			}
			// The bucket that produced the change is the one that CLOSED at the newer
			// timestamp — the same join I1 establishes.
			vol, ok := volumeByOpenTime[oi[i].Timestamp-periodMs]
			if !ok || vol <= 0 {
				continue
			}
			pairs++
			if math.Abs(curr-prev) > vol {
				violations++
			}
		}
	}

	if pairs == 0 {
		t.Skip("no comparable pairs returned")
	}
	rate := float64(violations) / float64(pairs)
	t.Logf("pooled violations: %d/%d = %.4f%%", violations, pairs, rate*100)

	if rate > 0.0005 {
		t.Errorf("pooled violation rate %.4f%% exceeds 0.05%% — the collected quantity "+
			"is probably not in contracts", rate*100)
	}
}

// TestI3_NotionalBreaksTheAccountingInvariant is integrity check I3, the unit canary.
//
// It asserts a FAILURE: run the same invariant against the notional field and it has
// to break, because notional carries price and price moves without anyone trading.
// If this check ever passes, somebody swapped the unit back and I2 stopped meaning
// anything — a green I2 over notional would be a green light over the exact defect
// the contract quantity exists to avoid.
func TestI3_NotionalBreaksTheAccountingInvariant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const period = "5m"
	var pairs, violations int

	for _, symbol := range integritySymbols {
		oi := fetchOIHistory(ctx, t, symbol, period, 60)
		candles := fetchKlines(ctx, t, symbol, period, 200)
		if len(oi) < 10 || len(candles) < 10 {
			continue
		}

		volumeByOpenTime := make(map[int64]float64, len(candles))
		for _, c := range candles {
			volumeByOpenTime[c.OpenTimeMs] = c.BaseVolume
		}
		periodMs := int64(5 * 60 * 1000)

		sort.Slice(oi, func(i, j int) bool { return oi[i].Timestamp < oi[j].Timestamp })
		for i := 1; i < len(oi); i++ {
			prev, err := oi[i-1].notional()
			if err != nil {
				continue
			}
			curr, err := oi[i].notional()
			if err != nil {
				continue
			}
			vol, ok := volumeByOpenTime[oi[i].Timestamp-periodMs]
			if !ok || vol <= 0 {
				continue
			}
			pairs++
			if math.Abs(curr-prev) > vol {
				violations++
			}
		}
	}

	if pairs == 0 {
		t.Skip("no comparable pairs returned")
	}
	rate := float64(violations) / float64(pairs)
	t.Logf("notional violation rate: %d/%d = %.2f%%", violations, pairs, rate*100)

	// The measured signature is around 13%; the exact magnitude tracks the window's
	// volatility, so the assertion is qualitative — it must be VISIBLY broken, not
	// merely above the contracts threshold.
	if rate <= 0.01 {
		t.Errorf("notional passed the contracts invariant at %.2f%% — either the field "+
			"changed meaning or the comparison is no longer against base-asset volume; "+
			"either way I2 has stopped discriminating", rate*100)
	}
}

// TestI6_CoarsePeriodIsAnExactSubsampleOfTheFine is integrity check I6.
//
// The venue returns the SAME open-interest value for `period=1h` as for `period=5m`
// at a coinciding timestamp. The whole timeframe menu rests on that: it is why one
// collection feeds every scale, and why a window wider than one bar is forbidden.
//
// It is observed behavior and NOT documented, so it can change without notice. That
// is exactly why it is a regression check rather than an assumption in a comment.
func TestI6_CoarsePeriodIsAnExactSubsampleOfTheFine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	for _, symbol := range integritySymbols {
		t.Run(symbol, func(t *testing.T) {
			fine := fetchOIHistory(ctx, t, symbol, "5m", 200)
			coarse := fetchOIHistory(ctx, t, symbol, "1h", 20)
			if len(fine) < 12 || len(coarse) < 2 {
				t.Skipf("not enough data (fine=%d coarse=%d)", len(fine), len(coarse))
			}

			fineByTs := make(map[int64]float64, len(fine))
			for _, p := range fine {
				if v, err := p.contracts(); err == nil {
					fineByTs[p.Timestamp] = v
				}
			}

			var compared int
			for _, p := range coarse {
				coarseVal, err := p.contracts()
				if err != nil {
					continue
				}
				fineVal, ok := fineByTs[p.Timestamp]
				if !ok {
					continue
				}
				compared++
				if fineVal == 0 {
					t.Errorf("fine series reports zero at %d while coarse reports %g", p.Timestamp, coarseVal)
					continue
				}
				if rel := math.Abs(coarseVal-fineVal) / fineVal; rel > 1e-12 {
					t.Errorf("coarse period is not an exact subsample at %d: "+
						"1h=%g 5m=%g (relative %.2e) — the timeframe menu is no longer "+
						"one object at several scales", p.Timestamp, coarseVal, fineVal, rel)
				}
			}
			if compared == 0 {
				t.Skip("no coinciding timestamps between the two periods")
			}
			t.Logf("compared %d coinciding timestamps", compared)
		})
	}
}

func medianOf(values []float64) (float64, bool) {
	if len(values) == 0 {
		return 0, false
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2], true
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2, true
}

func formatLagErrors(byLag map[int][]float64) string {
	out := ""
	for _, lag := range []int{-2, -1, 0, 1} {
		if med, ok := medianOf(byLag[lag]); ok {
			out += fmt.Sprintf("lag%+d=%.2e ", lag, med)
		}
	}
	return out
}
