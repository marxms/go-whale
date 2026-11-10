package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
)

const (
	batchFixtureTs int64 = 1_700_000_100_000
	batchFixtureTF int64 = 30 * 60 * 1000
)

type boundedBatchStore struct {
	mockDerivativesStore
	badSymbol, failStage        string
	blockAll, empty, noBaseline bool
	entered                     chan string
	healthyDone                 chan string
	active, max                 atomic.Int64
}

func (s *boundedBatchStore) GetSnapshotSeries(ctx context.Context, _, _, symbol, _ string, _ int) ([]*derivatives.DerivativesSnapshot, error) {
	n := s.active.Add(1)
	defer s.active.Add(-1)
	for old := s.max.Load(); n > old; old = s.max.Load() {
		if s.max.CompareAndSwap(old, n) {
			break
		}
	}
	if s.entered != nil {
		s.entered <- symbol
	}
	if s.blockAll || (symbol == s.badSymbol && s.failStage == "blocked") {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(6 * time.Second):
		}
	}
	if symbol == s.badSymbol && s.failStage == "series" {
		return nil, errors.New("redis://secretpass@unreachable")
	}
	if s.empty {
		return nil, nil
	}
	return oiPair(batchFixtureTs, batchFixtureTF, 102, 100), nil
}

func (s *boundedBatchStore) GetBaseline(ctx context.Context, _, _, symbol, _ string) (*derivatives.DerivativesBaseline, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if symbol == s.badSymbol && s.failStage == "baseline" {
		return nil, errors.New("redis://secretpass@unreachable")
	}
	if s.noBaseline {
		return nil, nil
	}
	return &derivatives.DerivativesBaseline{
		WindowN: 288, NMin: 30,
		OpenInterest: &derivatives.Baseline{MAD: 0.01, N: 288}, Price: &derivatives.Baseline{MAD: 0.004, N: 288},
	}, nil
}

func (s *boundedBatchStore) PriceIncrement(ctx context.Context, _, _, symbol, _ string, _ int64) (*float64, string, error) {
	if symbol == s.badSymbol && s.failStage == "price_blocked" {
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-time.After(6 * time.Second):
		}
	}
	if symbol == s.badSymbol && s.failStage == "price" {
		return nil, "", errors.New("redis://secretpass@unreachable")
	}
	v := 0.007
	if s.healthyDone != nil {
		s.healthyDone <- symbol
	}
	return &v, "", nil
}

type batchWireReading struct {
	NormalizedReading
	UnavailableReason string `json:"unavailableReason"`
}

type boundedBatchWire struct {
	Data       map[string][]BatchDerivativesSeriesEntry `json:"data"`
	Normalized map[string]batchWireReading              `json:"normalized"`
}

func normalizedBatchRequest(ctx context.Context, t *testing.T, s *boundedBatchStore, symbols string) boundedBatchWire {
	t.Helper()
	return decodeNormalizedBatch(t, normalizedBatchRecord(ctx, s, symbols))
}

func normalizedBatchRecord(ctx context.Context, s *boundedBatchStore, symbols string) *httptest.ResponseRecorder {
	h := newMarketDataHandlerWith(s, "binance", "future", candle.NewTimeframeRegistry([]string{"30m"}))
	h.nowMs = func() int64 { return batchFixtureTs + 2*batchFixtureTF }
	r := httptest.NewRequestWithContext(ctx, "GET", "/api/batch/derivatives-series?timeframe=30m&limit=3&symbols="+symbols, nil)
	w := httptest.NewRecorder()
	h.GetBatchDerivativesSeries(w, r)
	return w
}

func decodeNormalizedBatch(t *testing.T, w *httptest.ResponseRecorder) boundedBatchWire {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("HTTP status=%d body=%s", w.Code, w.Body.String())
	}
	var got boundedBatchWire
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func assertBatchHealthy(t *testing.T, r batchWireReading) {
	t.Helper()
	want := math.Log(102.0 / 100.0)
	if r.UnavailableReason != "" || r.Timestamp != batchFixtureTs || r.WindowN != 288 ||
		r.OpenInterest.Value == nil || math.Abs(*r.OpenInterest.Value-want) > 1e-12 ||
		r.OpenInterest.Z == nil || math.Abs(*r.OpenInterest.Z-want/(1.4826*0.01)) > 1e-12 ||
		r.OpenInterest.State != derivatives.BaselineMature || r.OpenInterest.N != 288 || r.OpenInterest.NMin != 30 ||
		r.Price.Value == nil || *r.Price.Value != 0.007 || r.Price.Z == nil || math.Abs(*r.Price.Z-0.007/(1.4826*0.004)) > 1e-12 {
		t.Fatalf("healthy reading changed: %+v", r)
	}
}

func TestNormalizedBatchIsolatesOneStalledSymbolWithinFourSeconds(t *testing.T) {
	s := &boundedBatchStore{badSymbol: "BAD/USDT", failStage: "blocked", healthyDone: make(chan string, 2)}
	started := time.Now()
	done := make(chan *httptest.ResponseRecorder, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { done <- normalizedBatchRecord(ctx, s, "BAD/USDT,BTC/USDT,SOL/USDT") }()
	fast := time.NewTimer(100 * time.Millisecond)
	defer fast.Stop()
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case symbol := <-s.healthyDone:
			if symbol != "BTC/USDT" && symbol != "SOL/USDT" {
				t.Fatalf("unexpected healthy symbol %s", symbol)
			}
			seen[symbol] = true
		case <-fast.C:
			t.Fatal("healthy reads did not finish within 100ms while bad symbol stalled")
		}
	}
	var record *httptest.ResponseRecorder
	select {
	case record = <-done:
	case <-time.After(4*time.Second - time.Since(started)):
		t.Fatal("normalized batch did not finish within four seconds")
	}
	got := decodeNormalizedBatch(t, record)
	if elapsed := time.Since(started); elapsed >= 4*time.Second {
		t.Fatalf("whole fleet retained by slow symbol: %v", elapsed)
	}
	for _, symbol := range []string{"BTC/USDT", "SOL/USDT"} {
		assertBatchHealthy(t, got.Normalized[symbol])
	}
	bad, ok := got.Normalized["BAD/USDT"]
	if !ok || bad.UnavailableReason != "deadline_exceeded" || bad.Timestamp != 0 || bad.OpenInterest.Value != nil || bad.OpenInterest.Z != nil || bad.Price.Value != nil || bad.Price.Z != nil {
		t.Fatalf("deadline manufactured or omitted measurement: %+v", bad)
	}
	if s.active.Load() != 0 || len(got.Data) != 2 {
		t.Fatalf("worker/read payload not drained: active=%d data=%+v", s.active.Load(), got.Data)
	}
}

func TestNormalizedBatchBoundsEightReadersAndJoinsOnCancel(t *testing.T) {
	s := &boundedBatchStore{blockAll: true, entered: make(chan string, 40)}
	var symbols []string
	for i := 0; i < 40; i++ {
		symbols = append(symbols, fmt.Sprintf("ASSET%d/USDT", i))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- normalizedBatchRecord(ctx, s, strings.Join(symbols, ",")) }()
	for i := 0; i < 8; i++ {
		select {
		case <-s.entered:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("eight bounded readers never started")
		}
	}
	select {
	case <-s.entered:
		cancel()
		t.Fatal("ninth reader admitted beyond bound")
	case <-time.After(20 * time.Millisecond):
	}
	if s.max.Load() != 8 {
		t.Fatalf("reader count=%d", s.max.Load())
	}
	cancel()
	select {
	case record := <-done:
		got := decodeNormalizedBatch(t, record)
		if s.active.Load() != 0 || len(got.Data) != 0 || len(got.Normalized) != 40 {
			t.Fatalf("cancel not joined/declared: active=%d data=%d normalized=%d", s.active.Load(), len(got.Data), len(got.Normalized))
		}
		for symbol, r := range got.Normalized {
			if r.UnavailableReason != "canceled" || r.Timestamp != 0 || r.OpenInterest.Z != nil {
				t.Fatalf("%s cancel fabricated reading: %+v", symbol, r)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("cooperative readers not joined on cancellation")
	}
}

func TestNormalizedBatchDeclaresTransportFailuresWithoutErasingIndependentAxes(t *testing.T) {
	for _, stage := range []string{"series", "baseline", "price"} {
		t.Run(stage, func(t *testing.T) {
			s := &boundedBatchStore{badSymbol: "BAD/USDT", failStage: stage}
			got := normalizedBatchRequest(context.Background(), t, s, "BAD/USDT,BTC/USDT")
			assertBatchHealthy(t, got.Normalized["BTC/USDT"])
			r, ok := got.Normalized["BAD/USDT"]
			if !ok || r.UnavailableReason != stage+"_unavailable" {
				t.Fatalf("missing transport cause: %+v", r)
			}
			switch stage {
			case "series":
				if r.Timestamp != 0 || r.OpenInterest.Z != nil || r.Price.Z != nil || len(got.Data) != 1 {
					t.Fatalf("series failure fabricated record: %+v", r)
				}
			case "baseline":
				if r.OpenInterest.Z != nil || r.Price.Z != nil || r.OpenInterest.Reason != baselineUnreadable || len(got.Data) != 2 {
					t.Fatalf("baseline failure dropped series: %+v", r)
				}
			case "price":
				if r.Price.Value != nil || r.Price.Z != nil || r.OpenInterest.Z == nil || len(got.Data) != 2 {
					t.Fatalf("independent OI erased: %+v", r)
				}
			}
		})
	}
}

func TestNormalizedBatchKeepsDomainAbsenceDistinctFromTransport(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			s := &boundedBatchStore{empty: empty, noBaseline: true}
			got := normalizedBatchRequest(context.Background(), t, s, "BTC/USDT")
			if empty {
				if len(got.Data) != 0 || len(got.Normalized) != 0 {
					t.Fatalf("empty domain fabricated reading: %+v", got)
				}
				return
			}
			r := got.Normalized["BTC/USDT"]
			if r.UnavailableReason != "" || r.OpenInterest.Z != nil || r.OpenInterest.State != derivatives.BaselineImmature || r.OpenInterest.Reason != "no baseline has been computed for this (symbol, timeframe) yet" {
				t.Fatalf("legitimate absence became transport: %+v", r)
			}
		})
	}
}

func TestNormalizedBatchStalledPricePreservesHealthyOpenInterest(t *testing.T) {
	s := &boundedBatchStore{badSymbol: "BAD/USDT", failStage: "price_blocked"}
	started := time.Now()
	got := normalizedBatchRequest(context.Background(), t, s, "BAD/USDT,BTC/USDT")
	if time.Since(started) >= 4*time.Second {
		t.Fatal("stalled price exceeded mini-series budget")
	}
	assertBatchHealthy(t, got.Normalized["BTC/USDT"])
	r := got.Normalized["BAD/USDT"]
	want := math.Log(102.0 / 100.0)
	if r.UnavailableReason != "deadline_exceeded" || r.Timestamp != batchFixtureTs ||
		r.Price.Value != nil || r.Price.Z != nil || r.OpenInterest.Value == nil ||
		math.Abs(*r.OpenInterest.Value-want) > 1e-12 || r.OpenInterest.Z == nil ||
		math.Abs(*r.OpenInterest.Z-want/(1.4826*0.01)) > 1e-12 || r.OpenInterest.State != derivatives.BaselineMature || len(got.Data) != 2 {
		t.Fatalf("price deadline erased independent OI: %+v", r)
	}
}

func TestNormalizedBatchRecordsScopedFailureWithoutCredentials(t *testing.T) {
	if os.Getenv("STREAMER_TEST_BATCH_OBSERVATION") == "1" {
		for _, stage := range []string{"series", "baseline", "price", "blocked"} {
			normalizedBatchRequest(context.Background(), t, &boundedBatchStore{badSymbol: "BAD/USDT", failStage: stage}, "BAD/USDT")
		}
		return
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestNormalizedBatchRecordsScopedFailureWithoutCredentials$", "-test.count=1") //nolint:gosec // G204: test harness runs a fixed binary with arguments built by the test.
	cmd.Env = append(os.Environ(), "STREAMER_TEST_BATCH_OBSERVATION=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("observation fixture: %v %s", err, output)
	}
	if strings.Contains(string(output), "secretpass") || strings.Contains(string(output), "redis://") {
		t.Fatalf("failure logging exposed credentials: %s", output)
	}
	for _, stage := range []string{"series", "baseline", "price", "blocked"} {
		cause := stage + "_unavailable"
		if stage == "blocked" {
			cause = "deadline_exceeded"
		}
		found := false
		for _, line := range strings.Split(string(output), "\n") {
			if !strings.Contains(line, "derivatives batch: symbols failed") || !strings.Contains(line, "failure="+cause) {
				continue
			}
			if !strings.Contains(line, "exchange=binance market_type=future timeframe=30m symbols=1") || !strings.Contains(line, "affected=1") {
				t.Fatalf("failure scope missing: %s", line)
			}
			for _, field := range strings.Fields(line) {
				if strings.HasPrefix(field, "elapsed=") {
					elapsed, err := time.ParseDuration(strings.TrimPrefix(field, "elapsed="))
					if err != nil || elapsed <= 0 {
						t.Fatalf("elapsed not recorded: %s", line)
					}
					found = true
				}
			}
		}
		if !found {
			t.Fatalf("batch failure observation missing for %s: %s", stage, output)
		}
	}
}
