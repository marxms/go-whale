package recovery

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/binance/binance-connector-go/clients/derivativestradingusdsfutures"
	"github.com/binance/binance-connector-go/common/v2/common"
)

// TestFill_FetchesTheOpenBucketWhenItIsAlsoTheWindowStart is the regression for a filler that
// reported success having issued ZERO requests.
//
// The manager derives GapStart from the provider's latest persisted timestamp, and while a
// stream is alive the ring's latest entry IS the open bucket — so GapStart == CurrentBucket
// for every healthy symbol. endMs is CurrentBucket, and the pagination loop was `currentStart
// < endMs`: start == end, zero iterations, empty FillResult, no error.
//
// That is why enqueueBootGap cannot heal the bucket that was open at a restart. The enqueue
// fires, a worker picks it up, the filler asks the exchange for nothing, and a deficit measured
// against Binance (ETH -2,64% on exactly one bucket, the next bucket 0,0000%) survives a
// mechanism written to remove it.
//
// The failing shape observes 0 requests and a nil Current. The correct one makes one request
// and promotes the open bucket to Current, which is what SeedCurrentBucketCandle MAX-merges
// into the live bucket.
func TestFill_FetchesTheOpenBucketWhenItIsAlsoTheWindowStart(t *testing.T) {
	const openBucket = int64(1_700_000_100_000) // arbitrary 5m boundary

	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		// One kline, open-time == the open bucket. Binance filters by open-time inclusively,
		// so this is what the endpoint returns for a window whose bound equals the bucket.
		_, _ = fmt.Fprintf(w, `[[%d,"100.0","110.0","90.0","105.0","12.5",%d,"1312.5",42,"7.5","787.5","0"]]`,
			openBucket, openBucket+299_999)
	}))
	defer srv.Close()

	f := &SDKGapFiller{
		client: derivativestradingusdsfutures.NewBinanceDerivativesTradingUsdsFuturesClient(
			derivativestradingusdsfutures.WithRestAPI(&common.ConfigurationRestAPI{BasePath: srv.URL}),
		),
	}

	res, err := f.Fill(context.Background(), nil, FillRequest{
		ExchangeID:    "binance",
		MarketType:    "future",
		Symbol:        "ETH/USDT",
		Timeframe:     "5m",
		SinceMs:       openBucket,
		GapStart:      openBucket,
		CurrentBucket: openBucket,
		FetchLimit:    2,
	})
	if err != nil {
		t.Fatalf("Fill returned an error: %v", err)
	}

	if got := requests.Load(); got != 1 {
		t.Fatalf("issued %d klines requests, want 1 — with an exclusive page bound a job whose "+
			"window starts at the open bucket asks the exchange for nothing and returns success, "+
			"which is how the boot-gap heal became a no-op", got)
	}
	if res.Current == nil {
		t.Fatalf("Current is nil — the open bucket is what SeedCurrentBucketCandle MAX-merges " +
			"into the live bucket; without it the restart deficit is never raised")
	}
	if res.Current.Timestamp != openBucket {
		t.Errorf("Current.Timestamp = %d, want %d", res.Current.Timestamp, openBucket)
	}
	if res.Current.Volume != 12.5 {
		t.Errorf("Current.Volume = %v, want 12.5 — the exchange's total for the bucket", res.Current.Volume)
	}
	if len(res.Historical) != 0 {
		t.Errorf("Historical = %d candles, want 0 — the only kline in the window is still open", len(res.Historical))
	}
}

func TestFill_TargetedOldBucketStopsAtInclusiveTarget(t *testing.T) {
	const target = int64(1_700_000_100_000)
	const current = target + 90*60_000
	var requests atomic.Int32
	var ends []int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		end, err := strconv.ParseInt(r.URL.Query().Get("endTime"), 10, 64)
		if err != nil {
			t.Errorf("endTime query: %v", err)
		}
		ends = append(ends, end)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `[[%d,"100.0","110.0","90.0","105.0","12.5",%d,"1312.5",42,"7.5","787.5","0"]]`, target, target+59_999)
	}))
	defer srv.Close()

	f := &SDKGapFiller{client: derivativestradingusdsfutures.NewBinanceDerivativesTradingUsdsFuturesClient(
		derivativestradingusdsfutures.WithRestAPI(&common.ConfigurationRestAPI{BasePath: srv.URL}),
	)}
	res, err := f.Fill(context.Background(), nil, FillRequest{
		ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT",
		Timeframe: "1m", GapStart: target, SinceMs: target,
		CurrentBucket: current, UntilMs: target, FetchLimit: 2,
	})
	if err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("requests = %d, want one page for the named bucket", got)
	}
	if len(ends) != 1 || ends[0] != target {
		t.Errorf("endTime values = %v, want inclusive target %d, not current %d", ends, target, current)
	}
	if res.Current != nil || len(res.Historical) != 1 || res.Historical[0].Timestamp != target {
		t.Errorf("result = %+v, want one closed historical target and no current bucket", res)
	}
	if !res.TakerVolumesAuthoritative {
		t.Error("SDK kline result must declare measured taker volumes, including legitimate zeroes")
	}
}

func TestFill_TakerAuthorityRequiresParsedTargetField(t *testing.T) {
	const target = int64(1_700_000_100_000)
	for _, tc := range []struct {
		name         string
		tail         string
		wantMeasured bool
	}{
		{name: "short OHLCV array", tail: ""},
		{name: "invalid taker field", tail: `,0,"0",1,"invalid"`},
		{name: "literal zero taker buy", tail: `,0,"0",1,"0"`, wantMeasured: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `[[%d,"1","2","1","2","3"%s]]`, target, tc.tail)
			}))
			defer srv.Close()
			f := &SDKGapFiller{client: derivativestradingusdsfutures.NewBinanceDerivativesTradingUsdsFuturesClient(
				derivativestradingusdsfutures.WithRestAPI(&common.ConfigurationRestAPI{BasePath: srv.URL}),
			)}
			res, err := f.Fill(context.Background(), nil, FillRequest{
				ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "1m",
				SinceMs: target, GapStart: target, UntilMs: target, CurrentBucket: target + 60_000, FetchLimit: 2,
			})
			if err != nil {
				t.Fatalf("Fill: %v", err)
			}
			if len(res.Historical) != 1 {
				t.Fatalf("historical count = %d, want target candle", len(res.Historical))
			}
			if res.TakerVolumesAuthoritative != tc.wantMeasured {
				t.Errorf("taker authority = %v, want %v for %s", res.TakerVolumesAuthoritative, tc.wantMeasured, tc.name)
			}
			if tc.wantMeasured && (res.Historical[0].TakerBuyVolume != 0 || res.Historical[0].TakerSellVolume != 3) {
				t.Errorf("measured zero buy should yield pair 0/3, got %+v", res.Historical[0])
			}
		})
	}
}

func TestFill_TakerAuthorityIgnoresDiscardedKline(t *testing.T) {
	const target = int64(1_700_000_100_000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w,
			`[[%d,"1","2","1","2","3",0,"0",1,"0"],[%d,"1","2","1","2","3"]]`,
			target, target+60_000)
	}))
	defer srv.Close()
	f := &SDKGapFiller{client: derivativestradingusdsfutures.NewBinanceDerivativesTradingUsdsFuturesClient(
		derivativestradingusdsfutures.WithRestAPI(&common.ConfigurationRestAPI{BasePath: srv.URL}),
	)}
	res, err := f.Fill(context.Background(), nil, FillRequest{
		ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT", Timeframe: "1m",
		SinceMs: target, GapStart: target, UntilMs: target, CurrentBucket: target + 60_000, FetchLimit: 2,
	})
	if err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if len(res.Historical) != 1 || res.Historical[0].Timestamp != target || !res.TakerVolumesAuthoritative {
		t.Errorf("result = %+v; discarded out-of-bound kline must not change the target's measured taker flag", res)
	}
}

// TestMorePagesToFetch_TerminatesOnTheAdvancedCursor states the other half of the contract: an
// inclusive bound must still terminate. The loop advances to lastOpenTime+1, so a page that
// ends exactly at the horizon leaves the cursor one millisecond past it.
func TestMorePagesToFetch_TerminatesOnTheAdvancedCursor(t *testing.T) {
	t.Parallel()

	const end = int64(1_700_000_100_000)

	if !morePagesToFetch(end, end) {
		t.Error("start == end must fetch: the horizon IS the open bucket's open-time, and Binance " +
			"filters by open-time inclusively")
	}
	if !morePagesToFetch(end-300_000, end) {
		t.Error("a window strictly before the horizon must fetch")
	}
	if morePagesToFetch(end+1, end) {
		t.Error("the cursor advanced past the horizon must stop — this is what bounds the loop")
	}
}
