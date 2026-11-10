package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/binance/binance-connector-go/clients/derivativestradingusdsfutures"
	"github.com/binance/binance-connector-go/clients/derivativestradingusdsfutures/src/restapi/models"
	"github.com/binance/binance-connector-go/common/v2/common"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/candle"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// ErrPermanentSymbol marks a per-symbol error that retrying cannot fix (e.g. an
// invalid or delisted symbol). The manager skips such symbols without tripping
// the shared per-exchange circuit breaker or rescheduling them, so one bad
// symbol cannot disable gap recovery for the whole exchange scope.
var ErrPermanentSymbol = errors.New("permanent symbol error")

// SDKGapFiller implements GapFiller using the official Binance SDK REST API.
// It replaces the legacy ExtendedKlinesFiller that used raw HTTP calls.
type SDKGapFiller struct {
	client *derivativestradingusdsfutures.BinanceDerivativesTradingUsdsFuturesClient
	budget *exchange.RestBudget
	log    *slog.Logger
}

// NewSDKGapFiller creates a new gap filler backed by the Binance SDK. The caller injects the
// process-wide REST weight budget so seed and gap recovery draw from the same per-IP pool.
// It creates its own REST client independent of the streaming WebSocket client.
func NewSDKGapFiller(budget *exchange.RestBudget, logs ...*slog.Logger) *SDKGapFiller {
	restCfg := &common.ConfigurationRestAPI{
		BasePath: common.DerivativesTradingUsdsFuturesRestApiProdUrl,
	}
	client := derivativestradingusdsfutures.NewBinanceDerivativesTradingUsdsFuturesClient(
		derivativestradingusdsfutures.WithRestAPI(restCfg),
	)
	return &SDKGapFiller{client: client, budget: budget, log: logger.OrDiscard(logs...)}
}

// Name returns "sdk-klines".
func (f *SDKGapFiller) Name() string {
	return "sdk-klines"
}

// Fill retrieves klines for a symbol/timeframe using the SDK REST API.
// The HistorySource parameter is ignored; the SDK client is used directly.
func (f *SDKGapFiller) Fill(ctx context.Context, _ HistorySource, req FillRequest) (FillResult, error) {
	if f.client == nil || f.client.RestApi == nil {
		return FillResult{}, fmt.Errorf("SDK REST client not initialized")
	}

	interval := stringToKlineInterval(req.Timeframe)
	if interval == "" {
		return FillResult{}, fmt.Errorf("unsupported interval: %s", req.Timeframe)
	}

	// req.Symbol is canonical internal (BTC/USDT). The binance-connector-go SDK
	// klines endpoint requires the raw form (BTCUSDT); the venue spelling stays
	// inside this filler.
	rawSymbol := toRawFuturesSymbol(req.Symbol)

	startMs := fillStartMs(req)
	endMs := req.CurrentBucket
	if req.UntilMs > 0 {
		endMs = req.UntilMs
	}
	if endMs <= 0 {
		endMs = time.Now().UnixMilli()
	}

	limit := int64(req.FetchLimit)
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	var allCandles []candle.Candle
	takerAuthoritative := true
	currentStart := startMs

	for morePagesToFetch(currentStart, endMs) {
		// RESERVE THE WEIGHT BEFORE EACH PAGE. This loop paginates: one gap can issue many
		// calls, and a batch fans it out across workers, so an unmetered filler is the
		// largest REST consumer in the process at exactly the moment the seed is also
		// running.
		//
		// Wiring the budget into the seed alone was not enough — measured on the first boot
		// of the ryzen host, the seed stayed clean while gap recovery took 30 HTTP 429s:
		//   [GAP-RECOVERY] Worker 0: sdk-klines failed ACU/USDT 5m: HTTP error 429
		// with zero budget waits logged, because this path never asked for any.
		//
		// The weight comes from the ACTUAL limit on the request below, so it cannot drift
		// from what is billed. Note limit defaults to 1000 here, which is the weight-5 tier —
		// a single page of this loop costs 2,5x a seed page.
		if f.budget != nil {
			if err := f.budget.AcquireKlines(ctx, int(limit)); err != nil {
				return FillResult{}, fmt.Errorf("rest budget for %s %s: %w", rawSymbol, req.Timeframe, err)
			}
		}

		resp, err := f.client.RestApi.MarketDataAPI.KlineCandlestickData(ctx).
			Symbol(rawSymbol).
			Interval(interval).
			StartTime(currentStart).
			EndTime(endMs).
			Limit(limit).
			Execute()
		if err != nil {
			if isInvalidSymbolError(err) {
				return FillResult{}, fmt.Errorf("invalid binance symbol %q (skipping, will not retry): %w", rawSymbol, ErrPermanentSymbol)
			}
			// A refusal is about US, not this symbol, and it is per-IP: continuing at full
			// pace on the other workers is what escalates a 429 into a 418 ban. Brake the
			// whole process, not just this call.
			if isRateLimitErr(err) && f.budget != nil {
				f.budget.PenalizeForRefusal()
			}
			return FillResult{}, fmt.Errorf("failed to fetch klines for %s %s: %w", rawSymbol, req.Timeframe, err)
		}

		if resp == nil {
			break
		}

		klines := resp.Data.Items
		if len(klines) == 0 {
			break
		}

		for _, kline := range klines {
			candle := parseSDKKlineItem(kline)
			if candle != nil && candle.Timestamp <= endMs {
				allCandles = append(allCandles, *candle)
				if _, measured := parseSDKKlineTakerBuy(kline); !measured {
					takerAuthoritative = false
				}
			}
		}

		// Advance start time for next page
		lastOpenTime := extractKlineOpenTime(klines[len(klines)-1])
		if lastOpenTime <= currentStart {
			break // Prevent infinite loop
		}
		currentStart = lastOpenTime + 1
	}

	// /: split out the open in-progress bucket so the manager seeds the
	// latest timestamp and the symbol's persisted freshness advances. Without
	// this, Fill only ever returns closed historical candles, the persisted
	// latest-timestamp never reaches "now", isStale() never clears, and the
	// symbol is re-enqueued every cycle (the burning-REST-weight bug). The open
	// bucket is derived honestly from the most recent fetched kline — Binance
	// returns it as the last item with open-time == CurrentBucket when endTime
	// reaches "now"; no price data is fabricated.
	historical, current := splitHistoricalAndCurrent(allCandles, req.CurrentBucket)

	logger.OrDiscard(f.log).Info("SDK gap fill complete",
		"symbol", rawSymbol, "timeframe", req.Timeframe, "candles", len(historical), "current_bucket", current != nil)
	return FillResult{Historical: historical, Current: current, TakerVolumesAuthoritative: len(allCandles) > 0 && takerAuthoritative}, nil
}

// morePagesToFetch decides whether the pagination loop issues another request.
//
// # THE BOUND IS INCLUSIVE, AND THE EXCLUSIVE VERSION WAS A SILENT NO-OP
//
// endMs is req.CurrentBucket for ordinary recovery, or the named closed bucket
// for a targeted repair. Binance filters klines by open-time inclusively, so a
// bucket is fetched exactly when the window's upper bound EQUALS it.
//
// With the exclusive `currentStart < endMs` the loop body did not run at all whenever the two
// coincided — and they coincide in the single most important case this filler exists for. The
// manager derives GapStart from the provider's latest timestamp, and the ring's latest entry
// IS the open bucket whenever the stream is alive. So a recovery job for a healthy symbol
// produced startMs == endMs == the open bucket, zero HTTP requests, an empty FillResult, and a
// Fill that returned success having fetched nothing.
//
// That is why enqueueBootGap cannot heal the bucket that was open at a restart: the enqueue
// fires, the worker runs, the filler asks for nothing, and a deficit measured against Binance
// (ETH -2,64% on exactly one bucket) survives a mechanism written to remove it. It also
// disables the freshness advance — Fill could only ever return CLOSED history, so the persisted
// latest timestamp never reached "now" for the very symbols the manager was asked to refresh.
//
// Termination is unchanged: the loop advances to lastOpenTime+1, so a page ending exactly at
// endMs leaves currentStart == endMs+1 and the next test is false. The empty-response and
// non-advancing-page breaks above are untouched.
func morePagesToFetch(currentStart, endMs int64) bool {
	return currentStart <= endMs
}

// fillStartMs decides where a fill actually begins.
//
// # THE FIELD THAT WAS BUILT AND NEVER CONNECTED
//
// The manager computes GapStart on every branch — bucket-aligned from the ring's latest
// timestamp when the provider knows one, and from the job's DisconnectTime when it does not
// — and puts it on the request. Nothing read it. The filler used SinceMs alone.
//
// On the fallback branches that is not a rounding error, it is a different query. SinceMs is
// left at ZERO there, so the pagination loop started at the Unix epoch and walked forward to
// now in pages of FetchLimit — which the same branch sizes to the gap, so it can be as small
// as two. Recovering a thirty-second disconnect on a symbol listed three years ago meant tens
// of thousands of weighted REST calls to arrive at the handful of buckets that were actually
// missing. Every one of them is billed against the shared per-IP budget the whole process
// shares with the seeder, which is what turns a 429 warning into a 418 ban.
//
// GapStart is the window the job MEANT. Honoring it makes the request describe the gap
// instead of describing all of history.
//
// # WHAT THIS DOES NOT FIX, STATED PLAINLY
//
// It does not recover INTERIOR holes. When the provider knows a latest timestamp, the
// manager derives GapStart from that same timestamp, so both fields point at the head of the
// series and the fetch covers [head, now]. A bucket missing BEHIND the head is not in that
// window and no amount of wiring here reaches it — closing that requires the manager to
// locate the hole, which it does not currently do. See the closure notes.
func fillStartMs(req FillRequest) int64 {
	if req.GapStart > 0 {
		return req.GapStart
	}
	return req.SinceMs
}

// splitHistoricalAndCurrent separates closed historical candles from the single
// open in-progress bucket. The open bucket is the candle whose open-time equals
// currentBucket (the start of the bucket that is still forming). It is returned
// as Current — not Historical — because it is not yet closed; seeding it advances
// the persisted latest timestamp to "now" and clears staleness.
//
// When currentBucket is 0 (unknown) nothing is promoted, so every candle stays
// historical. Candles with open-time beyond currentBucket are left historical
// defensively — only an exact open-time match is treated as the open bucket.
func splitHistoricalAndCurrent(all []candle.Candle, currentBucket int64) ([]candle.Candle, *candle.Candle) {
	if currentBucket <= 0 || len(all) == 0 {
		return all, nil
	}
	for i := range all {
		if all[i].Timestamp == currentBucket {
			current := all[i]
			historical := make([]candle.Candle, 0, len(all)-1)
			historical = append(historical, all[:i]...)
			historical = append(historical, all[i+1:]...)
			if len(historical) == 0 {
				return nil, &current
			}
			return historical, &current
		}
	}
	return all, nil
}

// toRawFuturesSymbol converts a unified or internal symbol to the raw Binance
// USDⓈ-M format the SDK REST klines endpoint expects:
//
//	"FOLKS/USDT:USDT" -> "FOLKSUSDT", "BTC/USDT" -> "BTCUSDT".
func toRawFuturesSymbol(s string) string {
	if i := strings.IndexByte(s, ':'); i >= 0 {
		s = s[:i]
	}
	return strings.ReplaceAll(s, "/", "")
}

// isInvalidSymbolError reports whether err is Binance's permanent "invalid
// symbol" rejection (REST code -1121), which retrying cannot fix.
func isInvalidSymbolError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "-1121") || strings.Contains(msg, "Invalid symbol")
}

// isRateLimitErr reports whether err is the exchange REFUSING US rather than a verdict
// about the request.
//
// The distinction drives a process-wide brake: Binance counts weight per IP and escalates
// 429 into a 418 IP ban that outlives the minute window, so the correct response is to stop
// every consumer, not to retry this one call. Matches the shapes the SDK surfaces — the raw
// status, Binance's own -1003 code, and the vendor-style DDoS protection wrapper.
func isRateLimitErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "429") ||
		strings.Contains(msg, "418") ||
		strings.Contains(msg, "-1003") ||
		strings.Contains(msg, "too many requests") ||
		strings.Contains(msg, "ddosprotection")
}

// stringToKlineInterval maps timeframe strings to SDK interval enum values.
func stringToKlineInterval(s string) models.ContinuousContractKlineCandlestickDataIntervalParameter {
	switch s {
	case "1s":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval1s
	case "1m":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval1m
	case "3m":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval3m
	case "5m":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval5m
	case "15m":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval15m
	case "30m":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval30m
	case "1h", "60m":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval1h
	case "2h":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval2h
	case "4h":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval4h
	case "6h":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval6h
	case "8h":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval8h
	case "12h":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval12h
	case "1d":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval1d
	case "3d":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval3d
	case "1w":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval1w
	case "1M":
		return models.ContinuousContractKlineCandlestickDataIntervalParameterInterval1M
	default:
		return ""
	}
}

// parseSDKKlineItem converts a single SDK kline response item to a candle.Candle.
// Binance kline format: [openTime, open, high, low, close, volume, closeTime, ...].
func parseSDKKlineItem(item models.KlineCandlestickDataResponseItem) *candle.Candle {
	if len(item.Items) < 6 {
		return nil
	}

	openTime := extractInt64(item.Items, 0)
	openStr := extractString(item.Items, 1)
	highStr := extractString(item.Items, 2)
	lowStr := extractString(item.Items, 3)
	closeStr := extractString(item.Items, 4)
	volStr := extractString(item.Items, 5)

	if openStr == "" || highStr == "" || lowStr == "" || closeStr == "" || volStr == "" {
		return nil
	}

	open, err := strconv.ParseFloat(openStr, 64)
	if err != nil {
		return nil
	}
	high, err := strconv.ParseFloat(highStr, 64)
	if err != nil {
		return nil
	}
	low, err := strconv.ParseFloat(lowStr, 64)
	if err != nil {
		return nil
	}
	closePrice, err := strconv.ParseFloat(closeStr, 64)
	if err != nil {
		return nil
	}
	volume, err := strconv.ParseFloat(volStr, 64)
	if err != nil {
		return nil
	}

	candle := &candle.Candle{
		Timestamp:  openTime,
		Open:       open,
		High:       high,
		Low:        low,
		Close:      closePrice,
		Volume:     volume,
		CreatedAt:  time.Now().UnixMilli(),
		LastUpdate: time.Now().UnixMilli(),
	}

	// R2 full-fidelity backfill: the Binance kline carries numberOfTrades (idx 8)
	// and takerBuyBaseAssetVolume (idx 9). Populating them — and deriving taker-sell,
	// CVD, and aggression exactly as the live aggregator does (sell = vol - buy,
	// cvd = buy - sell, aggr = buy / vol) — means a backfilled candle is not
	// CVD-void (CVD=0), which a trading-grade SLO forbids. Absent on
	// short arrays, the fields stay zero.
	if takerBuy, measured := parseSDKKlineTakerBuy(item); measured {
		// Clamp to [0, volume]: taker-buy can never exceed total volume.
		// Guards FP rounding when buy ≈ vol and keeps buy+sell == volume and
		// cvd == buy-sell self-consistent for downstream reconstruction.
		if takerBuy > volume {
			takerBuy = volume
		}
		candle.TakerBuyVolume = takerBuy
		candle.TakerSellVolume = volume - takerBuy
	}

	return candle
}

func parseSDKKlineTakerBuy(item models.KlineCandlestickDataResponseItem) (float64, bool) {
	if len(item.Items) < 10 {
		return 0, false
	}
	raw := extractString(item.Items, 9)
	takerBuy, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(takerBuy) || math.IsInf(takerBuy, 0) || takerBuy < 0 {
		return 0, false
	}
	return takerBuy, true
}

func extractInt64(items []models.KlineCandlestickDataResponseItemInner, idx int) int64 {
	if idx >= len(items) {
		return 0
	}
	if items[idx].Int64 != nil {
		return *items[idx].Int64
	}
	return 0
}

func extractString(items []models.KlineCandlestickDataResponseItemInner, idx int) string {
	if idx >= len(items) {
		return ""
	}
	if items[idx].String != nil {
		return *items[idx].String
	}
	return ""
}

func extractKlineOpenTime(item models.KlineCandlestickDataResponseItem) int64 {
	return extractInt64(item.Items, 0)
}
