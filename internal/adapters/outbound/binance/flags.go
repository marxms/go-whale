package binancesdk

import (
	"os"
	"strings"
)

// Combined-URL partition budget. Binance allows 1024 streams and a
// bounded URL length per connection; we stay well under both so one socket
// failure costs at most one small partition and the handshake stays short.
const (
	// maxStreamsPerConnection caps the number of @aggTrade streams placed on one
	// combined-URL connection. 800 leaves the URL cap to split 500 symbols into
	// two trade connections. The 100/125-stream experiment on 2026-11-23 raised
	// the total connection count from 4 to 8-10 and put partition 0 into a
	// 1-7 s reopen loop (38 closes in 25 min, all 1006); the two-connection
	// layout plus pinned trade edges is the configuration that held the regime.
	maxStreamsPerConnection = 800

	// maxKlineStreamsPerConnection caps @kline streams on one connection. It is a SEPARATE
	// budget because kline and aggTrade do not share the same tolerance. MEASURED 2026-11-24:
	// the 332-stream kline partition closed at 05:00, 09:01, 09:44 and again after 44 s, while
	// the 168-stream kline partition and the 332-stream aggTrade partition stayed up — kline
	// does not tolerate the 332-stream layout. 200 splits 500 symbols into 3 partitions
	// (~167 each) and bounds the blast radius; the aggTrade layout is unchanged.
	maxKlineStreamsPerConnection = 200

	// maxCombinedURLLength caps the total characters of the joined stream list
	// (the "a@aggTrade/b@aggTrade/..." portion). ~6KB is comfortably below the
	// practical request-line ceiling while ~500 symbols × ~15 chars ≈ 7.5KB, so
	// this also forces a split well before any server rejects the URL.
	maxCombinedURLLength = 6000
)

// wsBaseFromEnv resolves an operational WebSocket base URL.
func wsBaseFromEnv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

// wsEdgesFromEnv resolves an optional comma-separated list of pinned WS edges
// (host:port). An empty list keeps the venue's DNS resolution.
func wsEdgesFromEnv(name string) []string {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil
	}
	edges := make([]string, 0, 4)
	for _, part := range strings.Split(raw, ",") {
		if edge := strings.TrimSpace(part); edge != "" {
			edges = append(edges, edge)
		}
	}
	return edges
}

// isTestnetWSBase reports whether the configured WebSocket base points at the Binance
// futures TESTNET cluster.
//
// The legacy host names resolve to the testnet. It publishes production-like prices but a
// different order book, so volume, trades and taker splits are NOT production data. It stays
// reachable from networks that cannot consume the production stream, but choosing it must be
// explicit and audible: silently feeding testnet candles into a production chart is worse than
// having no live candle at all.
func isTestnetWSBase(base string) bool {
	return strings.Contains(strings.ToLower(base), "binancefuture.com")
}

// aggTradeStreams maps domain symbols (e.g. "BTC/USDT") to the SDK stream names
// ("btcusdt@aggTrade") used in the combined-stream URL.
func aggTradeStreams(symbols []string) []string {
	streams := make([]string, 0, len(symbols))
	for _, sym := range symbols {
		streams = append(streams, streamName(sym))
	}
	return streams
}

// streamName converts a single domain symbol to its @aggTrade stream name. The
// aggregate trade stream is the documented market feed and belongs on /market; the
// earlier @trade choice came from a probe taken on the wrong route. @aggTrade carries
// every fill, aggregated per price and side, at a fraction of the frame rate, and its
// aggregate id is the id the gap ledger was built around.
func streamName(symbol string) string {
	binanceSymbol := strings.ToLower(strings.ReplaceAll(symbol, "/", ""))
	return binanceSymbol + "@aggTrade"
}

// partitionStreams splits the full stream list into per-connection slices, each
// within the stream-count budget AND the combined-URL length budget. Every
// stream lands in exactly one partition; ordering is preserved. The result is
// never empty when streams is non-empty.
func partitionStreams(streams []string) [][]string {
	return partitionStreamsWithBudget(streams, maxStreamsPerConnection, maxCombinedURLLength)
}

// partitionKlineStreams is the kline feed's partitioner. It reads the kline budget, so the
// experiment can change one value without touching the aggTrade path (or the reverse).
func partitionKlineStreams(streams []string) [][]string {
	return partitionStreamsWithBudget(streams, maxKlineStreamsPerConnection, maxCombinedURLLength)
}

// partitionStreamsWithBudget is partitionStreams with explicit budgets, so tests can exercise
// the split without shrinking package state.
func partitionStreamsWithBudget(streams []string, maxStreams, maxLength int) [][]string {
	if len(streams) == 0 {
		return nil
	}

	var partitions [][]string
	var current []string
	currentLen := 0

	for _, s := range streams {
		// +1 accounts for the "/" separator the SDK inserts between streams.
		addLen := len(s) + 1
		overCount := len(current) >= maxStreams
		overLength := len(current) > 0 && currentLen+addLen > maxLength
		if overCount || overLength {
			partitions = append(partitions, current)
			current = nil
			currentLen = 0
		}
		current = append(current, s)
		currentLen += addLen
	}
	if len(current) > 0 {
		partitions = append(partitions, current)
	}
	return partitions
}
