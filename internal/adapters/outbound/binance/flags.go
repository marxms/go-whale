package binancesdk

import (
	"os"
	"strings"
)

// Combined-URL partition budget. Binance allows 1024 streams and a
// bounded URL length per connection; we stay under both so a single handshake
// never trips a server-side limit.
const (
	// maxStreamsPerConnection caps the number of @aggTrade streams placed on one
	// combined-URL connection. 800 leaves headroom under Binance's 1024 limit.
	maxStreamsPerConnection = 800

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

// streamName converts a single domain symbol to its @trade stream name. The
// individual-trade stream is used instead of @aggTrade: measured in this region, the
// official futures host delivered @trade while @aggTrade produced no frames at all. @trade
// carries every fill, which is a superset of the aggregate trades the aggregator already
// accepts.
func streamName(symbol string) string {
	binanceSymbol := strings.ToLower(strings.ReplaceAll(symbol, "/", ""))
	return binanceSymbol + "@trade"
}

// partitionStreams splits the full stream list into per-connection slices, each
// within the stream-count budget AND the combined-URL length budget. Every
// stream lands in exactly one partition; ordering is preserved. The result is
// never empty when streams is non-empty.
func partitionStreams(streams []string) [][]string {
	return partitionStreamsWithBudget(streams, maxStreamsPerConnection, maxCombinedURLLength)
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
