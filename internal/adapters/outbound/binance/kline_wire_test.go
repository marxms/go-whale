package binancesdk

import (
	"encoding/json"
	"testing"
)

// Captured from wss://fstream.binancefuture.com/stream?streams=btcusdt@kline_5m.
// The venue sends case-paired keys (e/E, t/T, l/L, v/V); this payload exercises all of them
// being folded together by encoding/json's case-insensitive fallback.
const capturedKlineData = `{
	"e": "kline", "E": 1793914022475, "s": "BTCUSDT",
	"k": {
		"t": 1793913900000, "T": 1793914199999, "s": "BTCUSDT", "i": "5m",
		"f": 554718279, "L": 554718702,
		"o": "75900.10", "c": "75903.60", "h": "76051.80", "l": "75895.40",
		"v": "1366.9472", "n": 424, "x": false,
		"q": 103748509.331910, "V": "8.3949", "Q": 638429.914750, "B": "0"
	}
}`

func TestKlineEventDecodesVenueCasePairedKeys(t *testing.T) {
	var event klineEvent
	if err := json.Unmarshal([]byte(capturedKlineData), &event); err != nil {
		t.Fatalf("unmarshal kline frame: %v", err)
	}
	if event.EventType != "kline" {
		t.Fatalf("event type = %q, want kline", event.EventType)
	}
	if event.EventTime == nil || *event.EventTime != 1793914022475 {
		t.Fatalf("event time = %v, want 1793914022475", event.EventTime)
	}
	if event.Kline == nil {
		t.Fatal("kline payload is nil")
	}
	if event.Kline.OpenTime == nil || *event.Kline.OpenTime != 1793913900000 {
		t.Fatalf("open time = %v, want 1793913900000 (the close time T must not overwrite it)", event.Kline.OpenTime)
	}
	if event.Kline.Low == nil || *event.Kline.Low != "75895.40" {
		t.Fatalf("low = %v, want 75895.40 (the last trade id L must not overwrite it)", event.Kline.Low)
	}
}

func TestKlineToVenueCandleFromCapturedFrame(t *testing.T) {
	var event klineEvent
	if err := json.Unmarshal([]byte(capturedKlineData), &event); err != nil {
		t.Fatalf("unmarshal kline frame: %v", err)
	}
	candle, ok := toVenueCandle(event)
	if !ok {
		t.Fatal("toVenueCandle rejected a frame the venue actually sends")
	}
	if candle.Timestamp != 1793913900000 {
		t.Fatalf("timestamp = %d, want 1793913900000", candle.Timestamp)
	}
	if candle.Open != 75900.10 || candle.High != 76051.80 || candle.Low != 75895.40 || candle.Close != 75903.60 {
		t.Fatalf("ohlc = %v/%v/%v/%v, want captured values", candle.Open, candle.High, candle.Low, candle.Close)
	}
	if candle.Volume != 1366.9472 {
		t.Fatalf("volume = %v, want 1366.9472", candle.Volume)
	}
	if candle.TakerBuyVolume != 8.3949 {
		t.Fatalf("taker buy = %v, want 8.3949", candle.TakerBuyVolume)
	}
	if candle.TakerSellVolume != 1358.5523 {
		t.Fatalf("taker sell = %v, want 1358.5523", candle.TakerSellVolume)
	}
	if candle.KlineEventAt != 1793914022475 {
		t.Fatalf("kline event at = %d, want 1793914022475", candle.KlineEventAt)
	}
}
