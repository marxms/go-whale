package recovery

import (
	"math"
	"testing"

	models "github.com/binance/binance-connector-go/clients/derivativestradingusdsfutures/src/restapi/models"
)

func i64Item(v int64) models.KlineCandlestickDataResponseItemInner {
	return models.KlineCandlestickDataResponseItemInner{Int64: &v}
}

func strItem(v string) models.KlineCandlestickDataResponseItemInner {
	return models.KlineCandlestickDataResponseItemInner{String: &v}
}

// TestParseSDKKlineItemFullFidelity proves backfilled candles carry trade-level
// fidelity (taker-buy -> CVD/aggression), not CVD=0.
// Binance REST kline array: [0]openTime [1]o [2]h [3]l [4]c [5]vol [6]closeTime
// [7]quoteVol [8]numberOfTrades [9]takerBuyBase [10]takerBuyQuote [11]ignore.
// The derivation (sell = vol-buy, cvd = buy-sell, aggr = buy/vol) matches the
// live aggregator and the public API output.
func TestParseSDKKlineItemFullFidelity(t *testing.T) {
	item := models.KlineCandlestickDataResponseItem{
		Items: []models.KlineCandlestickDataResponseItemInner{
			i64Item(1_700_000_000_000), // openTime
			strItem("100"), strItem("110"), strItem("90"), strItem("105"),
			strItem("68657.3"),         // volume
			i64Item(1_700_000_299_999), // closeTime
			strItem("7200000"),         // quoteVol
			i64Item(135),               // numberOfTrades
			strItem("38357.2"),         // takerBuyBase
			strItem("4000000"),         // takerBuyQuote
			strItem("0"),               // ignore
		},
	}

	c := parseSDKKlineItem(item)
	if c == nil {
		t.Fatal("parseSDKKlineItem returned nil")
	}
	if math.Abs(c.TakerBuyVolume-38357.2) > 1e-6 {
		t.Errorf("TakerBuyVolume = %v, want 38357.2", c.TakerBuyVolume)
	}
	wantSell := 68657.3 - 38357.2
	if math.Abs(c.TakerSellVolume-wantSell) > 1e-6 {
		t.Errorf("TakerSellVolume = %v, want %v", c.TakerSellVolume, wantSell)
	}
	// The reconstruction must leave the bar internally consistent: what it claims was
	// classified cannot exceed what actually traded.
	if c.TakerBuyVolume+c.TakerSellVolume > c.Volume+1e-6 {
		t.Errorf("buy+sell (%v) exceeds volume (%v)", c.TakerBuyVolume+c.TakerSellVolume, c.Volume)
	}
}

// TestParseSDKKlineItemShortArrayStillWorks proves a kline with only the OHLCV
// head (no trade-level tail) degrades gracefully — no panic, OHLCV intact, zero
// fidelity fields.
func TestParseSDKKlineItemShortArrayStillWorks(t *testing.T) {
	item := models.KlineCandlestickDataResponseItem{
		Items: []models.KlineCandlestickDataResponseItemInner{
			i64Item(1_700_000_000_000),
			strItem("100"), strItem("110"), strItem("90"), strItem("105"), strItem("1000"),
		},
	}
	c := parseSDKKlineItem(item)
	if c == nil {
		t.Fatal("nil on a valid 6-field kline")
	}
	if c.Close != 105 || c.Volume != 1000 {
		t.Errorf("OHLCV not parsed: close=%v vol=%v", c.Close, c.Volume)
	}
}
