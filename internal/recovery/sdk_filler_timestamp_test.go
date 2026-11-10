package recovery

import (
	"testing"
	"time"

	models "github.com/binance/binance-connector-go/clients/derivativestradingusdsfutures/src/restapi/models"
)

// TestParseSDKKlineItem_TimestampsInMillis guards that backfilled candles stamp
// CreatedAt/LastUpdate in MILLISECONDS (time.Now().UnixMilli()), matching the unit used
// everywhere else (candle_engine.go, redis seed). The prior code used time.Now().Unix()
// (seconds), placing every backfilled candle ~1000x in the past (year 1970-ish) and
// breaking any age/staleness consumer that reads those fields.
func TestParseSDKKlineItem_TimestampsInMillis(t *testing.T) {
	before := time.Now().UnixMilli()
	item := models.KlineCandlestickDataResponseItem{
		Items: []models.KlineCandlestickDataResponseItemInner{
			i64Item(1_700_000_000_000), // openTime
			strItem("100"), strItem("110"), strItem("90"), strItem("105"),
			strItem("50"),              // volume
			i64Item(1_700_000_299_999), // closeTime
			strItem("5000"),            // quoteVol
			i64Item(10),                // numberOfTrades
			strItem("30"),              // takerBuyBase
			strItem("3000"),            // takerBuyQuote
			strItem("0"),               // ignore
		},
	}

	c := parseSDKKlineItem(item)
	if c == nil {
		t.Fatal("parseSDKKlineItem returned nil")
	}
	after := time.Now().UnixMilli()

	// A millisecond epoch is ~1.7e12; a seconds epoch would be ~1.7e9, far below `before`.
	// Asserting the stamp falls within the call's ms window proves UnixMilli (not Unix).
	if c.CreatedAt < before || c.CreatedAt > after {
		t.Fatalf("CreatedAt=%d not within ms window [%d,%d] — seconds-scale value indicates the Unix() bug", c.CreatedAt, before, after)
	}
	if c.LastUpdate < before || c.LastUpdate > after {
		t.Fatalf("LastUpdate=%d not within ms window [%d,%d]", c.LastUpdate, before, after)
	}
}
