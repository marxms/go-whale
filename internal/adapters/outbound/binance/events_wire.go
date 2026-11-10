package binancesdk

// tradeEvent is the Binance public-trade payload consumed by GoWhale. The
// @trade stream carries an individual trade id in `t`; the @aggTrade form
// carries an aggregate id in `a`. Supporting both keeps the adapter correct
// when an operator selects either stream without changing the canonical Trade.
type tradeEvent struct {
	Symbol       string `json:"s"`
	AggregateID  int64  `json:"a"`
	TradeID      int64  `json:"t"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	TradeTime    int64  `json:"T"`
	BuyerIsMaker bool   `json:"m"`
}

// klineEvent is the venue's kline frame. Pointers distinguish "field absent"
// from a legitimate zero in the venue payload.
//
// Every case-paired key the venue sends must have its own field. encoding/json
// falls back to a case-insensitive tag match, so without the uppercase fields
// the string "e":"kline" decodes into the int64 E and the whole frame fails,
// and "T"/"L" silently overwrite the lowercase open time and low.
type klineEvent struct {
	EventType string      `json:"e"`
	EventTime *int64      `json:"E"`
	Kline     *klineValue `json:"k"`
}

type klineValue struct {
	OpenTime        *int64  `json:"t"`
	CloseTime       *int64  `json:"T"`
	Open            *string `json:"o"`
	High            *string `json:"h"`
	Low             *string `json:"l"`
	Close           *string `json:"c"`
	Volume          *string `json:"v"`
	TakerBuyBaseVol *string `json:"V"`
	LastTradeID     *int64  `json:"L"`
	Closed          *bool   `json:"x"`
}
