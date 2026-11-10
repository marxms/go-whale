package market

// MarketInfo is the canonical description of one venue market, as far as the
// core needs it. It is deliberately free of any vendor SDK type: the venue
// adapter translates the venue response into this shape at the boundary, and no
// consumer imports the vendor package to read metadata.
//
// Optional fields stay POINTERS. The distinction between "absent" and "zero"
// is load-bearing here: a market with no Created date must not be read as one
// listed at the Unix epoch, and a market with no Active flag is treated as
// active while an explicit false is not.
type MarketInfo struct {
	Symbol        *string
	ID            *string
	BaseCurrency  *string
	BaseID        *string
	QuoteCurrency *string
	QuoteID       *string
	Settle        *string
	SettleID      *string
	Type          *string
	// UnderlyingType separates crypto from venue-listed TradFi markets (EQUITY,
	// HK_EQUITY, COMMODITY, ...). TradFi follows its own trading hours, so the
	// fleet-liveness denominator excludes it; nil means the venue did not say.
	UnderlyingType *string
	Active         *bool
	Spot           *bool
	Swap           *bool
	Linear         *bool
	Contract       *bool
	Created        *int64
}
