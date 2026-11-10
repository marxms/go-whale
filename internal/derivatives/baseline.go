package derivatives

// DerivativesBaseline is the persisted robust ruler for one (symbol, timeframe).
// A nil axis means the baseline could not be built for it; it is never a zeroed
// ruler.
type DerivativesBaseline struct {
	Source     string                 `json:"source,omitempty"`
	SeriesTail []*DerivativesSnapshot `json:"seriesTail,omitempty"`
	Exchange   string                 `json:"exchange"`
	MarketType string                 `json:"marketType"`
	Symbol     string                 `json:"symbol"`
	Timeframe  string                 `json:"timeframe"`

	WindowN int `json:"windowN"`
	NMin    int `json:"nMin"`

	OpenInterest      *Baseline     `json:"openInterest,omitempty"`
	OpenInterestState BaselineState `json:"openInterestState"`

	LongShortRatio      *Baseline     `json:"longShortRatio,omitempty"`
	LongShortRatioState BaselineState `json:"longShortRatioState"`

	Price      *Baseline     `json:"price,omitempty"`
	PriceState BaselineState `json:"priceState"`

	UpdatedAt int64 `json:"updatedAt"`
}
