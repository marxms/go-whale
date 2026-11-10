package candle

// DeriveTakerSell reconstructs the taker-SELL side of a candle that carries only taker-buy
// and total volume, using the identity the exchange guarantees for a fully-classified kline:
//
//	sell = volume − buy
//
// This is a compatibility shim, not a derivation rule. It serves only REST/SDK klines and
// cold rows written before taker_sell_volume existed; live data is measured directly by the
// aggregator. See docs/decisions/0010-taker-sell-legacy-reconstruction.md.
//
// The no-op on `TakerBuyVolume <= 0` keeps the ILP zero-sentinel honest: ILP has no NULL, so
// a candle with no measured flow is stored with taker_buy = 0, indistinguishable from a
// genuine 100%-sell bar. Reconstructing anyway would turn every legacy OHLCV-only row into a
// fleet-wide claim of 100% sell aggression that no measurement supports.
func DeriveTakerSell(c *Candle) {
	if c.TakerBuyVolume <= 0 || c.Volume <= 0 {
		return
	}
	// Clamp FP rounding: taker-buy can never exceed total volume.
	if c.TakerBuyVolume > c.Volume {
		c.TakerBuyVolume = c.Volume
	}
	c.TakerSellVolume = c.Volume - c.TakerBuyVolume
}
