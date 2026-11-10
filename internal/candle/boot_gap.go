package candle

// BootGapMarkTimeframes returns the set of timeframes whose open bucket needs the
// gap mark at boot.
//
// The feed interval is ALWAYS included, even when the configuration does not list
// it: it is the interval the venue subscription actually receives, so its bucket is
// in flight by construction. Every other streamed timeframe is also marked, because
// a restart leaves all of them open at the same instant; marking only the wire
// interval lets `venueHasSpoken` veto the returning REST total for the derived ones
// and the bar stays short forever.
func BootGapMarkTimeframes(feedInterval string, streamed []string) []string {
	out := make([]string, 0, len(streamed)+1)
	seen := make(map[string]struct{}, len(streamed)+1)

	add := func(tf string) {
		if tf == "" {
			return
		}
		if _, ok := seen[tf]; ok {
			return
		}
		seen[tf] = struct{}{}
		out = append(out, tf)
	}

	add(feedInterval)
	for _, tf := range streamed {
		add(tf)
	}
	return out
}
