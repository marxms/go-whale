package collector

import (
	"context"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"
)

// TestBootstrapDepthFollowsTheDeclaredPerTimeframeCap pins the other end of the cap.
//
// A configurable cap that the bootstrap does not fill is a cap in name only: the ZSET
// would be allowed to hold 900 members and the walk would keep stopping at the
// baseline's 288, leaving the declared capacity permanently empty. The poller
// therefore asks config.DeclaredSeriesCap for THIS timeframe rather than reading the
// one global number, and this test is what makes that call load-bearing.
func TestBootstrapDepthFollowsTheDeclaredPerTimeframeCap(t *testing.T) {
	const timeframe = "5m"
	periodMs, _ := candle.ParseTimeframeMs(timeframe)

	tests := []struct {
		name      string
		cfg       config.DerivativesConfig
		wantDepth int
	}{
		{
			// Without an override the global value is below the declared baseline
			// window, so the window is what the walk reaches.
			name:      "no override stops at the declared baseline window",
			cfg:       config.DerivativesConfig{Enabled: true, MaxSeriesSize: 200},
			wantDepth: declaredBaselinePoints,
		},
		{
			name: "a per-timeframe override deepens the walk",
			cfg: config.DerivativesConfig{
				Enabled:              true,
				MaxSeriesSize:        200,
				SeriesCapByTimeframe: map[string]int{"5m": 900},
			},
			wantDepth: 900,
		},
		{
			// An override on ANOTHER timeframe must not move this one — otherwise the
			// map would be a second global knob wearing a per-timeframe shape.
			name: "an override on a different timeframe leaves this walk alone",
			cfg: config.DerivativesConfig{
				Enabled:              true,
				MaxSeriesSize:        200,
				SeriesCapByTimeframe: map[string]int{"1d": 900},
			},
			wantDepth: declaredBaselinePoints,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := &recordingStore{}
			poller := &DerivativesPoller{
				cfg:             tt.cfg,
				store:           store,
				tfReg:           candle.NewTimeframeRegistry([]string{timeframe}),
				fundingProfiles: NewFundingProfileRegistry(),
			}
			client := &fakeRestClient{lsrHistory: func(_, _ string, _ int64, sinceMs, endMs int64) []derivatives.LongShortRatioPoint {
				var points []derivatives.LongShortRatioPoint
				for _, ts := range providerPage(historyPage{StartMs: sinceMs, EndMs: endMs}, periodMs, historyPageLimit) {
					points = append(points, derivatives.LongShortRatioPoint{
						Timestamp: ts, Ratio: 1.2, LongPct: 54.5, ShortPct: 45.5, HasShares: true,
					})
				}
				return points
			}}

			saved := poller.bootstrapLSRForTimeframe(
				context.Background(), client, testScope(), timeframe)

			// One bucket of slack, and only one: the fake provider floors the window's
			// newest edge to a bucket boundary, exactly as the venue does, so the top
			// slot of the requested span holds no closed bucket yet. The slack is far
			// smaller than the gap between the cases (288 against 900), so it cannot
			// hide a walk that ignored the declared cap.
			if saved < tt.wantDepth-1 || saved > tt.wantDepth {
				t.Errorf("collected %d points, want %d — the walk is not sized by the cap "+
					"declared for %s", saved, tt.wantDepth, timeframe)
			}
			if store.count() != saved {
				t.Errorf("store holds %d, walk reported %d", store.count(), saved)
			}
		})
	}
}
