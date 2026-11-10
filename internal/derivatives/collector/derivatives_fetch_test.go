package collector

import (
	"context"
	"errors"
	"testing"

	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/market"
	"github.com/marxms/go-whale/internal/platform/config"
)

// ─────────────────────────────────────────────────────────────────────────────
// OPEN INTEREST IN CONTRACTS, decoupled from the funding fetch.
//
// Two properties, and they are separate claims:
//
//  1. The decision quantity is the contract count (`sumOpenInterest`, surfaced as
//     OpenInterestAmount). It is NEVER contracts × markPrice — measured, that product
//     changes with the price alone, correlates +0.982 with the candle return, and
//     exceeded the volume traded in the same bucket 98.31% of the time.
//  2. A FetchFundingRate that fails does not take the open interest down with it. Coupling
//     the contract count to `markPrice` would let a funding outage publish a symbol with no
//     open interest at all.
// ─────────────────────────────────────────────────────────────────────────────

// fakeRestClient is the smallest thing that satisfies derivatives.Source. Only
// the reads under test carry behavior; the rest exist to satisfy the interface
// and would fail loudly if the path under test ever reached them.
type fakeRestClient struct {
	funding    derivatives.FundingRate
	fundingErr error

	openInterest    derivatives.OpenInterest
	openInterestErr error

	// fundingHistory is what the settlement-interval revalidation reads. It is a
	// FUNCTION of the call count so a test can change the venue's cadence between two
	// revalidations, which is the event the whole mechanism exists to notice.
	fundingHistory    func(call int) []derivatives.FundingRateHistoryPoint
	fundingHistoryErr error

	// lsrHistory serves the long/short bootstrap walk. The signature carries the
	// page the collector asked for, so a test can assert both the timeframe and the
	// window without a seam in the poller.
	lsrHistory func(symbol, timeframe string, limit, sinceMs, endMs int64) []derivatives.LongShortRatioPoint

	fundingCalls        int
	oiCalls             int
	fundingHistoryCalls int
}

var _ derivatives.Source = (*fakeRestClient)(nil)

func (c *fakeRestClient) FundingRate(context.Context, string) (derivatives.FundingRate, error) {
	c.fundingCalls++
	return c.funding, c.fundingErr
}

func (c *fakeRestClient) OpenInterest(context.Context, string) (derivatives.OpenInterest, error) {
	c.oiCalls++
	return c.openInterest, c.openInterestErr
}

func (c *fakeRestClient) LoadMarkets(context.Context) (map[string]market.MarketInfo, error) {
	return nil, errNotWiredInFake
}

func (c *fakeRestClient) FundingRateHistory(context.Context, string, int64) ([]derivatives.FundingRateHistoryPoint, error) {
	c.fundingHistoryCalls++
	if c.fundingHistoryErr != nil {
		return nil, c.fundingHistoryErr
	}
	if c.fundingHistory == nil {
		return nil, errNotWiredInFake
	}
	return c.fundingHistory(c.fundingHistoryCalls), nil
}

func (c *fakeRestClient) OpenInterestHistory(context.Context, string, string, int64, int64, int64) ([]derivatives.OpenInterestHistoryPoint, error) {
	return nil, errNotWiredInFake
}

func (c *fakeRestClient) LongShortRatioHistory(_ context.Context, symbol, timeframe string, limit, sinceMs, endMs int64) ([]derivatives.LongShortRatioPoint, error) {
	if c.lsrHistory == nil {
		return nil, errNotWiredInFake
	}
	return c.lsrHistory(symbol, timeframe, limit, sinceMs, endMs), nil
}

func (c *fakeRestClient) Close() []error { return nil }

var errNotWiredInFake = errors.New("this fake does not serve that call")

func fetchTestPoller() *DerivativesPoller {
	return &DerivativesPoller{
		cfg:             config.DerivativesConfig{Enabled: true, MaxSeriesSize: 200},
		store:           &recordingStore{},
		fundingProfiles: NewFundingProfileRegistry(),
	}
}

// TestFetchDerivatives_OpenInterestIsContractsAndSurvivesAFundingFailure is the
// anchor the coverage matrix names: "a FetchFundingRate failure does NOT zero the
// collected open interest".
func TestFetchDerivatives_OpenInterestIsContractsAndSurvivesAFundingFailure(t *testing.T) {
	const contracts = 84_231.5
	const markPrice = 64_500.0

	tests := []struct {
		name   string
		client *fakeRestClient
		// wantContracts is the DECISION quantity. nil means the reading is absent,
		// which is a different answer from zero and is asserted as such.
		wantContracts *float64
		// wantNotional is the DISPLAY quantity. It needs a price, and without one it
		// is absent rather than substituted.
		wantNotional     *float64
		wantFundingRate  *float64
		wantFundingCalls int
	}{
		{
			// THE CASE. Funding is down; open interest was read cleanly and must land.
			name: "a funding failure leaves the contract count intact",
			client: &fakeRestClient{
				fundingErr:   errors.New("HTTP 503 from the funding endpoint"),
				openInterest: derivatives.OpenInterest{Amount: fptr(contracts)},
			},
			wantContracts:    fptr(contracts),
			wantNotional:     nil,
			wantFundingRate:  nil,
			wantFundingCalls: 1,
		},
		{
			// Same shape, different cause: the call succeeded but carried no price.
			// The contract count must not depend on the price being there either.
			name: "funding without a mark price leaves the contract count intact",
			client: &fakeRestClient{
				funding:      derivatives.FundingRate{Rate: fptr(0.0001)},
				openInterest: derivatives.OpenInterest{Amount: fptr(contracts)},
			},
			wantContracts:    fptr(contracts),
			wantNotional:     nil,
			wantFundingRate:  fptr(0.0001),
			wantFundingCalls: 1,
		},
		{
			name: "with both legs healthy the two quantities coexist under their own names",
			client: &fakeRestClient{
				funding:      derivatives.FundingRate{Rate: fptr(0.0001), MarkPrice: fptr(markPrice)},
				openInterest: derivatives.OpenInterest{Amount: fptr(contracts)},
			},
			wantContracts:    fptr(contracts),
			wantNotional:     fptr(contracts * markPrice),
			wantFundingRate:  fptr(0.0001),
			wantFundingCalls: 1,
		},
		{
			// The reverse coupling, asserted so the fix cannot be "read them together
			// in the other order": an open-interest failure does not take funding down.
			name: "an open-interest failure leaves the funding rate intact",
			client: &fakeRestClient{
				funding:         derivatives.FundingRate{Rate: fptr(0.0001), MarkPrice: fptr(markPrice)},
				openInterestErr: errors.New("HTTP 503 from the open-interest endpoint"),
			},
			wantContracts:    nil,
			wantNotional:     nil,
			wantFundingRate:  fptr(0.0001),
			wantFundingCalls: 1,
		},
		{
			// Absence stays absence: a response with no amount produces no reading,
			// never a zero and never the notional standing in for it.
			name: "an open interest response with no amount produces no reading",
			client: &fakeRestClient{
				funding:      derivatives.FundingRate{Rate: fptr(0.0001), MarkPrice: fptr(markPrice)},
				openInterest: derivatives.OpenInterest{},
			},
			wantContracts:    nil,
			wantNotional:     nil,
			wantFundingRate:  fptr(0.0001),
			wantFundingCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Each case owns its client and its poller, so they are independent.
			t.Parallel()

			poller := fetchTestPoller()
			scope := SymbolScope{ExchangeID: "binance", MarketType: "future", Symbol: "BTC/USDT"}

			snapshot, err := poller.fetchDerivatives(context.Background(), tt.client, scope)
			if err != nil {
				t.Fatalf("fetchDerivatives: %v", err)
			}

			assertFloatPtr(t, "openInterestBase (contracts — the decision quantity)",
				snapshot.OpenInterestBase, tt.wantContracts)
			assertFloatPtr(t, "openInterest (notional — display only)",
				snapshot.OpenInterest, tt.wantNotional)
			assertFloatPtr(t, "fundingRate", snapshot.FundingRate, tt.wantFundingRate)

			if tt.client.fundingCalls != tt.wantFundingCalls {
				t.Errorf("FetchFundingRate calls = %d, want %d", tt.client.fundingCalls, tt.wantFundingCalls)
			}
			if tt.client.oiCalls != 1 {
				t.Errorf("FetchOpenInterest calls = %d, want 1 — the open-interest read must be "+
					"issued regardless of what funding did", tt.client.oiCalls)
			}

			// The unit itself, stated as its own assertion: whatever landed in the
			// decision field is not the notional. contracts × markPrice is 64500× the
			// contract count here, so the two cannot be confused by accident.
			if snapshot.OpenInterestBase != nil && *snapshot.OpenInterestBase == contracts*markPrice {
				t.Fatalf("openInterestBase = %v, which is contracts × markPrice — the decision "+
					"quantity is the contract count", *snapshot.OpenInterestBase)
			}
		})
	}
}

func fptr(v float64) *float64 { return &v }

func assertFloatPtr(t *testing.T, field string, got, want *float64) {
	t.Helper()
	switch {
	case want == nil && got != nil:
		t.Errorf("%s = %v, want ABSENT — a reading that was not made must not be published", field, *got)
	case want != nil && got == nil:
		t.Errorf("%s is absent, want %v", field, *want)
	case want != nil && got != nil && *got != *want:
		t.Errorf("%s = %v, want %v", field, *got, *want)
	}
}
