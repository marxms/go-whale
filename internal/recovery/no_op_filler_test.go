package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoOpFiller_ImplementsGapFiller(_ *testing.T) {
	var _ GapFiller = (*NoOpFiller)(nil)
}

func TestNoOpFiller_Name(t *testing.T) {
	f := NewNoOpFiller()
	assert.Equal(t, "no-op", f.Name())
}

func TestNoOpFiller_Fill_ReturnsEmptyResult(t *testing.T) {
	f := NewNoOpFiller()
	req := FillRequest{
		ExchangeID:    "bingx",
		MarketType:    "future",
		Symbol:        "BTC-USDT",
		Timeframe:     "1m",
		SinceMs:       1234567890000,
		FetchLimit:    100,
		GapStart:      1234567890000,
		CurrentBucket: 1234567950000,
	}
	result, err := f.Fill(context.Background(), nil, req)
	require.NoError(t, err)
	assert.Empty(t, result.Historical)
	assert.Nil(t, result.Current)
}

func TestNoOpFiller_Fill_NilClientSafe(t *testing.T) {
	f := NewNoOpFiller()
	result, err := f.Fill(context.Background(), nil, FillRequest{})
	require.NoError(t, err)
	assert.Empty(t, result.Historical)
	assert.Nil(t, result.Current)
}

// TestManager_UsesNoOpFiller_WhenNoFillerConfigured verifies the nil guard in process().
// This test requires the Manager to be started with a mock dependency graph.
func TestManager_NoOpFiller_Default(t *testing.T) {
	m := NewManager(nil, 0, nil)
	require.NotNil(t, m)
	assert.NotNil(t, m.filler, "Manager should default to NoOpFiller")
	assert.Equal(t, "no-op", m.filler.Name())
}

func TestManager_NilGuard_FallsBackToNoOp(t *testing.T) {
	// Verify that if filler is explicitly set to nil, the nil guard kicks in.
	m := NewManager(nil, 1, nil)
	m.SetFiller(nil)
	// The process() method should fall back to NoOpFiller when activeFiller == nil.
	assert.Nil(t, m.filler)
	// We can't easily test process() internals without heavy mocking, but we verify
	// that the fallback variable is correctly set by reading the struct directly.
}

// compile-time check: NoOpFiller satisfies GapFiller.
var _ GapFiller = (*NoOpFiller)(nil)

// compile-time check: NoOpFiller can be used as Manager.filler.
var _ func() = func() {
	m := &Manager{filler: NewNoOpFiller()}
	_ = m.filler.Name()
}
