package binancesdk

import (
	"math"
	"testing"

	"github.com/marxms/go-whale/internal/ingestion/streaming"
	"github.com/marxms/go-whale/internal/trade"
)

// TestSDKClient_GetBufferFillRatio_ResolvedCapDivisor guards the divisor-zero defect:
// GetBufferFillRatio must divide by the resolved capacity. Dividing by config.TradeBufferSize,
// which is 0 when the operator did not set it (the resolved 30000 fallback lives only inside
// BufferManager), produces +Inf, which calculateAdaptiveDelay reads as "buffers full" and pins
// the scheduler permanently to minProcessDelay. The divisor is the resolved bufCap.
func TestSDKClient_GetBufferFillRatio_ResolvedCapDivisor(t *testing.T) {
	c := NewSDKClient(streaming.ClientConfig{TradeBufferSize: 0}) // unset → resolves to 30000
	defer c.cancel()
	const sym = "BTCUSDT"
	buf := c.buffers.GetOrCreate(sym)
	for range 3000 {
		buf.Append(trade.Trade{})
	}

	got := c.GetBufferFillRatio(sym)
	if math.IsInf(got, 0) || math.IsNaN(got) {
		t.Fatalf("fill ratio must be finite, got %v (divisor-zero bug)", got)
	}
	if want := 3000.0 / 30000.0; math.Abs(got-want) > 1e-9 {
		t.Fatalf("fill ratio: want %v got %v", want, got)
	}
	// An untouched symbol's empty buffer must read 0, never NaN.
	if r := c.GetBufferFillRatio("OTHERUSDT"); r != 0 {
		t.Fatalf("empty-buffer ratio: want 0 got %v", r)
	}
}

// TestSpotSDKClient_GetBufferFillRatio_ResolvedCapDivisor mirrors the futures guard
// for the spot client.
func TestSpotSDKClient_GetBufferFillRatio_ResolvedCapDivisor(t *testing.T) {
	c := NewSpotSDKClient(streaming.ClientConfig{TradeBufferSize: 0})
	defer c.cancel()
	const sym = "BTCUSDT"
	buf := c.buffers.GetOrCreate(sym)
	for range 3000 {
		buf.Append(trade.Trade{})
	}

	got := c.GetBufferFillRatio(sym)
	if math.IsInf(got, 0) || math.IsNaN(got) {
		t.Fatalf("spot fill ratio must be finite, got %v", got)
	}
	if want := 3000.0 / 30000.0; math.Abs(got-want) > 1e-9 {
		t.Fatalf("spot fill ratio: want %v got %v", want, got)
	}
}

// TestSDKClient_GetBufferFillRatio_ExplicitCapNotOverridden ensures the 30000 fallback
// only applies when TradeBufferSize is unset — a configured cap is honored as divisor.
func TestSDKClient_GetBufferFillRatio_ExplicitCapNotOverridden(t *testing.T) {
	c := NewSDKClient(streaming.ClientConfig{TradeBufferSize: 10})
	defer c.cancel()
	const sym = "BTCUSDT"
	buf := c.buffers.GetOrCreate(sym)
	for range 5 {
		buf.Append(trade.Trade{})
	}
	if got := c.GetBufferFillRatio(sym); math.Abs(got-0.5) > 1e-9 {
		t.Fatalf("explicit cap=10 with 5 trades: want 0.5 got %v", got)
	}
}
