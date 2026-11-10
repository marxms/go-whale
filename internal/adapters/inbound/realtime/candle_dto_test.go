package realtime

import (
	"encoding/json"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// The candle JSON contract belongs to CandleDTO, not to `candle.Candle`: these cases assert
// the DTO directly so the domain type can stay tag-free without the bus/socket shape
// drifting. The golden files in testdata/wire pin the whole envelope; these pin the
// candle-level rules that a golden diff would only show as a blob.

func TestCandleDTO_RoundTripPreservesEveryField(t *testing.T) {
	original := fullCandle()

	data, err := json.Marshal(NewCandleDTO(original))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded CandleDTO
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := decoded.ToCandle(); got != original {
		t.Fatalf("roundtrip mismatch:\n want %+v\n got  %+v", original, got)
	}
}

// TestCandleDTO_MeasuredPairAlwaysOnTheWire asserts the measured pair is always on the wire,
// and is the reason the domain contract exists: zero taker volume is a real market state, and
// with the derived order-flow fields gone there is no second copy to recover it from.
func TestCandleDTO_MeasuredPairAlwaysOnTheWire(t *testing.T) {
	data, err := json.Marshal(NewCandleDTO(candle.Candle{Timestamp: 1, Volume: 12.5}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"takerBuyVolume", "takerSellVolume"} {
		got, present := raw[key]
		if !present {
			t.Errorf("%q is absent — a legitimate zero would become indistinguishable from an "+
				"unmeasured bar: %s", key, data)
		}
		if string(got) != "0" {
			t.Errorf("%s = %s, want 0", key, got)
		}
	}
}

// TestCandleDTO_OptionalFieldsOmitAtZero pins every additive field's omitempty rule.
func TestCandleDTO_OptionalFieldsOmitAtZero(t *testing.T) {
	zero, err := json.Marshal(NewCandleDTO(candle.Candle{Timestamp: 1}))
	if err != nil {
		t.Fatalf("marshal zero candle: %v", err)
	}
	var zeroRaw map[string]json.RawMessage
	if err := json.Unmarshal(zero, &zeroRaw); err != nil {
		t.Fatalf("unmarshal zero candle: %v", err)
	}
	for _, key := range []string{
		"source", "aggregateMethod", "fetchedAt", "requestedAt", "lastTradeAt",
		"klineEventAt", "sideUnknownCount", "lateTradeCount", "hasGap",
	} {
		if _, present := zeroRaw[key]; present {
			t.Errorf("expected zero-value %q to be omitted, got %s", key, zero)
		}
	}

	populated, err := json.Marshal(NewCandleDTO(fullCandle()))
	if err != nil {
		t.Fatalf("marshal populated candle: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(populated, &raw); err != nil {
		t.Fatalf("unmarshal populated: %v", err)
	}
	for _, key := range []string{
		"source", "aggregateMethod", "sideUnknownCount", "lateTradeCount",
	} {
		if _, present := raw[key]; !present {
			t.Errorf("expected populated candle to carry %q, got %s", key, populated)
		}
	}
}

// TestCandleDTO_OriginNeverCrossesTheWire replaces the domain-level `json:"-"`. Origin is
// process-local provenance and a consumer must never inherit the producer's claim.
func TestCandleDTO_OriginNeverCrossesTheWire(t *testing.T) {
	c := fullCandle()
	c.Origin = candle.OriginNative

	data, err := json.Marshal(NewCandleDTO(c))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"Origin", "origin"} {
		if _, present := raw[key]; present {
			t.Errorf("Origin is on the wire — it must never leave the process: %s", data)
		}
	}

	var decoded CandleDTO
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal DTO: %v", err)
	}
	if got := decoded.ToCandle().Origin; got != candle.OriginUnknown {
		t.Fatalf("restored Origin = %v, want OriginUnknown", got)
	}
}
