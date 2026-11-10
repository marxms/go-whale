package trade

import (
	"encoding/json"
	"testing"
)

func TestParseSide(t *testing.T) {
	tests := []struct {
		raw  string
		want Side
	}{
		{"buy", Buy},
		{"BUY", Buy},
		{" B ", Buy},
		{"bid", Buy},
		{"long", Buy},
		{"sell", Sell},
		{"SELL", Sell},
		{" ask ", Sell},
		{"short", Sell},
		{"", Unknown},
		{"maker", Unknown},
		{"0", Unknown},
	}

	for _, tt := range tests {
		if got := ParseSide(tt.raw); got != tt.want {
			t.Errorf("ParseSide(%q) = %q, want %q", tt.raw, got, tt.want)
		}
	}
}

func TestSideVocabulary(t *testing.T) {
	if Buy.String() != "buy" || Sell.String() != "sell" {
		t.Fatalf("String() must preserve the storage vocabulary, got %q and %q", Buy, Sell)
	}
	if Unknown.String() != "unknown" {
		t.Fatalf("Unknown must stay explicit absence, got %q", Unknown)
	}
}

func TestLastTradeJSONContract(t *testing.T) {
	raw, err := json.Marshal(LastTrade{
		Timestamp:  1700000000123,
		Price:      42.5,
		Volume:     1.25,
		Side:       Sell,
		TradeID:    "987",
		ReceivedAt: 1700000000456,
		LatencyMS:  7,
	})
	if err != nil {
		t.Fatalf("marshal LastTrade: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal LastTrade: %v", err)
	}

	want := map[string]any{
		"timestamp":  float64(1700000000123),
		"price":      42.5,
		"volume":     1.25,
		"side":       "sell",
		"tradeId":    "987",
		"receivedAt": float64(1700000000456),
		"latency":    float64(7),
	}
	for key, value := range want {
		if got := decoded[key]; got != value {
			t.Errorf("LastTrade JSON %q = %v, want %v", key, got, value)
		}
	}
	if len(decoded) != len(want) {
		t.Errorf("LastTrade JSON has %d keys, want %d: %v", len(decoded), len(want), decoded)
	}
}
