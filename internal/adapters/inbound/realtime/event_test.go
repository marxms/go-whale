package realtime

import (
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

func TestNewEnvelopeSetsSchemaVersion(t *testing.T) {
	e := NewEnvelope(EventCandleClose, Scope{Symbol: "BTCUSDT"}, candle.Candle{}, 1, 123)
	if e.Version != SchemaVersion {
		t.Fatalf("Version = %d, want %d", e.Version, SchemaVersion)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	scope := Scope{Exchange: "binance", MarketType: "futures", Symbol: "BTCUSDT", Timeframe: "4h"}
	candle := candle.Candle{Timestamp: 2000, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 10}
	want := NewEnvelope(EventCandleClose, scope, candle, 42, 1_710_000_000_000)

	data, err := want.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.Version != want.Version || got.Seq != want.Seq || got.Type != want.Type {
		t.Errorf("header mismatch: got %+v, want %+v", got, want)
	}
	if got.Scope != want.Scope {
		t.Errorf("scope mismatch: got %+v, want %+v", got.Scope, want.Scope)
	}
	if got.Candle.Timestamp != want.Candle.Timestamp || got.Candle.Close != want.Candle.Close {
		t.Errorf("candle mismatch: got %+v, want %+v", got.Candle, want.Candle)
	}
	if got.EmittedAt != want.EmittedAt {
		t.Errorf("EmittedAt = %d, want %d", got.EmittedAt, want.EmittedAt)
	}
}

func TestChannelIsPrefixed(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		typ    EventType
		want   string
	}{
		{"close", "gowhale:prod", EventCandleClose, "gowhale:prod:candle.close"},
		{"revised", "gowhale:prod", EventCandleRevised, "gowhale:prod:candle.revised"},
		{"open-update", "gowhale:staging", EventCandleOpenUpdate, "gowhale:staging:candle.open_update"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Channel(tt.prefix, tt.typ); got != tt.want {
				t.Errorf("Channel(%q, %q) = %q, want %q", tt.prefix, tt.typ, got, tt.want)
			}
		})
	}
}
