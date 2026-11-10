package redisstore

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// The legacy JSON contract belongs to this adapter's DTO: `candleLegacyWire` is the only form of
// the legacy JSON member, and it must stay byte-compatible with the rows older binaries left in
// Redis. The production binary never emits this form — it decodes it — so the output mapping
// lives here, in the test, and not in the adapter.

// legacyWireFromCandle builds the legacy DTO for a candle. It is test-only on purpose: the
// production path writes the positional v1 member, and keeping the output mapping out of the
// binary is what prevents a second writer from drifting back in.
func legacyWireFromCandle(c candle.Candle) candleLegacyWire {
	return candleLegacyWire{
		Timestamp:        c.Timestamp,
		Open:             c.Open,
		High:             c.High,
		Low:              c.Low,
		Close:            c.Close,
		Volume:           c.Volume,
		CreatedAt:        c.CreatedAt,
		LastUpdate:       c.LastUpdate,
		TakerBuyVolume:   c.TakerBuyVolume,
		TakerSellVolume:  c.TakerSellVolume,
		Source:           c.Source,
		AggregateMethod:  c.AggregateMethod,
		FetchedAt:        c.FetchedAt,
		RequestedAt:      c.RequestedAt,
		LastTradeAt:      c.LastTradeAt,
		KlineEventAt:     c.KlineEventAt,
		SideUnknownCount: c.SideUnknownCount,
		LateTradeCount:   c.LateTradeCount,
		HasGap:           c.HasGap,
	}
}

// TestCandleLegacyWire_ByteShape pins the byte-for-byte shape of a candle without order flow.
//
// The taker pair IS PRESENT with value 0, and that is the point: it has no omitempty so that a
// legitimately one-sided bar is not erased. The consumer reads absence as "previous contract"
// and 0/0 as "no classified flow", which are different answers.
func TestCandleLegacyWire_ByteShape(t *testing.T) {
	c := candle.Candle{
		Timestamp:  1_700_000_000_000,
		Open:       100.5,
		High:       110.25,
		Low:        99.75,
		Close:      105,
		Volume:     12.5,
		CreatedAt:  1_700_000_000_500,
		LastUpdate: 1_700_000_000_999,
	}

	got, err := json.Marshal(legacyWireFromCandle(c))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	want := `{"timestamp":1700000000000,"open":100.5,"high":110.25,"low":99.75,"close":105,"volume":12.5,"createdAt":1700000000500,"lastUpdate":1700000000999,"takerBuyVolume":0,"takerSellVolume":0}`
	if string(got) != want {
		t.Fatalf("legacy JSON shape drift:\n want %s\n got  %s", want, string(got))
	}
}

// TestCandleLegacyWire_AbsentTakerPairDecodesButIsNeverWritten documents both halves of
// the absent-vs-zero rule. A row written before the pair existed decodes to 0/0 — the
// domain has no presence bit, and absence means "older contract". The encoder, however,
// always writes both keys, so a bar whose aggression was genuinely one-sided is not
// erased and stays distinguishable from a never-measured one at the wire level.
func TestCandleLegacyWire_AbsentTakerPairDecodesButIsNeverWritten(t *testing.T) {
	withoutPair := `{"timestamp":1700000000000,"open":100,"high":110,"low":90,"close":105,"volume":12.5,"createdAt":1700000000500,"lastUpdate":1700000000999}`

	c, err := decodeCandleMember(withoutPair)
	if err != nil {
		t.Fatalf("decode legacy row without the pair: %v", err)
	}
	if c.TakerBuyVolume != 0 || c.TakerSellVolume != 0 {
		t.Fatalf("absent pair must decode as zero-valued measurement, got %v/%v",
			c.TakerBuyVolume, c.TakerSellVolume)
	}

	encoded, err := json.Marshal(legacyWireFromCandle(candle.Candle{Timestamp: 1}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{`"takerBuyVolume":0`, `"takerSellVolume":0`} {
		if !strings.Contains(string(encoded), key) {
			t.Errorf("expected %s to be present — an erased zero is indistinguishable from an "+
				"absent measurement, and there is no derived field to recover it from:\n%s",
				key, encoded)
		}
	}
}

// TestCandleLegacyWire_OptionalFieldsOmitAtZero pins the omitempty rules of the legacy
// member, field by field. `sideUnknownCount`, `lateTradeCount` and `hasGap` keep omitempty
// on purpose: absence means "not measured/recorded", never a fabricated zero.
func TestCandleLegacyWire_OptionalFieldsOmitAtZero(t *testing.T) {
	zero, err := json.Marshal(legacyWireFromCandle(candle.Candle{Timestamp: 1}))
	if err != nil {
		t.Fatalf("marshal zero candle: %v", err)
	}
	for _, key := range []string{
		"source", "aggregateMethod", "fetchedAt", "requestedAt", "lastTradeAt",
		"klineEventAt", "sideUnknownCount", "lateTradeCount", "hasGap",
	} {
		if strings.Contains(string(zero), `"`+key+`"`) {
			t.Errorf("expected zero-value %q to be omitted, got %s", key, zero)
		}
	}

	populated, err := json.Marshal(legacyWireFromCandle(productionCandle()))
	if err != nil {
		t.Fatalf("marshal populated candle: %v", err)
	}
	for _, key := range []string{
		"source", "aggregateMethod", "fetchedAt", "requestedAt", "lastTradeAt",
		"klineEventAt", "sideUnknownCount", "lateTradeCount", "hasGap",
	} {
		if !strings.Contains(string(populated), `"`+key+`"`) {
			t.Errorf("expected populated candle to carry %q, got %s", key, populated)
		}
	}
}

// TestCandleLegacyWire_OriginStaysOutOfTheMember: Origin is process-local provenance and has no
// representation in the member, so a persisted row must come back as Unknown, never inherit a
// claim from the writer.
func TestCandleLegacyWire_OriginStaysOutOfTheMember(t *testing.T) {
	c := productionCandle()
	c.Origin = candle.OriginNative

	data, err := json.Marshal(legacyWireFromCandle(c))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"Origin", "origin"} {
		if _, present := raw[key]; present {
			t.Errorf("Origin is on the legacy member — it is process-local and must never be persisted: %s", data)
		}
	}

	got, err := decodeCandleMember(string(data))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Origin != candle.OriginUnknown {
		t.Fatalf("restored Origin = %v, want OriginUnknown — provenance must not survive persistence", got.Origin)
	}
}

// TestCandleLegacyWire_RoundTripPreservesEveryField ensures the legacy DTO is lossless in
// both directions, and that a one-sided bar survives as a MEASURED bar (buy+sell > 0).
func TestCandleLegacyWire_RoundTripPreservesEveryField(t *testing.T) {
	original := productionCandle()

	data, err := json.Marshal(legacyWireFromCandle(original))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := decodeCandleMember(string(data))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != original {
		t.Fatalf("roundtrip mismatch:\n want %+v\n got  %+v", original, got)
	}

	oneSided := productionCandle()
	oneSided.TakerBuyVolume = 12.5
	oneSided.TakerSellVolume = 0

	data, err = json.Marshal(legacyWireFromCandle(oneSided))
	if err != nil {
		t.Fatalf("marshal one-sided: %v", err)
	}
	got, err = decodeCandleMember(string(data))
	if err != nil {
		t.Fatalf("decode one-sided: %v", err)
	}
	if got.TakerBuyVolume != 12.5 || got.TakerSellVolume != 0 {
		t.Fatalf("100%%-buy bar did not survive the legacy wire: buy=%v sell=%v",
			got.TakerBuyVolume, got.TakerSellVolume)
	}
	if got.TakerBuyVolume+got.TakerSellVolume <= 0 {
		t.Fatal("a fully buy-side bar must still read as MEASURED (buy+sell > 0)")
	}
}
