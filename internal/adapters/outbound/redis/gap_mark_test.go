package redisstore

import (
	"strings"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

func TestMergeClosedCandlePreservesGapMark(t *testing.T) {
	current := candle.Candle{Timestamp: 1000, Open: 1, High: 2, Low: 0.5, Close: 1.5, HasGap: true, GapKind: candle.GapVolumeIncomplete}
	incoming := candle.Candle{Timestamp: 1000, Open: 1, High: 2, Low: 0.5, Close: 1.6} // late clean delta
	got := mergeClosedCandleBySource(current, incoming, CandleMergeSourceWSDelta, 2000)
	if !got.HasGap || got.GapKind != candle.GapVolumeIncomplete {
		t.Fatalf("merge erased the mark: %+v", got)
	}
}

func TestMergeOpenCandlePreservesGapKind(t *testing.T) {
	current := candle.Candle{Timestamp: 1000, Open: 1, High: 2, Low: 0.5, Close: 1.5, HasGap: true, GapKind: candle.GapProvisional}
	incoming := candle.Candle{Timestamp: 900, Open: 1, High: 2, Low: 0.5, Close: 1.2}
	got := mergeOpenCandleBySource(current, incoming, CandleMergeSourceWSDelta, 2000)
	if !got.HasGap || got.GapKind != candle.GapProvisional {
		t.Fatalf("open merge erased the cause: %+v", got)
	}
}

func TestCandleMemberRoundTripPreservesGapKind(t *testing.T) {
	for _, kind := range []candle.GapKind{candle.GapUnknown, candle.GapVolumeIncomplete, candle.GapProvisional} {
		in := candle.Candle{Timestamp: 1000, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 3, HasGap: kind != candle.GapUnknown, GapKind: kind}
		raw, err := encodeCandleMember(in)
		if err != nil {
			t.Fatalf("encode %v: %v", kind, err)
		}
		out, err := decodeCandleMember(raw)
		if err != nil {
			t.Fatalf("decode %v: %v", kind, err)
		}
		if out.HasGap != in.HasGap || out.GapKind != kind {
			t.Fatalf("round trip %v = hasGap %v kind %v", kind, out.HasGap, out.GapKind)
		}
	}
}

func TestDecodeV1MemberLeavesGapKindUnknown(t *testing.T) {
	v2, err := encodeCandleMember(candle.Candle{Timestamp: 1000, Close: 1.5, HasGap: true, GapKind: candle.GapProvisional})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	fields := strings.Split(v2, candleMemberSep)
	fields[0] = "v1"
	fields = fields[:candleMemberFieldCountV1]
	out, err := decodeCandleMember(strings.Join(fields, candleMemberSep))
	if err != nil {
		t.Fatalf("decode v1: %v", err)
	}
	if !out.HasGap || out.GapKind != candle.GapUnknown {
		t.Fatalf("v1 member = hasGap %v kind %v, want true/unknown", out.HasGap, out.GapKind)
	}
}

func TestDecodeRejectsUnknownGapKind(t *testing.T) {
	raw, err := encodeCandleMember(candle.Candle{Timestamp: 1000, Close: 1.5, HasGap: true})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	fields := strings.Split(raw, candleMemberSep)
	fields[20] = "x"
	if _, err := decodeCandleMember(strings.Join(fields, candleMemberSep)); err == nil {
		t.Fatal("an unknown gapKind token was accepted")
	}
}
