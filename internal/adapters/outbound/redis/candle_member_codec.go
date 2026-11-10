package redisstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/marxms/go-whale/internal/candle"
)

// `candles` MEMBER ENCODING — POSITIONAL AND VERSIONED.
//
// # WHERE THE BYTES COME FROM
//
// Measured: 256.0 B per member, of which **56.6% is field names and punctuation**. The
// member does not echo the key — it has no `exchange`/`symbol` — so the waste here is
// almost entirely nominal: `"takerBuyVolume":`, `"aggregateMethod":`,
// `"sideUnknownCount":` and their siblings, rewritten in each of the ~521 members of each of
// the 4,024 keys. The positional member costs 109.2 B: **-146.7 B**.
//
// # `timestamp` STAYS, AND IT IS THE ONLY THING IN THE MEMBER THAT DISCRIMINATES ONE BUCKET
// FROM ANOTHER
//
// **In a ZSET the member is the IDENTITY.** Without the `timestamp` inside it, two candles
// with the same OHLCV produce the same member, and the `ZADD` of the second bucket MOVES the
// first one's score instead of inserting — the old candle disappears.
//
// This is not hypothetical: a bar with no trades has `open == high == low == close` at the
// last price, zero volume and taker 0/0, and the seed writes the whole batch with a single
// `now`, which also makes `createdAt` and `lastUpdate` equal. Measured in this package: two
// such candles become `ZCARD` = 1, and
// `TestTieredStore_GetCandlesByScoreRange_NoColdTier` saw 50 buckets become 1. A
// repeated-value bar is the normal bar of a stalled asset.
//
// Price of the encoding, declared: the member is ~14 B larger than the projected 109.2 B.
//
// # THE EMPTY POSITION MIRRORS THE FIELD'S `omitempty`, FIELD BY FIELD
//
// Where the struct declares `omitempty`, zero is absence and the position goes empty. Where
// it DELIBERATELY does not — `takerBuyVolume` and `takerSellVolume` — the position is always
// filled, including with "0". This is not aesthetic symmetry: zero there is a measured
// market state (all aggression on one side), present in ~1% of the fleet's bars — 2,567 of
// 305,269 5m bars with taker-buy at zero — and those are precisely the bars with the
// strongest signal. Treating that zero as "did not arrive" would erase the measurement
// instead of saving a byte.
//
// # THE FAILURE RULE
//
// An unknown version fails LOUD. It only appears one way — a newer image wrote `v2` and
// someone reverted the binary — and returning the candles this reader understands would
// serve a SHORT window. The gap validator reads that window and would read the gap as a
// hole, triggering a backfill over data that is there. The image comes back; the data in
// Redis does not.

const (
	// candleMemberVersion is the version THIS binary writes.
	candleMemberVersion = "v1"
	// candleMemberFieldCount includes the version itself and the `timestamp`.
	candleMemberFieldCount = 20
	candleMemberSep        = "|"
)

// ErrCandleMemberUnknownVersion marks a member written by a version this binary does not read.
var ErrCandleMemberUnknownVersion = errors.New("candles: unknown member encoding version")

// ErrCandleMemberMalformed marks a member with the right version and the wrong content.
var ErrCandleMemberMalformed = errors.New("candles: malformed member")

// CandleMemberDecodeError is the typed error that carries the version, reason and sample.
type CandleMemberDecodeError struct {
	Version string
	Reason  string
	Sample  string
	cause   error
}

func (e *CandleMemberDecodeError) Error() string {
	return fmt.Sprintf("candles: unreadable member (version %q): %s (sample: %s)", e.Version, e.Reason, e.Sample)
}

func (e *CandleMemberDecodeError) Unwrap() error { return e.cause }

// candleLegacyWire is the JSON form of the member written BEFORE the v1 positional encoding.
//
// This binary does not emit it — `encodeCandleMember` writes v1 delimited by `|` — but every
// key not yet rewritten holds rows in that form, and `decodeCandleMember` has to read them
// exactly as the legacy writer left them. The names, the ORDER and the omitempty rules
// reproduce the historical `candle.Candle` contract byte for byte; the domain type carries no
// JSON tags, so this DTO is the only owner of the legacy contract.
type candleLegacyWire struct {
	Timestamp  int64   `json:"timestamp"`
	Open       float64 `json:"open"`
	High       float64 `json:"high"`
	Low        float64 `json:"low"`
	Close      float64 `json:"close"`
	Volume     float64 `json:"volume"`
	CreatedAt  int64   `json:"createdAt"`
	LastUpdate int64   `json:"lastUpdate"`

	// WITHOUT omitempty, on purpose: zero is a measured market state and must be in the
	// member, including when both sides are zero (see the block above).
	TakerBuyVolume  float64 `json:"takerBuyVolume"`
	TakerSellVolume float64 `json:"takerSellVolume"`
	Source          string  `json:"source,omitempty"`
	AggregateMethod string  `json:"aggregateMethod,omitempty"`

	FetchedAt        int64 `json:"fetchedAt,omitempty"`
	RequestedAt      int64 `json:"requestedAt,omitempty"`
	LastTradeAt      int64 `json:"lastTradeAt,omitempty"`
	KlineEventAt     int64 `json:"klineEventAt,omitempty"`
	SideUnknownCount int64 `json:"sideUnknownCount,omitempty"`
	LateTradeCount   int64 `json:"lateTradeCount,omitempty"`
	HasGap           bool  `json:"hasGap,omitempty"`
}

// candleLegacyWireToCandle is the explicit input mapping. `Origin` is process-local
// provenance and has no representation in the member; a row read always comes back as
// OriginUnknown.
func candleLegacyWireToCandle(w candleLegacyWire) candle.Candle {
	return candle.Candle{
		Timestamp:        w.Timestamp,
		Open:             w.Open,
		High:             w.High,
		Low:              w.Low,
		Close:            w.Close,
		Volume:           w.Volume,
		CreatedAt:        w.CreatedAt,
		LastUpdate:       w.LastUpdate,
		TakerBuyVolume:   w.TakerBuyVolume,
		TakerSellVolume:  w.TakerSellVolume,
		Source:           w.Source,
		AggregateMethod:  w.AggregateMethod,
		FetchedAt:        w.FetchedAt,
		RequestedAt:      w.RequestedAt,
		LastTradeAt:      w.LastTradeAt,
		KlineEventAt:     w.KlineEventAt,
		SideUnknownCount: w.SideUnknownCount,
		LateTradeCount:   w.LateTradeCount,
		HasGap:           w.HasGap,
	}
}

// encodeCandleMember serializes the candle into the positional member.
func encodeCandleMember(c candle.Candle) (string, error) {
	for name, text := range map[string]string{"source": c.Source, "aggregateMethod": c.AggregateMethod} {
		if strings.Contains(text, candleMemberSep) {
			return "", fmt.Errorf("candles: field %s contains the separator %q: %q", name, candleMemberSep, text)
		}
	}

	fields := make([]string, 0, candleMemberFieldCount)
	fields = append(fields,
		candleMemberVersion,
		// The member identifier. See the block above on why it is not an echo.
		strconv.FormatInt(c.Timestamp, 10),
		// OHLCV and the two write clocks: no omitempty in the struct, but their zero is
		// indistinguishable from absence in Go, so the empty position represents them without
		// loss.
		candleFloat(c.Open),
		candleFloat(c.High),
		candleFloat(c.Low),
		candleFloat(c.Close),
		candleFloat(c.Volume),
		candleInt(c.CreatedAt),
		candleInt(c.LastUpdate),
		// ALWAYS filled — see the block about `omitempty` above.
		strconv.FormatFloat(c.TakerBuyVolume, 'g', -1, 64),
		strconv.FormatFloat(c.TakerSellVolume, 'g', -1, 64),
		c.Source,
		c.AggregateMethod,
		candleInt(c.FetchedAt),
		candleInt(c.RequestedAt),
		candleInt(c.LastTradeAt),
		candleInt(c.KlineEventAt),
		candleInt(c.SideUnknownCount),
		candleInt(c.LateTradeCount),
		candleBool(c.HasGap),
	)

	return strings.Join(fields, candleMemberSep), nil
}

// decodeCandleMember reconstructs the candle from the member. It is BIMODAL for as long as
// the rollout lasts: a member starting with `{` is the legacy form, which still occupies
// every key not yet rewritten.
func decodeCandleMember(raw string) (candle.Candle, error) {
	var c candle.Candle

	if raw == "" {
		return c, &CandleMemberDecodeError{
			Reason: "empty member", cause: ErrCandleMemberMalformed,
		}
	}
	if raw[0] == '{' {
		var w candleLegacyWire
		if err := json.Unmarshal([]byte(raw), &w); err != nil {
			return candle.Candle{}, &CandleMemberDecodeError{
				Version: "json", Reason: err.Error(), Sample: candleMemberSample(raw),
				cause: ErrCandleMemberMalformed,
			}
		}
		return candleLegacyWireToCandle(w), nil
	}

	fields := strings.Split(raw, candleMemberSep)
	if fields[0] != candleMemberVersion {
		return c, &CandleMemberDecodeError{
			Version: fields[0],
			Reason:  fmt.Sprintf("this binary only reads %q", candleMemberVersion),
			Sample:  candleMemberSample(raw),
			cause:   ErrCandleMemberUnknownVersion,
		}
	}
	if len(fields) != candleMemberFieldCount {
		return c, &CandleMemberDecodeError{
			Version: fields[0],
			Reason:  fmt.Sprintf("%d fields, want %d", len(fields), candleMemberFieldCount),
			Sample:  candleMemberSample(raw),
			cause:   ErrCandleMemberMalformed,
		}
	}

	// A parse error aborts the whole candle: a half-built candle enters the chart and the gap
	// validator as if it were a measurement.
	var bad error
	c.Timestamp, bad = candleParseInt(fields[1], bad)
	c.Open, bad = candleParseFloat(fields[2], bad)
	c.High, bad = candleParseFloat(fields[3], bad)
	c.Low, bad = candleParseFloat(fields[4], bad)
	c.Close, bad = candleParseFloat(fields[5], bad)
	c.Volume, bad = candleParseFloat(fields[6], bad)
	c.CreatedAt, bad = candleParseInt(fields[7], bad)
	c.LastUpdate, bad = candleParseInt(fields[8], bad)
	c.TakerBuyVolume, bad = candleParseFloat(fields[9], bad)
	c.TakerSellVolume, bad = candleParseFloat(fields[10], bad)
	c.Source = fields[11]
	c.AggregateMethod = fields[12]
	c.FetchedAt, bad = candleParseInt(fields[13], bad)
	c.RequestedAt, bad = candleParseInt(fields[14], bad)
	c.LastTradeAt, bad = candleParseInt(fields[15], bad)
	c.KlineEventAt, bad = candleParseInt(fields[16], bad)
	c.SideUnknownCount, bad = candleParseInt(fields[17], bad)
	c.LateTradeCount, bad = candleParseInt(fields[18], bad)
	c.HasGap, bad = candleParseBool(fields[19], bad)

	if bad != nil {
		return candle.Candle{}, &CandleMemberDecodeError{
			Version: fields[0], Reason: bad.Error(), Sample: candleMemberSample(raw),
			cause: ErrCandleMemberMalformed,
		}
	}

	return c, nil
}

func candleMemberSample(raw string) string {
	const limit = 120
	if len(raw) > limit {
		return raw[:limit] + "…"
	}
	return raw
}

func candleFloat(v float64) string {
	if v == 0 {
		return ""
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func candleInt(v int64) string {
	if v == 0 {
		return ""
	}
	return strconv.FormatInt(v, 10)
}

func candleBool(v bool) string {
	if v {
		return "1"
	}
	return ""
}

func candleParseFloat(raw string, prev error) (float64, error) {
	if prev != nil || raw == "" {
		return 0, prev
	}
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("float %q: %w", raw, err)
	}
	return parsed, nil
}

func candleParseInt(raw string, prev error) (int64, error) {
	if prev != nil || raw == "" {
		return 0, prev
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("int %q: %w", raw, err)
	}
	return parsed, nil
}

func candleParseBool(raw string, prev error) (bool, error) {
	if prev != nil {
		return false, prev
	}
	switch raw {
	case "":
		return false, nil
	case "1":
		return true, nil
	case "0":
		return false, nil
	default:
		return false, fmt.Errorf("bool %q: want empty, \"0\" or \"1\"", raw)
	}
}
