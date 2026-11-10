package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/candle"
)

// 56.6% of the candle member is field names and punctuation: 256.0 B measured, of which ~145
// are `"takerBuyVolume":`, `"aggregateMethod":` and the sibling keys, repeated in each of the
// ~521 members of each of the 4,024 keys. The positional member costs 109.2 B: **-146.7 B**.
//
// `timestamp` stays INSIDE the member because it is the member's identity in the ZSET —
// without it, two candles with the same OHLCV collapse into one. See the codec block.

// productionCandle is a real candle from the live path, with the optionals the mutant of this
// test takes down: `takerBuyVolume`/`takerSellVolume` (which have NO omitempty on purpose —
// zero is a market state, not absence), `klineEventAt` and `source`.
func productionCandle() candle.Candle {
	return candle.Candle{
		Timestamp:        1_787_586_300_000,
		Open:             112_483.21,
		High:             112_640.00,
		Low:              112_401.55,
		Close:            112_598.74,
		Volume:           1_842.3317,
		CreatedAt:        1_787_586_300_118,
		LastUpdate:       1_787_586_599_942,
		TakerBuyVolume:   961.4402,
		TakerSellVolume:  880.8915,
		Source:           "live",
		AggregateMethod:  "aggTrade",
		FetchedAt:        1_787_586_601_004,
		RequestedAt:      1_787_586_600_812,
		LastTradeAt:      1_787_586_599_931,
		KlineEventAt:     1_787_586_599_977,
		SideUnknownCount: 3,
		LateTradeCount:   1,
		HasGap:           true,
	}
}

// TestCandleMember_RoundTripEveryField covers `candles`: a real production candle comes back
// field by field, with every field intact.
func TestCandleMember_RoundTripEveryField(t *testing.T) {
	original := productionCandle()

	member, err := encodeCandleMember(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	got, err := decodeCandleMember(member)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got != original {
		t.Fatalf("round-trip lost a field:\nwant %+v\ngot %+v", original, got)
	}
}

// TestCandleMember_OptionalsSurviveOneByOne names each optional. The round-trip above already
// covers them by struct equality; this case exists so that losing ONE of them fails with the
// field name. The mutant is "drop an optional field in the positional encoding", and it only
// shows up in a candle that has them.
func TestCandleMember_OptionalsSurviveOneByOne(t *testing.T) {
	original := productionCandle()
	member, err := encodeCandleMember(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeCandleMember(member)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	casos := []struct {
		campo      string
		quero, tem any
	}{
		{"takerBuyVolume", original.TakerBuyVolume, got.TakerBuyVolume},
		{"takerSellVolume", original.TakerSellVolume, got.TakerSellVolume},
		{"klineEventAt", original.KlineEventAt, got.KlineEventAt},
		{"source", original.Source, got.Source},
		{"aggregateMethod", original.AggregateMethod, got.AggregateMethod},
		{"fetchedAt", original.FetchedAt, got.FetchedAt},
		{"requestedAt", original.RequestedAt, got.RequestedAt},
		{"lastTradeAt", original.LastTradeAt, got.LastTradeAt},
		{"sideUnknownCount", original.SideUnknownCount, got.SideUnknownCount},
		{"lateTradeCount", original.LateTradeCount, got.LateTradeCount},
		{"hasGap", original.HasGap, got.HasGap},
		{"createdAt", original.CreatedAt, got.CreatedAt},
		{"lastUpdate", original.LastUpdate, got.LastUpdate},
	}
	for _, c := range casos {
		if c.tem != c.quero {
			t.Errorf("field %q = %v, want %v", c.campo, c.tem, c.quero)
		}
	}
}

// TestCandleMember_MeasuredZeroSurvivesTheEncoding is the reason
// `takerBuyVolume`/`takerSellVolume` are declared WITHOUT omitempty: zero is a measured market
// state — a bar with all aggression on one side — and ~1% of the fleet's bars are in that
// state (measured: 2,567 of 305,269 5m bars with taker-buy at zero). An encoding that treated
// those two zeros as "did not arrive" would erase precisely the bars with the strongest
// signal.
func TestCandleMember_MeasuredZeroSurvivesTheEncoding(t *testing.T) {
	c := productionCandle()
	c.TakerBuyVolume = 0
	c.TakerSellVolume = 0

	member, err := encodeCandleMember(c)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// The position of both is FILLED, not empty — the same decision `json` makes by not putting
	// omitempty on them, transposed to the positional encoding.
	fields := strings.Split(member, "|")
	if len(fields) < 10 {
		t.Fatalf("member with %d fields: %s", len(fields), member)
	}
	if fields[9] == "" || fields[10] == "" {
		t.Errorf("empty taker buy/sell on a measured-zero bar: %s", member)
	}

	got, err := decodeCandleMember(member)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != c {
		t.Fatalf("round-trip of the measured-zero bar diverged: %+v", got)
	}
}

// TestCandleMember_ScoreStopsBeingEchoed measures that the score stops being echoed.
func TestCandleMember_ScoreStopsBeingEchoed(t *testing.T) {
	c := productionCandle()

	legacy, _ := json.Marshal(legacyWireFromCandle(c))
	member, err := encodeCandleMember(c)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// The `timestamp` STAYS: it is the member's identity in the ZSET. What leaves is its NAME.
	if !strings.Contains(member, "1787586300000") {
		t.Errorf("the member lost the timestamp, which is its identity in the ZSET: %s", member)
	}
	for _, name := range []string{"timestamp", "takerBuyVolume", "aggregateMethod", "hasGap"} {
		if strings.Contains(member, name) {
			t.Errorf("the member still carries the field name %q: %s", name, member)
		}
	}

	t.Logf("legacy member = %d B · positional member = %d B · gain = %d B",
		len(legacy), len(member), len(legacy)-len(member))

	// The fleet average projects 256.0 -> 109.2 B (a 57.3% cut), but that projection counted on
	// the `timestamp` leaving, which its identity forbids — see the codec block. With its 14 B
	// back, the honest target is ~123 B, a ~52% cut. This is ONE member with every field
	// filled, so the absolute differs; the ratio is what must hold.
	if len(member)*10 > len(legacy)*5 {
		t.Errorf("positional member = %d B against %d B of the legacy — %.1f%% cut, want >=50%%",
			len(member), len(legacy), 100*(1-float64(len(member))/float64(len(legacy))))
	}
}

// TestGetCandlesDesc_MixedEncodingsAreContiguous: a Redis restored from the RDB in the middle
// of the rollout returns a ZSET with both forms, and the series read must be contiguous,
// ordered and complete.
func TestGetCandlesDesc_MixedEncodingsAreContiguous(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := &RedisStore{client: client}

	key := candlesKeyV2("binance", "future", "BTCUSDT", "5m")

	legacy := productionCandle()
	legacy.Close = 111_000
	legacyRaw, _ := json.Marshal(legacyWireFromCandle(legacy))

	novo := productionCandle()
	novo.Timestamp = legacy.Timestamp + 300_000
	novo.Close = 112_000
	novoRaw, err := encodeCandleMember(novo)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if err := client.ZAdd(ctx, key,
		redis.Z{Score: float64(legacy.Timestamp), Member: string(legacyRaw)},
		redis.Z{Score: float64(novo.Timestamp), Member: novoRaw},
	).Err(); err != nil {
		t.Fatalf("zadd: %v", err)
	}

	desc, err := store.GetCandlesDesc(ctx, "binance", "future", "BTCUSDT", "5m", 10)
	if err != nil {
		t.Fatalf("GetCandlesDesc: %v", err)
	}
	if len(desc) != 2 {
		t.Fatalf("series with %d candles, want 2 — the bimodal reader discarded one of the forms", len(desc))
	}
	if desc[0].Timestamp != novo.Timestamp || desc[0].Close != 112_000 {
		t.Fatalf("newest candle = %+v", desc[0])
	}
	if desc[1].Timestamp != legacy.Timestamp || desc[1].Close != 111_000 {
		t.Fatalf("legacy candle = %+v", desc[1])
	}
	if desc[0].TakerBuyVolume != novo.TakerBuyVolume || desc[1].TakerBuyVolume != legacy.TakerBuyVolume {
		t.Fatalf("taker-buy did not cross one of the encodings: %v / %v",
			desc[0].TakerBuyVolume, desc[1].TakerBuyVolume)
	}

	asc, err := store.GetCandlesAsc(ctx, "binance", "future", "BTCUSDT", "5m", 10)
	if err != nil {
		t.Fatalf("GetCandlesAsc: %v", err)
	}
	if len(asc) != 2 || asc[0].Timestamp != legacy.Timestamp || asc[1].Timestamp != novo.Timestamp {
		t.Fatalf("ascending order broke: %+v", asc)
	}

	// ZREMRANGEBYRANK caps in the same order, because the score did not move.
	byScore, err := store.GetCandlesByScoreRange(ctx, "binance", "future", "BTCUSDT", "5m",
		legacy.Timestamp, novo.Timestamp)
	if err != nil {
		t.Fatalf("GetCandlesByScoreRange: %v", err)
	}
	if len(byScore) != 2 {
		t.Fatalf("score read returned %d candles, want 2", len(byScore))
	}
}

// TestCandleMember_UnknownVersionIsTypedError: the N-1 reader in front of the version N member
// fails LOUD. Returning the candles it understands would serve a short window nobody can
// detect — and the short window is read by the gap validator, which would read it as a hole.
func TestCandleMember_UnknownVersionIsTypedError(t *testing.T) {
	_, err := decodeCandleMember("v9|1|2|3")
	if err == nil {
		t.Fatal("unknown version decoded")
	}
	if !errors.Is(err, ErrCandleMemberUnknownVersion) {
		t.Fatalf("error = %v, want ErrCandleMemberUnknownVersion", err)
	}
	var decodeErr *CandleMemberDecodeError
	if !errors.As(err, &decodeErr) {
		t.Fatalf("error %v is not *CandleMemberDecodeError — the error must be TYPED", err)
	}
	if decodeErr.Version != "v9" {
		t.Errorf("Version = %q, want \"v9\"", decodeErr.Version)
	}
}

func TestGetCandlesDesc_UnknownVersionFailsLoudInsteadOfTruncating(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := &RedisStore{client: client}

	key := candlesKeyV2("binance", "future", "BTCUSDT", "5m")
	lida := productionCandle()
	lidaRaw, err := encodeCandleMember(lida)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := client.ZAdd(ctx, key,
		redis.Z{Score: float64(lida.Timestamp), Member: lidaRaw},
		redis.Z{Score: float64(lida.Timestamp + 300_000), Member: "v2|" + lidaRaw},
	).Err(); err != nil {
		t.Fatalf("zadd: %v", err)
	}

	got, err := store.GetCandlesDesc(ctx, "binance", "future", "BTCUSDT", "5m", 10)
	if err == nil {
		t.Fatalf("read returned %d candle(s) with no error — the short window is the failure mode this case kills", len(got))
	}
	if !errors.Is(err, ErrCandleMemberUnknownVersion) {
		t.Fatalf("error = %v, want ErrCandleMemberUnknownVersion", err)
	}
	if got != nil {
		t.Fatalf("partial series returned along with the error: %d candles", len(got))
	}
}

// TestWriteSnapshotBatch_WritesThePositionalMember covers the leg that writes every 5 s.
func TestWriteSnapshotBatch_WritesThePositionalMember(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := &RedisStore{client: client}

	c := productionCandle()
	if err := store.WriteSnapshotBatch(ctx, []CandleUpdate{{
		Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT", Timeframe: "5m", Candle: c,
	}}); err != nil {
		t.Fatalf("WriteSnapshotBatch: %v", err)
	}

	key := candlesKeyV2("binance", "future", "BTCUSDT", "5m")
	members, err := client.ZRange(ctx, key, 0, -1).Result()
	if err != nil {
		t.Fatalf("zrange: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("%d members, want 1", len(members))
	}
	if strings.HasPrefix(members[0], "{") {
		t.Fatalf("the snapshot leg still writes JSON: %s", members[0])
	}

	back, err := store.GetCandlesDesc(ctx, "binance", "future", "BTCUSDT", "5m", 1)
	if err != nil {
		t.Fatalf("GetCandlesDesc: %v", err)
	}
	if len(back) != 1 || back[0] != c {
		t.Fatalf("the candle did not come back identical: %+v", back)
	}
}

// storedCandleAt reads ONE member of the key and decodes it with the PRODUCTION decoder.
//
// It exists because three cases in this package reimplemented the parsing with `json.Unmarshal`
// over the raw member. The member is positional and the instant comes from the score, so a test
// that reimplements the format starts asserting against an encoding the writer does not emit —
// and would do so silently, because what it measures (source, taker, timestamp) still exists.
func storedCandleAt(t *testing.T, client *redis.Client, key string, rangeBy *redis.ZRangeBy) candle.Candle {
	t.Helper()

	members, err := client.ZRangeByScoreWithScores(context.Background(), key, rangeBy).Result()
	if err != nil {
		t.Fatalf("zrangebyscore %s: %v", key, err)
	}
	if len(members) != 1 {
		t.Fatalf("key %s returned %d members, want 1", key, len(members))
	}
	raw, _ := members[0].Member.(string)
	c, decErr := decodeCandleMember(raw)
	if decErr != nil {
		t.Fatalf("decode of the member of %s: %v", key, decErr)
	}
	return c
}

// TestWriteSnapshotBatch_IdenticalBarsInDifferentBucketsStayTwoMembers covers the identity
// case for `candles`.
//
// A bar with no trades has `open == high == low == close` at the last price, zero volume and
// taker 0/0; the batch is written with a single `now`, which also makes `createdAt` and
// `lastUpdate` equal. Without the `timestamp` INSIDE the member, those two candles are the same
// member, and the second `ZADD` moves the first one's score instead of inserting. Measured:
// `ZCARD` = 1, and the 50-bucket rig of
// `TestTieredStore_GetCandlesByScoreRange_NoColdTier` returned 1 candle.
func TestWriteSnapshotBatch_IdenticalBarsInDifferentBucketsStayTwoMembers(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := &RedisStore{client: client}

	parada := candle.Candle{
		Open: 100, High: 100, Low: 100, Close: 100,
		Volume: 0, TakerBuyVolume: 0, TakerSellVolume: 0,
		CreatedAt: 7, LastUpdate: 7, Source: "live",
	}
	a, b := parada, parada
	a.Timestamp = 1_787_586_300_000
	b.Timestamp = 1_787_586_600_000

	if err := store.WriteSnapshotBatch(ctx, []CandleUpdate{
		{Exchange: "binance", MarketType: "future", Symbol: "PARADO", Timeframe: "5m", Candle: a},
		{Exchange: "binance", MarketType: "future", Symbol: "PARADO", Timeframe: "5m", Candle: b},
	}); err != nil {
		t.Fatalf("WriteSnapshotBatch: %v", err)
	}

	card, err := client.ZCard(ctx, candlesKeyV2("binance", "future", "PARADO", "5m")).Result()
	if err != nil {
		t.Fatalf("zcard: %v", err)
	}
	if card != 2 {
		t.Fatalf("ZCARD = %d, want 2 — two no-trade bars in different buckets collided "+
			"and ZADD moved the score instead of inserting", card)
	}

	got, err := store.GetCandlesAsc(ctx, "binance", "future", "PARADO", "5m", 10)
	if err != nil {
		t.Fatalf("GetCandlesAsc: %v", err)
	}
	if len(got) != 2 || got[0].Timestamp != a.Timestamp || got[1].Timestamp != b.Timestamp {
		t.Fatalf("series = %+v, want both bars in their own buckets", got)
	}
}
