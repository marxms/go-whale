package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/marxms/go-whale/internal/derivatives"
)

// `derivatives:series` positional member v1.
//
// The member measured 344.2 B and 65.5% of that was field names and punctuation: `exchange`,
// `marketType` and `symbol` are ECHOES OF THE KEY — the key is
// `derivatives:series:{ex}:{mt}:{symbol}:{tf}`, so whoever read the member already knows the
// three. The positional member `v1|...` measures 91.5 B: **-252.7 B**.
//
// What these cases protect:
//   (a) field-by-field round-trip, including the fields that exist ONLY in the ZSET;
//   (b) mixed ZSET (legacy + positional) returns a contiguous series;
//   (c) unknown version => typed error, never a truncated series nor a half-built member.

// fullSnapshot returns a snapshot with EVERY field filled, including the fields that exist
// only in the ZSET window: losing one of them in the encoding is a permanent loss, not
// recomputable.
func fullSnapshot() *derivatives.DerivativesSnapshot {
	return &derivatives.DerivativesSnapshot{
		Exchange:                 "binance",
		MarketType:               "future",
		Symbol:                   "BTCUSDT",
		Timestamp:                1_787_586_300_000,
		ObservedAt:               1_787_586_301_234,
		Source:                   derivatives.SourceVenue,
		OpenInterestBase:         fptr(84_231.5),
		OpenInterestObservedAtMs: i64ptr(1_787_586_299_000),
		OpenInterest:             fptr(5_432_100_000.0),
		OIChangePctWindowMs:      i64ptr(300_000),
		FundingRate:              fptr(0.0001),
		NextFundingRate:          fptr(0.00012),
		FundingRate8h:            fptr(0.0001),
		FundingIntervalHours:     fptr(8),
		NStep:                    derivatives.NFromMeasuredSpacing,
		InterestRate:             fptr(0.0003),
		FundingAtAnchor:          bptr(true),
		LongShortRatio:           fptr(1.8342),
		LSLongPct:                fptr(64.71),
		LSShortPct:               fptr(35.29),
	}
}

func bptr(v bool) *bool { return &v }

func identityOf(s *derivatives.DerivativesSnapshot) derivativesSeriesIdentity {
	return derivativesSeriesIdentity{Exchange: s.Exchange, MarketType: s.MarketType, Symbol: s.Symbol}
}

// TestDerivativesSeriesMember_RoundTripEveryField is case (a): every field returns with the
// SAME value, and the identity and the instant come back from the key and the score — which
// is where they live.
func TestDerivativesSeriesMember_RoundTripEveryField(t *testing.T) {
	original := fullSnapshot()

	member, err := encodeDerivativesSeriesMember(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	got, err := decodeDerivativesSeriesMember(member, identityOf(original))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Field-by-field comparison via JSON: any field the positional encoding drops shows up as
	// a difference here, and that mutant is exactly what the test targets.
	wantJSON, _ := json.Marshal(original)
	gotJSON, _ := json.Marshal(got)
	if string(wantJSON) != string(gotJSON) {
		t.Fatalf("round-trip lost a field:\nwant %s\ngot %s", wantJSON, gotJSON)
	}
}

// TestDerivativesSeriesMember_ZsetOnlyFieldsSurvive names, one by one, the fields that have
// NO column in the durable tier. The round-trip above already covers them by JSON equality;
// this case exists so that losing any one of them fails with the field NAME in the error,
// not with a 400-character diff nobody reads.
func TestDerivativesSeriesMember_ZsetOnlyFieldsSurvive(t *testing.T) {
	original := fullSnapshot()
	member, err := encodeDerivativesSeriesMember(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeDerivativesSeriesMember(member, identityOf(original))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	coverage := DerivativesDurableCoverage()
	var wantJSON, gotJSON map[string]any
	raw, _ := json.Marshal(original)
	_ = json.Unmarshal(raw, &wantJSON)
	raw, _ = json.Marshal(got)
	_ = json.Unmarshal(raw, &gotJSON)

	// `oiChangePct` does not reach the ZSET (dropPollToPollOIChange), so it is not in the list.
	var semColuna int
	for field, column := range coverage {
		if column != "" || field == "oiChangePct" {
			continue
		}
		semColuna++
		if _, ok := gotJSON[field]; !ok {
			t.Errorf("field %q disappeared in the round-trip — it has no durable column, so the loss is permanent", field)
			continue
		}
		if gotJSON[field] != wantJSON[field] {
			t.Errorf("field %q = %v, want %v", field, gotJSON[field], wantJSON[field])
		}
	}
	// No field that reaches the ZSET has a permanent loss — `oiChangePct`, the only one still
	// without a column, does not reach the ZSET. The ratchet stays exact: it exists so that a
	// change in the declaration does NOT go unnoticed here, and the loop above keeps checking
	// the round-trip of every field it lists.
	if semColuna != 0 {
		t.Fatalf("fields without a durable column that reach the ZSET = %d, want 0 — the list changed and this case did not follow", semColuna)
	}
}

// TestDerivativesSeriesMember_AbsentOptionalStaysAbsent is the spec's edge case: "an absent
// optional field is read as absent, not as zero". A null `openInterest` becoming 0 would be a
// zero notional served as a measurement.
func TestDerivativesSeriesMember_AbsentOptionalStaysAbsent(t *testing.T) {
	sparse := &derivatives.DerivativesSnapshot{
		Exchange:    "binance",
		MarketType:  "future",
		Symbol:      "ETHUSDT",
		Timestamp:   1_787_586_300_000,
		FundingRate: fptr(0),
	}

	member, err := encodeDerivativesSeriesMember(sparse)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeDerivativesSeriesMember(member, identityOf(sparse))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.OpenInterest != nil {
		t.Errorf("absent openInterest came back as %v — absent became a value", *got.OpenInterest)
	}
	if got.OpenInterestBase != nil {
		t.Errorf("absent openInterestBase came back as %v", *got.OpenInterestBase)
	}
	if got.FundingAtAnchor != nil {
		t.Errorf("absent fundingAtAnchor came back as %v", *got.FundingAtAnchor)
	}
	if got.LSLongPct != nil {
		t.Errorf("absent lsLongPct came back as %v", *got.LSLongPct)
	}
	// And the PRESENT zero stays present: it is the other side of the same distinction.
	if got.FundingRate == nil || *got.FundingRate != 0 {
		t.Errorf("fundingRate = %v, want a present 0 — a present zero became absent", got.FundingRate)
	}
	if got.ObservedAt != 0 {
		t.Errorf("observedAt = %d, want 0", got.ObservedAt)
	}
	if got.NStep != "" {
		t.Errorf("nStep = %q, want empty", got.NStep)
	}
}

// TestDerivativesSeriesMember_KeyAndScoreStopBeingEchoed measures that the member stops
// repeating the key and the score, and that the gain is most of the byte.
func TestDerivativesSeriesMember_KeyAndScoreStopBeingEchoed(t *testing.T) {
	snap := fullSnapshot()

	legacy, _ := json.Marshal(snap)
	member, err := encodeDerivativesSeriesMember(snap)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// The identity that comes from the KEY leaves; the `timestamp`, which is the MEMBER's
	// identity, stays.
	for _, echo := range []string{"binance", "future", "BTCUSDT", "exchange", "symbol", "timestamp"} {
		if strings.Contains(member, echo) {
			t.Errorf("the member still echoes %q: %s", echo, member)
		}
	}
	if !strings.Contains(member, "1787586300000") {
		t.Errorf("the member lost the timestamp, which is its identity in the ZSET: %s", member)
	}

	t.Logf("legacy member = %d B · positional member = %d B · gain = %d B",
		len(legacy), len(member), len(legacy)-len(member))

	// The fleet average projects 344.2 -> 91.5 B. This is ONE member with every field filled,
	// so the absolute number differs; what must hold is the order of magnitude of the savings.
	// Below a 60% cut the encoding did not pay for the change.
	if len(member)*10 > len(legacy)*4 {
		t.Errorf("positional member = %d B against %d B of the legacy — %.1f%% cut, want >=60%%",
			len(member), len(legacy), 100*(1-float64(len(member))/float64(len(legacy))))
	}
}

// TestGetSnapshotSeries_MixedEncodingsAreContiguous is case (b). A Redis restarted during the
// rollout restores from the RDB a ZSET with both forms; the series read must be contiguous,
// ordered and complete.
func TestGetSnapshotSeries_MixedEncodingsAreContiguous(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1h")
	key := derivSeriesZsetKey("binance", "future", "BTCUSDT", "1h")

	legacy := fullSnapshot()
	legacy.Timestamp = 1_787_586_300_000
	legacyRaw, _ := json.Marshal(legacy)

	novo := fullSnapshot()
	novo.Timestamp = 1_787_589_900_000
	novo.OpenInterestBase = fptr(84_999.25)
	novoRaw, err := encodeDerivativesSeriesMember(novo)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if err := client.ZAdd(ctx, key,
		redis.Z{Score: float64(legacy.Timestamp), Member: string(legacyRaw)},
		redis.Z{Score: float64(novo.Timestamp), Member: novoRaw},
	).Err(); err != nil {
		t.Fatalf("zadd: %v", err)
	}

	series, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1h", 0)
	if err != nil {
		t.Fatalf("GetSnapshotSeries: %v", err)
	}
	if len(series) != 2 {
		t.Fatalf("series with %d points, want 2 — the bimodal reader discarded one of the forms", len(series))
	}
	// ZREVRANGE: newest first.
	if series[0].Timestamp != novo.Timestamp || series[1].Timestamp != legacy.Timestamp {
		t.Fatalf("order/timestamps = %d,%d — want %d,%d",
			series[0].Timestamp, series[1].Timestamp, novo.Timestamp, legacy.Timestamp)
	}
	if series[0].OpenInterestBase == nil || *series[0].OpenInterestBase != 84_999.25 {
		t.Fatalf("positional member lost openInterestBase: %+v", series[0])
	}
	if series[1].OpenInterestBase == nil || *series[1].OpenInterestBase != 84_231.5 {
		t.Fatalf("legacy member lost openInterestBase: %+v", series[1])
	}
	for i, s := range series {
		if s.Exchange != "binance" || s.MarketType != "future" || s.Symbol != "BTCUSDT" {
			t.Fatalf("point %d lost the identity that comes from the key: %+v", i, s)
		}
	}
}

// TestDerivativesSeriesMember_UnknownVersionIsTypedError is case (c) in the decoder.
func TestDerivativesSeriesMember_UnknownVersionIsTypedError(t *testing.T) {
	ident := derivativesSeriesIdentity{Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT"}

	_, err := decodeDerivativesSeriesMember("v9|1|venue|2|3", ident)
	if err == nil {
		t.Fatal("unknown version decoded — the half-built member is the defect")
	}
	if !errors.Is(err, ErrDerivativesSeriesUnknownVersion) {
		t.Fatalf("error = %v, want ErrDerivativesSeriesUnknownVersion", err)
	}
	var decodeErr *DerivativesSeriesDecodeError
	if !errors.As(err, &decodeErr) {
		t.Fatalf("error %v is not *DerivativesSeriesDecodeError — the error must be TYPED", err)
	}
	if decodeErr.Version != "v9" {
		t.Errorf("Version = %q, want \"v9\" — the error must say WHICH version it does not know", decodeErr.Version)
	}
}

// TestGetSnapshotSeries_UnknownVersionFailsLoudInsteadOfTruncating covers the rollback case:
// reverting the streamer IMAGE puts the N-1 reader in front of version N members. Returning
// the ones it understands serves a short series nobody can detect; the image comes back, the
// data already in Redis does not.
func TestGetSnapshotSeries_UnknownVersionFailsLoudInsteadOfTruncating(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1h")
	key := derivSeriesZsetKey("binance", "future", "BTCUSDT", "1h")

	lido := fullSnapshot()
	lidoRaw, err := encodeDerivativesSeriesMember(lido)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := client.ZAdd(ctx, key,
		redis.Z{Score: float64(lido.Timestamp), Member: lidoRaw},
		redis.Z{Score: float64(lido.Timestamp + 3_600_000), Member: "v2|" + lidoRaw},
	).Err(); err != nil {
		t.Fatalf("zadd: %v", err)
	}

	series, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1h", 0)
	if err == nil {
		t.Fatalf("read returned %d point(s) with no error — the truncated series is the failure mode this case kills", len(series))
	}
	if !errors.Is(err, ErrDerivativesSeriesUnknownVersion) {
		t.Fatalf("error = %v, want ErrDerivativesSeriesUnknownVersion", err)
	}
	if series != nil {
		t.Fatalf("partial series returned along with the error: %d points", len(series))
	}
}

// TestUpsertSeries_UnknownVersionAbortsInsteadOfOverwriting is the WRITE face of the same
// risk, and it is the one that loses data: the field-by-field merge reads the bucket member
// before writing. A member this reader does not understand, treated as "does not exist",
// makes the upsert write the new snapshot alone OVER it — every field the current cycle does
// not carry disappears. It is the same class as a failing pre-merge read.
func TestUpsertSeries_UnknownVersionAbortsInsteadOfOverwriting(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1h")
	key := derivSeriesZsetKey("binance", "future", "BTCUSDT", "1h")

	futuro := "v2|field|unknown"
	bucket := bucketAlignMs(1_787_586_300_000, "1h")
	if err := client.ZAdd(ctx, key, redis.Z{Score: float64(bucket), Member: futuro}).Err(); err != nil {
		t.Fatalf("zadd: %v", err)
	}

	incoming := &derivatives.DerivativesSnapshot{
		Exchange:    "binance",
		MarketType:  "future",
		Symbol:      "BTCUSDT",
		Timestamp:   bucket + 60_000,
		FundingRate: fptr(0.0002),
	}
	err := store.SaveSeriesOnly(ctx, incoming, "1h")
	if err == nil {
		t.Fatal("upsert proceeded over a member the reader does not understand")
	}
	if !errors.Is(err, ErrDerivativesSeriesUnknownVersion) {
		t.Fatalf("error = %v, want ErrDerivativesSeriesUnknownVersion", err)
	}

	members, mErr := client.ZRange(ctx, key, 0, -1).Result()
	if mErr != nil {
		t.Fatalf("zrange: %v", mErr)
	}
	if len(members) != 1 || members[0] != futuro {
		t.Fatalf("bucket member = %v — the unknown one had to remain intact", members)
	}
}

// TestRecomputeBaseline_ReadsTimestampFromScore proves the instant survives leaving the
// member: the ruler orders and dates by timestamps, and with the timestamp absent from the
// member they must come from the score. Without this, every point would have timestamp zero
// and the baseline window would collapse.
func TestRecomputeBaseline_ReadsTimestampFromScore(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestDerivStore(t, "1h")

	// Aligned to 1h on purpose: `SaveSeriesOnly` aligns to the bucket, and the written score is
	// the bucket. An unaligned base would measure alignment, not the instant round-trip.
	base := bucketAlignMs(1_787_586_300_000, "1h")
	for i := range 12 {
		snap := &derivatives.DerivativesSnapshot{
			Exchange:         "binance",
			MarketType:       "future",
			Symbol:           "BTCUSDT",
			Timestamp:        base + int64(i)*3_600_000,
			OpenInterestBase: fptr(80_000 + float64(i)*10),
		}
		if err := store.SaveSeriesOnly(ctx, snap, "1h"); err != nil {
			t.Fatalf("SaveSeriesOnly %d: %v", i, err)
		}
	}

	series, err := store.GetSnapshotSeries(ctx, "binance", "future", "BTCUSDT", "1h", 0)
	if err != nil {
		t.Fatalf("GetSnapshotSeries: %v", err)
	}
	if len(series) != 12 {
		t.Fatalf("series with %d points, want 12", len(series))
	}
	for i, s := range series {
		want := base + int64(11-i)*3_600_000
		if s.Timestamp != want {
			t.Fatalf("point %d with timestamp %d, want %d — the instant did not come back from the score", i, s.Timestamp, want)
		}
	}

	baseline, err := store.RecomputeBaseline(ctx, "binance", "future", "BTCUSDT", "1h")
	if err != nil {
		t.Fatalf("RecomputeBaseline: %v", err)
	}
	if baseline.OpenInterest == nil || baseline.OpenInterest.N == 0 {
		t.Fatal("baseline with n=0 — the ruler saw no increment, which is the symptom of a lost timestamp")
	}
	if baseline.OpenInterest.SpanMs == 0 {
		t.Fatal("baseline with spanMs=0 — the window collapsed, which is what happens when every point comes with the same instant")
	}
}

// TestUpsertSeries_IdenticalReadingsInDifferentBucketsStayTwoMembers covers the identity
// case for this family.
//
// `timestamp` looks like an echo of the score, but without it in the member an illiquid symbol
// — open interest, funding and LSR unchanged between buckets, no `observedAt`, which is the
// bootstrap case — produces the SAME member in different buckets. In a ZSET the member is the
// identity, so the second `ZADD` MOVES the first one's score instead of inserting, and the old
// point disappears. Measured: three buckets, `ZCARD` = 1.
func TestUpsertSeries_IdenticalReadingsInDifferentBucketsStayTwoMembers(t *testing.T) {
	ctx := context.Background()
	store, client := newTestDerivStore(t, "1h")

	buckets := []int64{1_787_583_600_000, 1_787_587_200_000, 1_787_590_800_000}
	for _, ts := range buckets {
		snap := &derivatives.DerivativesSnapshot{
			Exchange: "binance", MarketType: "future", Symbol: "PARADO",
			Timestamp:        ts,
			OpenInterestBase: fptr(1000),
			FundingRate:      fptr(0.0001),
		}
		if err := store.SaveSeriesOnly(ctx, snap, "1h"); err != nil {
			t.Fatalf("SaveSeriesOnly %d: %v", ts, err)
		}
	}

	card, err := client.ZCard(ctx, derivSeriesZsetKey("binance", "future", "PARADO", "1h")).Result()
	if err != nil {
		t.Fatalf("zcard: %v", err)
	}
	if card != 3 {
		t.Fatalf("ZCARD = %d, want 3 — identical readings in different buckets collided and "+
			"ZADD moved the score instead of inserting", card)
	}

	series, err := store.GetSnapshotSeries(ctx, "binance", "future", "PARADO", "1h", 0)
	if err != nil {
		t.Fatalf("GetSnapshotSeries: %v", err)
	}
	if len(series) != 3 {
		t.Fatalf("series with %d points, want 3", len(series))
	}
	for i, ts := range []int64{buckets[2], buckets[1], buckets[0]} {
		if series[i].Timestamp != ts {
			t.Fatalf("point %d with timestamp %d, want %d", i, series[i].Timestamp, ts)
		}
	}
}

// ============================================================================
// FINITENESS ON BOTH SIDES OF THE CODEC
// ============================================================================
//
// The codec is POSITIONAL and does not go through JSON: `formatFloatPtr` =
// `strconv.FormatFloat(*v, 'g', -1, 64)`, which formats `+Inf` and `NaN` successfully. This is
// how non-finite values ENTER the series instead of failing it — and the backfill path, which
// does not use JSON, depends on exactly this point. On the other side, `parseFloatPtr` accepts
// the same literals and would return them to the consumer.

func TestFormatFloatPtr_NonFiniteIsWrittenAsAbsent(t *testing.T) {
	t.Parallel()

	naoFinitos := map[string]float64{
		"+Inf": math.Inf(1),
		"-Inf": math.Inf(-1),
		"NaN":  math.NaN(),
	}
	for nome, v := range naoFinitos {
		if got := formatFloatPtr(&v); got != "" {
			t.Fatalf("formatFloatPtr(%s) = %q, want empty — the field must stay ABSENT",
				nome, got)
		}
	}

	// CONTRAST, and the part a badly written guard would break: a finite value is still
	// written, and ZERO stays zero — absence and zero are not the same thing in this format,
	// and confusing them would trade a zero delta for a field that never arrived.
	if got := formatFloatPtr(fptr(0)); got != "0" {
		t.Fatalf("formatFloatPtr(0) = %q, want \"0\" — zero is not absence", got)
	}
	if got := formatFloatPtr(fptr(0.0001)); got != "0.0001" {
		t.Fatalf("formatFloatPtr(0.0001) = %q, want \"0.0001\"", got)
	}
	if got := formatFloatPtr(nil); got != "" {
		t.Fatalf("formatFloatPtr(nil) = %q, want empty", got)
	}
}

func TestParseFloatPtr_NonFiniteIsRejected(t *testing.T) {
	t.Parallel()

	for _, literal := range []string{"Inf", "+Inf", "-Inf", "Infinity", "-Infinity", "NaN"} {
		v, err := parseFloatPtr(literal, nil)
		if err == nil {
			t.Fatalf("parseFloatPtr(%q) accepted the literal and returned %v — the non-finite "+
				"value crossed the read", literal, *v)
		}
		if v != nil {
			t.Fatalf("parseFloatPtr(%q) returned value %v along with an error", literal, *v)
		}
	}

	// CONTRAST: a finite literal is still read, and zero stays present.
	if v, err := parseFloatPtr("0.0001", nil); err != nil || v == nil || *v != 0.0001 {
		t.Fatalf("parseFloatPtr(\"0.0001\") = (%v, %v), want (0.0001, nil)", v, err)
	}
	if v, err := parseFloatPtr("0", nil); err != nil || v == nil || *v != 0 {
		t.Fatalf("parseFloatPtr(\"0\") = (%v, %v), want (0, nil)", v, err)
	}
}

// The whole member: the non-finite literal in the funding slot is not served as a number.
// The case pins POSITION 9 and checks its premise — if the format changes order, the premise
// fails loud instead of the case passing vacuously.
func TestDecodeDerivativesSeriesMember_NonFiniteSlotIsAMalformedMember(t *testing.T) {
	t.Parallel()

	const posFundingRate = 9

	original := fullSnapshot()
	raw, err := encodeDerivativesSeriesMember(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	slots := strings.Split(raw, derivativesSeriesSep)
	if len(slots) <= posFundingRate {
		t.Fatalf("member with %d fields, want more than %d", len(slots), posFundingRate)
	}
	if got := slots[posFundingRate]; got != "0.0001" {
		t.Fatalf("position %d holds %q, and the case's premise is that it holds the funding "+
			"(\"0.0001\") — the format changed order", posFundingRate, got)
	}

	for _, literal := range []string{"+Inf", "-Inf", "NaN"} {
		mutado := make([]string, 0, len(slots))
		mutado = append(mutado, slots[:posFundingRate]...)
		mutado = append(mutado, literal)
		mutado = append(mutado, slots[posFundingRate+1:]...)

		dec, err := decodeDerivativesSeriesMember(strings.Join(mutado, derivativesSeriesSep), identityOf(original))
		if err == nil {
			t.Fatalf("%q in the funding slot decoded (%v) — the literal crossed the read",
				literal, dec)
		}
		if !errors.Is(err, ErrDerivativesSeriesMalformedMember) {
			t.Fatalf("%q gave error %v, want ErrDerivativesSeriesMalformedMember", literal, err)
		}
		if dec != nil {
			t.Fatalf("%q returned a half-built snapshot: %v", literal, dec)
		}
	}
}
