package redisstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/marxms/go-whale/internal/derivatives"
)

// `derivatives:series` MEMBER ENCODING — POSITIONAL AND VERSIONED.
//
// # WHAT LEFT THE MEMBER, AND WHY IT ALREADY KNEW
//
// Measured: 344.2 B per member, of which **65.5% is field names and punctuation**. Four of
// the fields carried no information:
//
//   - `exchange`, `marketType`, `symbol` are ECHOES OF THE KEY. The key is
//     `derivatives:series:{ex}:{mt}:{symbol}:{tf}`, and whoever read the member necessarily
//     built or parsed that key to reach it. Repeating them 400 times per key, across 4,008
//     keys, pays for data the caller already holds. Inside ONE key the three are constant,
//     so their discriminating power between members is exactly zero.
//
// # `timestamp` STAYS, FOR THE SAME REASON THAT FORBIDS A BARE NUMBER IN THE INDICATOR
//
// The design classified it with the three above, as a "score echo". It is not. **In a ZSET
// the member is the IDENTITY**, and it is precisely `timestamp` that carries ALL the
// discriminating power between two members of the same key — the other three carry none.
// Without it, two buckets with the same magnitudes produce the same member, and `ZADD` on
// the second bucket MOVES the first one's score instead of inserting: the old point
// disappears.
//
// This is not hypothetical. Measured in this package: an illiquid symbol with open interest,
// funding and LSR unchanged between buckets — and no `observedAt`, which is the bootstrap
// case — collapses **three buckets into one** (`ZCARD` 3 -> 1). It is the same mutant that
// fails "silently only on repeated-value bars"; a repeated-value bar is the normal bar of a
// stalled asset.
//
// Price of the encoding, declared: the member is ~14 B larger than the projected 91.5 B. It
// is the cost of the key having more than one member.
//
// Eighteen magnitudes remain, and the member becomes `v1|` followed by them in fixed
// position.
//
// # WHY POSITIONAL AND NOT "JUST THE VALUES"
//
// An empty field means ABSENT, not zero. That is the distinction the spec requires at the
// edges ("an absent optional field is read as absent, not as zero"): a null `openInterest`
// coming back as 0 is a zero notional served as if it were a measurement. Therefore every
// optional is a pointer on both sides, and the empty position is the only way to say "did
// not arrive".
//
// # THE FAILURE RULE, AND WHY IT IS NOT ONE RULE
//
// Unknown version and corrupt member are different events and get different responses:
//
//   - UNKNOWN VERSION fails LOUD on every path. It only appears one way: a newer image
//     wrote `v2` and someone reverted the binary. The image comes back; the data already in
//     Redis does not. Returning the members this reader understands would serve a short
//     series nobody can detect — and, on the WRITE path, treating the member as nonexistent
//     would make the merge write the cycle's snapshot over it, erasing every field it does
//     not carry. It is the same class the pre-merge read failure refuses.
//   - CORRUPT MEMBER keeps the tolerance each caller already declared in writing (the
//     baseline ruler, for example, keeps discounting one observation and reporting the
//     smaller n). A stray member is not a format change.

const (
	// derivativesSeriesVersion is the version THIS binary writes.
	derivativesSeriesVersion = "v1"
	// derivativesSeriesFieldCount includes the version itself.
	derivativesSeriesFieldCount = 19
	// derivativesSeriesSep must not appear in any text field; the encoder refuses it.
	derivativesSeriesSep = "|"
)

// ErrDerivativesSeriesUnknownVersion marks a member written by a version this binary does
// not know. It is the error that prevents a series truncated by an image rollback.
var ErrDerivativesSeriesUnknownVersion = errors.New("derivatives: unknown member encoding version")

// ErrDerivativesSeriesMalformedMember marks the member with the right version and the wrong
// content — an extra field, a missing field, a number that does not parse.
var ErrDerivativesSeriesMalformedMember = errors.New("derivatives: malformed series member")

// DerivativesSeriesDecodeError is the typed error that says WHICH version appeared and
// carries a truncated sample, so that a log line does not take a whole payload.
type DerivativesSeriesDecodeError struct {
	Version string
	Reason  string
	Sample  string
	cause   error
}

func (e *DerivativesSeriesDecodeError) Error() string {
	return fmt.Sprintf("derivatives: unreadable series member (version %q): %s (sample: %s)",
		e.Version, e.Reason, e.Sample)
}

func (e *DerivativesSeriesDecodeError) Unwrap() error { return e.cause }

// derivativesSeriesIdentity is the part of the snapshot that lives in the KEY, not in the
// member.
type derivativesSeriesIdentity struct {
	Exchange   string
	MarketType string
	Symbol     string
}

// encodeDerivativesSeriesMember serializes the snapshot into the positional member.
//
// It refuses any text field containing the separator: a `source` with `|` would shift every
// following field and the member would decode as something else — silently, which is the
// failure mode the fixed position exists to prevent.
func encodeDerivativesSeriesMember(snap *derivatives.DerivativesSnapshot) (string, error) {
	if snap == nil {
		return "", fmt.Errorf("derivatives: encode of nil snapshot")
	}
	for name, text := range map[string]string{"source": snap.Source, "nStep": string(snap.NStep)} {
		if strings.Contains(text, derivativesSeriesSep) {
			return "", fmt.Errorf("derivatives: field %s contains the separator %q: %q", name, derivativesSeriesSep, text)
		}
	}

	fields := make([]string, 0, derivativesSeriesFieldCount)
	fields = append(fields,
		derivativesSeriesVersion,
		// The member identifier. See the block above on why it is not an echo.
		strconv.FormatInt(snap.Timestamp, 10),
		formatInt64OmitZero(snap.ObservedAt),
		snap.Source,
		formatFloatPtr(snap.OpenInterestBase),
		formatInt64Ptr(snap.OpenInterestObservedAtMs),
		formatFloatPtr(snap.OpenInterest),
		formatFloatPtr(snap.OIChangePct),
		formatInt64Ptr(snap.OIChangePctWindowMs),
		formatFloatPtr(snap.FundingRate),
		formatFloatPtr(snap.NextFundingRate),
		formatFloatPtr(snap.FundingRate8h),
		formatFloatPtr(snap.FundingIntervalHours),
		string(snap.NStep),
		formatFloatPtr(snap.InterestRate),
		formatBoolPtr(snap.FundingAtAnchor),
		formatFloatPtr(snap.LongShortRatio),
		formatFloatPtr(snap.LSLongPct),
		formatFloatPtr(snap.LSShortPct),
	)

	return strings.Join(fields, derivativesSeriesSep), nil
}

// decodeDerivativesSeriesMember reconstructs the snapshot from the member, the identity
// (which comes from the key) and the instant (which comes from the score). It is BIMODAL for
// as long as the rollout lasts: a member starting with `{` is the legacy form, which still
// occupies every key not yet rewritten.
func decodeDerivativesSeriesMember(
	raw string,
	ident derivativesSeriesIdentity,
) (*derivatives.DerivativesSnapshot, error) {
	if raw == "" {
		return nil, &DerivativesSeriesDecodeError{
			Version: "", Reason: "empty member", Sample: "",
			cause: ErrDerivativesSeriesMalformedMember,
		}
	}
	if raw[0] == '{' {
		return decodeLegacyDerivativesSeriesMember(raw)
	}

	fields := strings.Split(raw, derivativesSeriesSep)
	if fields[0] != derivativesSeriesVersion {
		return nil, &DerivativesSeriesDecodeError{
			Version: fields[0],
			Reason:  fmt.Sprintf("this binary only reads %q", derivativesSeriesVersion),
			Sample:  derivativesSeriesSample(raw),
			cause:   ErrDerivativesSeriesUnknownVersion,
		}
	}
	if len(fields) != derivativesSeriesFieldCount {
		return nil, &DerivativesSeriesDecodeError{
			Version: fields[0],
			Reason:  fmt.Sprintf("%d fields, want %d", len(fields), derivativesSeriesFieldCount),
			Sample:  derivativesSeriesSample(raw),
			cause:   ErrDerivativesSeriesMalformedMember,
		}
	}

	snap := &derivatives.DerivativesSnapshot{
		Exchange:   ident.Exchange,
		MarketType: ident.MarketType,
		Symbol:     ident.Symbol,
		Source:     fields[3],
		NStep:      derivatives.NDerivationStep(fields[13]),
	}

	// A parse error does not become a missing field: it aborts the whole member, because a
	// half-built snapshot is indistinguishable from a read that did not bring that field.
	var bad error
	snap.Timestamp, bad = parseInt64OmitZero(fields[1], bad)
	snap.ObservedAt, bad = parseInt64OmitZero(fields[2], bad)
	snap.OpenInterestBase, bad = parseFloatPtr(fields[4], bad)
	snap.OpenInterestObservedAtMs, bad = parseInt64Ptr(fields[5], bad)
	snap.OpenInterest, bad = parseFloatPtr(fields[6], bad)
	snap.OIChangePct, bad = parseFloatPtr(fields[7], bad)
	snap.OIChangePctWindowMs, bad = parseInt64Ptr(fields[8], bad)
	snap.FundingRate, bad = parseFloatPtr(fields[9], bad)
	snap.NextFundingRate, bad = parseFloatPtr(fields[10], bad)
	snap.FundingRate8h, bad = parseFloatPtr(fields[11], bad)
	snap.FundingIntervalHours, bad = parseFloatPtr(fields[12], bad)
	snap.InterestRate, bad = parseFloatPtr(fields[14], bad)
	snap.FundingAtAnchor, bad = parseBoolPtr(fields[15], bad)
	snap.LongShortRatio, bad = parseFloatPtr(fields[16], bad)
	snap.LSLongPct, bad = parseFloatPtr(fields[17], bad)
	snap.LSShortPct, bad = parseFloatPtr(fields[18], bad)

	if bad != nil {
		return nil, &DerivativesSeriesDecodeError{
			Version: fields[0],
			Reason:  bad.Error(),
			Sample:  derivativesSeriesSample(raw),
			cause:   ErrDerivativesSeriesMalformedMember,
		}
	}

	return snap, nil
}

// decodeLegacyDerivativesSeriesMember reads the JSON form. It carries its own identity and
// its own timestamp, so it needs neither the key nor the score.
func decodeLegacyDerivativesSeriesMember(raw string) (*derivatives.DerivativesSnapshot, error) {
	var snap derivatives.DerivativesSnapshot
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		return nil, &DerivativesSeriesDecodeError{
			Version: "json",
			Reason:  err.Error(),
			Sample:  derivativesSeriesSample(raw),
			cause:   ErrDerivativesSeriesMalformedMember,
		}
	}
	return &snap, nil
}

// derivativesSeriesSample truncates the offending member's echo, so that a log line does not
// take a whole payload.
func derivativesSeriesSample(raw string) string {
	const limit = 120
	if len(raw) > limit {
		return raw[:limit] + "…"
	}
	return raw
}

// formatFloatPtr writes a numeric member field, and the FINITENESS GUARD lives here.
//
// `strconv.FormatFloat` formats `+Inf` and `NaN` SUCCESSFULLY — it is not the guard one
// might assume exists. This is the point where a non-finite value ENTERS the series: the
// member is positional and does not go through JSON, so nothing here failed, and the value
// went to the ZSET and from there to the axis computation. A value that is not a number is
// written as ABSENT, which is what the empty position already means in this format — never
// as zero, and never as the literal.
func formatFloatPtr(v *float64) string {
	if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) {
		return ""
	}
	return strconv.FormatFloat(*v, 'g', -1, 64)
}

func formatInt64Ptr(v *int64) string {
	if v == nil {
		return ""
	}
	return strconv.FormatInt(*v, 10)
}

// formatInt64OmitZero treats zero as absent because the corresponding field is an int64
// without a pointer whose zero ALREADY means "did not arrive" (`observedAt,omitempty`).
func formatInt64OmitZero(v int64) string {
	if v == 0 {
		return ""
	}
	return strconv.FormatInt(v, 10)
}

func formatBoolPtr(v *bool) string {
	if v == nil {
		return ""
	}
	if *v {
		return "1"
	}
	return "0"
}

// parseFloatPtr is the other side of the same guard: a member that ALREADY contains the
// non-finite literal — data written without this guard, for example — is refused,
// and not returned as a number. The refusal aborts the whole member, as the failure rule
// already declares: a value that is not a number served as if it were is the regression this
// codec exists to prevent.
func parseFloatPtr(raw string, prev error) (*float64, error) {
	if prev != nil || raw == "" {
		return nil, prev
	}
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil, fmt.Errorf("float %q: %w", raw, err)
	}
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return nil, fmt.Errorf("float %q: not finite", raw)
	}
	return &parsed, nil
}

func parseInt64Ptr(raw string, prev error) (*int64, error) {
	if prev != nil || raw == "" {
		return nil, prev
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("int %q: %w", raw, err)
	}
	return &parsed, nil
}

func parseInt64OmitZero(raw string, prev error) (int64, error) {
	if prev != nil || raw == "" {
		return 0, prev
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("int %q: %w", raw, err)
	}
	return parsed, nil
}

func parseBoolPtr(raw string, prev error) (*bool, error) {
	if prev != nil || raw == "" {
		return nil, prev
	}
	switch raw {
	case "1":
		v := true
		return &v, nil
	case "0":
		v := false
		return &v, nil
	default:
		return nil, fmt.Errorf("bool %q: want \"0\" or \"1\"", raw)
	}
}
