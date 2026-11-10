package redisstore

import (
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/marxms/go-whale/internal/derivatives"
)

// ============================================================================
// THE FINITENESS GUARD'S COVERAGE IS ENUMERATED FROM THE SCHEMA
// ============================================================================
//
// A new numeric field can be added without a guard and nobody notices. The mitigation is this
// case — it enumerates the snapshot's numeric fields and asserts the guard for EACH ONE.
//
// The list MUST NOT be hand-written: a second field list is exactly the divergence the test
// exists to catch, and it would age in the same commit that added a field. The enumeration
// comes from the SCHEMA (the struct, by field type) and the name of each field found is
// checked against the repository's OTHER mechanical declaration, `DerivativesDurableCoverage`
// — two declarations that must agree.
//
// What each field must prove, by EFFECT and not by reading the guard:
//   - the member still has the format's field count (the guard swaps the value, not the shape);
//   - no text slot is a non-finite literal (the write does not store "+Inf"/"NaN");
//   - the target field comes back ABSENT on the read, and not zero;
//   - the finite siblings come back present (the guard does not drop the row);
//   - a member with the non-finite literal in the field's slot is REFUSED on the read, which is
//     why the read-side guard exists.

// snapshotWithAllFloats returns a snapshot with EVERY `*float64` field filled with a finite
// value distinct per position — distinct so each field's slot can be located in the member
// without a hand-written position table. The `target` field receives `value`.
func snapshotWithAllFloats(target string, value float64) *derivatives.DerivativesSnapshot {
	snap := &derivatives.DerivativesSnapshot{
		Exchange:   "binance",
		MarketType: "future",
		Symbol:     "BTCUSDT",
		Timestamp:  1_787_586_300_000,
		Source:     derivatives.SourceVenue,
		NStep:      derivatives.NFromMeasuredSpacing,
	}

	tipo := reflect.TypeOf(*snap)
	rv := reflect.ValueOf(snap).Elem()
	for i := 0; i < tipo.NumField(); i++ {
		if tipo.Field(i).Type.String() != "*float64" {
			continue
		}
		v := 1000.0 + float64(i) + 0.5
		if tipo.Field(i).Name == target {
			v = value
		}
		rv.Field(i).Set(reflect.ValueOf(&v))
	}
	return snap
}

// floatFieldsOfTheSnapshot is the ENUMERATION. It checks each field found against the durable
// coverage declaration, so the set cannot shrink silently.
func floatFieldsOfTheSnapshot(t *testing.T) []string {
	t.Helper()

	tipo := reflect.TypeOf(derivatives.DerivativesSnapshot{})
	coverage := DerivativesDurableCoverage()

	var fields []string
	for i := 0; i < tipo.NumField(); i++ {
		f := tipo.Field(i)
		if f.Type.String() != "*float64" {
			continue
		}
		if _, declared := coverage[jsonName(f)]; !declared {
			t.Fatalf("the field %s is *float64 and is NOT declared in DerivativesDurableCoverage() "+
				"— either the coverage aged, or the enumeration stopped looking at the schema", f.Name)
		}
		fields = append(fields, f.Name)
	}
	if len(fields) == 0 {
		t.Fatal("the enumeration found no *float64 field: the schema changed shape or " +
			"reflection broke, and the case would pass without exercising anything")
	}
	return fields
}

// slotIndexOf locates the slot carrying that field's finite and distinct value. This is how
// the READ test injects the literal in the right place without a position table.
func slotIndexOf(snap *derivatives.DerivativesSnapshot, campo string) (int, bool) {
	raw, err := encodeDerivativesSeriesMember(snap)
	if err != nil {
		return 0, false
	}
	campoValor := reflect.ValueOf(snap).Elem().FieldByName(campo)
	if campoValor.IsNil() {
		return 0, false
	}
	procurado := formatFloatPtr(campoValor.Interface().(*float64))

	pos, achados := -1, 0
	for i, slot := range strings.Split(raw, derivativesSeriesSep) {
		if slot == procurado {
			pos, achados = i, achados+1
		}
	}
	return pos, achados == 1
}

func TestDerivativesSeriesCodec_EveryFloatQuantityOfTheSnapshotIsGuarded(t *testing.T) {
	t.Parallel()

	fields := floatFieldsOfTheSnapshot(t)
	ident := derivativesSeriesIdentity{Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT"}

	naoFinitos := map[string]float64{"+Inf": math.Inf(1), "-Inf": math.Inf(-1), "NaN": math.NaN()}

	for _, target := range fields {
		t.Run(target, func(t *testing.T) {
			t.Parallel()

			for literal, value := range naoFinitos {
				raw, err := encodeDerivativesSeriesMember(snapshotWithAllFloats(target, value))
				if err != nil {
					t.Fatalf("%s=%s: encode returned error %v — the guard swaps the value for absence, "+
						"it does not drop the member", target, literal, err)
				}

				// The SHAPE does not change: the guard removes the value, not the field.
				slots := strings.Split(raw, derivativesSeriesSep)
				if len(slots) != derivativesSeriesFieldCount {
					t.Fatalf("%s=%s: member with %d fields, want %d",
						target, literal, len(slots), derivativesSeriesFieldCount)
				}

				// The WRITE cannot store a non-finite literal in ANY slot.
				for pos, slot := range slots {
					if slot == "" {
						continue
					}
					v, parseErr := strconv.ParseFloat(slot, 64)
					if parseErr != nil {
						continue
					}
					if math.IsNaN(v) || math.IsInf(v, 0) {
						t.Fatalf("%s=%s: slot %d of the member is the non-finite literal %q — "+
							"the value entered the series instead of being blocked", target, literal, pos, slot)
					}
				}

				// The READ: the field comes back ABSENT (nil), never zero, and the siblings
				// survive.
				dec, err := decodeDerivativesSeriesMember(raw, ident)
				if err != nil {
					t.Fatalf("%s=%s: decode returned error %v — the member's shape is intact",
						target, literal, err)
				}
				rv := reflect.ValueOf(dec).Elem()
				if f := rv.FieldByName(target); !f.IsNil() {
					t.Fatalf("%s=%s: the field came back PRESENT (%v) — non-finite must become absence, "+
						"never a value", target, literal, f.Interface())
				}
				for _, sibling := range fields {
					if sibling == target {
						continue
					}
					if rv.FieldByName(sibling).IsNil() {
						t.Fatalf("%s=%s: the sibling %s became absent — the guard dropped more than the field",
							target, literal, sibling)
					}
				}
			}
		})
	}
}

// The member that ALREADY contains the non-finite literal is REFUSED on the read, and not
// served with the field missing or with the literal.
func TestDerivativesSeriesCodec_NonFiniteSlotIsRefusedOnRead(t *testing.T) {
	t.Parallel()

	fields := floatFieldsOfTheSnapshot(t)
	ident := derivativesSeriesIdentity{Exchange: "binance", MarketType: "future", Symbol: "BTCUSDT"}

	for _, target := range fields {
		t.Run(target, func(t *testing.T) {
			t.Parallel()

			clean := snapshotWithAllFloats(target, 0)
			pos, ok := slotIndexOf(clean, target)
			if !ok {
				t.Fatalf("the field %s's finite value does not appear exactly once in the member — "+
					"the write did not emit it, or the slot location is no longer unambiguous", target)
			}
			raw, err := encodeDerivativesSeriesMember(clean)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			slots := strings.Split(raw, derivativesSeriesSep)

			for _, literal := range []string{"+Inf", "NaN"} {
				mutado := append([]string(nil), slots...)
				mutado[pos] = literal

				dec, err := decodeDerivativesSeriesMember(strings.Join(mutado, derivativesSeriesSep), ident)
				if err == nil {
					t.Fatalf("%s: %q in slot %d decoded as if it were a number (%v) — the literal "+
						"crossed the read", target, literal, pos, dec)
				}
				if !errors.Is(err, ErrDerivativesSeriesMalformedMember) {
					t.Fatalf("%s: %q gave error %v, want ErrDerivativesSeriesMalformedMember",
						target, literal, err)
				}
				if dec != nil {
					t.Fatalf("%s: %q returned a half-built snapshot", target, literal)
				}
			}
		})
	}
}
