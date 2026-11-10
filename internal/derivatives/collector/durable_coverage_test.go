package collector

import (
	"errors"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// ABSENCE OF DURABILITY IS NOT ABSENCE OF DATA.
//
// What these cases guard, and what they refuse to guard: they require `1w` to be DECLARED without a
// durable tier in all three quantities, and do NOT require it to stop being served. The "keeps being
// served" half lives in `internal/adapters/inbound/httpapi`, where the route answers — here there is no
// route to check.
// ─────────────────────────────────────────────────────────────────────────────

func TestDurableAbsences_WeeklyIsDeclaredAcrossTheThreeFeeds(t *testing.T) {
	absences := DurableAbsences("1w")
	if len(absences) != 3 {
		t.Fatalf("1w declared %d absences, want 3 — one per quantity: %v", len(absences), absences)
	}

	// THE THREE QUANTITIES, BY NAME. Two of three would leave one column promised.
	want := map[string]bool{"open_interest": false, "long_short_ratio": false, "funding_rate": false}
	for _, a := range absences {
		if _, ok := want[a.Feed]; !ok {
			t.Fatalf("unexpected quantity in the declaration: %q", a.Feed)
		}
		want[a.Feed] = true
		if a.Source != "coinalyze" {
			t.Errorf("%s: source declared %q, want coinalyze — the absence belongs to the source that would write the durable tier", a.Feed, a.Source)
		}
		if a.Timeframe != "1w" {
			t.Errorf("%s: timeframe declared %q, want 1w", a.Feed, a.Timeframe)
		}
		if a.Reason != reasonUnavailable {
			t.Errorf("%s: reason %q — weekly is a MEASURED absence of support, not a lack of measurement", a.Feed, a.Reason)
		}
	}
	for feed, declared := range want {
		if !declared {
			t.Errorf("%s was left without an absence declaration at 1w", feed)
		}
	}
}

// COMPLETE COVERAGE DECLARES NO ABSENCE. Without this case, an implementation returning the three
// absences for EVERY timeframe would pass the case above and fill the envelope with seven false
// answers.
func TestDurableAbsences_ACoveredTimeframeDeclaresNothing(t *testing.T) {
	for _, tf := range []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d"} {
		if got := DurableAbsences(tf); len(got) != 0 {
			t.Errorf("%s has a durable tier and declared %v", tf, got)
		}
	}
}

// "WE NEVER ASKED" CARRIES ANOTHER REASON. The two absences send the operator to different places:
// one is a provider limit, the other is a measurement nobody took.
func TestDurableAbsences_UndeclaredCarriesADifferentReasonThanUnsupported(t *testing.T) {
	absences := DurableAbsences("7m")
	if len(absences) != 3 {
		t.Fatalf("7m declared %d absences, want 3: %v", len(absences), absences)
	}
	for _, a := range absences {
		if a.Reason != reasonUndeclared {
			t.Errorf("%s: reason %q, want the one for absence of measurement", a.Feed, a.Reason)
		}
	}
	if reasonUndeclared == reasonUnavailable {
		t.Fatal("the two reasons collapsed into one — the distinction the requirement makes disappeared")
	}
}

// THE CELL IS INSPECTABLE: through the sentinel, without knowing the coordinates; and through the
// type, recovering all three. A ready-made sentence would force the consumer to parse text.
func TestDurableAbsence_IsInspectableThroughTheSentinelAndTheType(t *testing.T) {
	var err error = DurableAbsences("1w")[0]

	if !errors.Is(err, ErrNoDurableTier) {
		t.Errorf("the absence does not answer the class sentinel")
	}
	var cell DurableAbsenceError
	if !errors.As(err, &cell) {
		t.Fatal("the absence is not recoverable as a typed value")
	}
	if cell.Timeframe != "1w" || cell.Source != "coinalyze" || cell.Feed == "" || cell.Reason == "" {
		t.Errorf("the coordinates did not survive the chain: %+v", cell)
	}
}

// NO SYNTHETIC `1w` POINT CAN BE PRODUCED, and both halves are checked: weekly does not enter
// durable coverage, and even if it did there would be no interval to request it with — translation
// fails before the call.
//
// THE MUTANT THIS CASE KILLS along with the ones above: declaring `1w` available in the Coinalyze
// row. It is the only way this code can fabricate a durable weekly point.
func TestNoSyntheticWeeklyPointCanBeCollected(t *testing.T) {
	for _, tf := range DurableTimeframes([]string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"}) {
		if tf == "1w" {
			t.Error("1w entered durable coverage — collection would start writing weekly")
		}
	}
	if _, err := CoinalyzeInterval("1w"); !errors.Is(err, ErrCoinalyzeTimeframeUnsupported) {
		t.Errorf("the 1w translation returned %v — without a refusal here, a weekly request reaches the API", err)
	}
}
