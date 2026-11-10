package derivatives

import "testing"

// THE CONVENTION HAS NO DEFAULT, and that is what these cases pin.
//
// The zero value is invalid on purpose: a convention defaulting to something is a guess wearing a
// type, and the guess is right for exactly the feeds that already exist — which are precisely the
// ones that would never exercise it. A new feed that forgets to declare must be REFUSED, not aligned
// as if it were the old one.
func TestLabelConventionZeroValueIsInvalid(t *testing.T) {
	var undeclared LabelConvention
	if undeclared != LabelUnspecified {
		t.Fatalf("the zero value should be LabelUnspecified, got %d", undeclared)
	}
	if undeclared.Valid() {
		t.Fatal("a feed that did not declare the convention cannot be valid")
	}
}

func TestLabelConventionDeclaredIsValid(t *testing.T) {
	for _, declared := range []LabelConvention{LabelIntervalStart, LabelIntervalEnd} {
		if !declared.Valid() {
			t.Fatalf("%s was declared and should be valid", declared)
		}
	}
}

// THE LOG IS WHAT THE MAINTAINER READS when ingestion refuses, so a feed without a declaration must
// name itself as such instead of appearing as a number.
func TestLabelConventionNamesItselfForTheLog(t *testing.T) {
	cases := map[LabelConvention]string{
		LabelUnspecified:   "unspecified",
		LabelIntervalStart: "interval-start",
		LabelIntervalEnd:   "interval-end",
	}
	for convention, want := range cases {
		if got := convention.String(); got != want {
			t.Fatalf("convention %d: expected %q, got %q", convention, want, got)
		}
	}
}

// The two conventions are DISTINCT: a type where they collapsed would pass everything above and
// align the two feeds into the same wrong bucket.
func TestLabelConventionsAreDistinct(t *testing.T) {
	if LabelIntervalStart == LabelIntervalEnd {
		t.Fatal("interval start and end cannot be the same value")
	}
}
