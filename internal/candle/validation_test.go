package candle

import "testing"

func sanityKid(high, low, volume float64) Candle {
	return Candle{Open: low, High: high, Low: low, Close: high, Volume: volume}
}

func TestValidateFoldedParent(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		parent Candle
		kids   []Candle
		want   []SanityViolation
	}{
		{
			name:   "a consistent fold reports nothing",
			parent: Candle{Open: 1, High: 3, Low: 1, Close: 3, Volume: 6},
			kids:   []Candle{sanityKid(2, 1, 3), sanityKid(3, 2, 3)},
		},
		{
			// The failing shape: a low above every child's low, i.e. a price no child printed.
			name:   "the parent low sits above every child low",
			parent: Candle{Open: 2.5, High: 3, Low: 2.5, Close: 3, Volume: 6},
			kids:   []Candle{sanityKid(2, 1, 3), sanityKid(3, 2, 3)},
			want:   []SanityViolation{ViolationLowAboveChildren},
		},
		{
			name:   "the parent high sits below a child high",
			parent: Candle{Open: 1, High: 2.5, Low: 1, Close: 2, Volume: 6},
			kids:   []Candle{sanityKid(3, 1, 3), sanityKid(2.5, 1, 3)},
			want:   []SanityViolation{ViolationHighBelowChildren},
		},
		{
			name:   "open outside the range",
			parent: Candle{Open: 5, High: 3, Low: 1, Close: 2, Volume: 3},
			kids:   []Candle{sanityKid(3, 1, 3)},
			want:   []SanityViolation{ViolationOHLCOrder},
		},
		{
			name:   "a complete bucket that is not the sum of its children",
			parent: Candle{Open: 1, High: 3, Low: 1, Close: 3, Volume: 5},
			kids:   []Candle{sanityKid(2, 1, 3), sanityKid(3, 2, 3)},
			want:   []SanityViolation{ViolationVolumeMismatch},
		},
		{
			// A marked bucket exists BECAUSE a part is missing: its volume is short by definition.
			name:   "a marked bucket is exempt from the volume check",
			parent: Candle{Open: 1, High: 3, Low: 1, Close: 3, Volume: 5, HasGap: true, GapKind: GapVolumeIncomplete},
			kids:   []Candle{sanityKid(2, 1, 3), sanityKid(3, 2, 3)},
		},
		{
			// An untraded child carries the previous close on every price field; counting it would
			// invent a 999 high that no trade ever printed.
			name:   "untraded children are not evidence",
			parent: Candle{Open: 1, High: 3, Low: 1, Close: 3, Volume: 6},
			kids:   []Candle{sanityKid(2, 1, 3), sanityKid(3, 2, 3), {High: 999, Low: 0.01, Close: 999}},
		},
		{
			name:   "an impossible bucket can violate several rules at once",
			parent: Candle{Open: 1.5, High: 1, Low: 2, Close: 1.5, Volume: 3},
			kids:   []Candle{sanityKid(2, 1, 3)},
			want: []SanityViolation{
				ViolationOHLCOrder, ViolationHighBelowChildren, ViolationLowAboveChildren,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ValidateFoldedParent(tc.parent, tc.kids)
			if len(got) != len(tc.want) {
				t.Fatalf("violations = %v, want %v", got, tc.want)
			}
			seen := make(map[SanityViolation]struct{}, len(got))
			for _, v := range got {
				seen[v] = struct{}{}
			}
			for _, want := range tc.want {
				if _, ok := seen[want]; !ok {
					t.Fatalf("violations = %v, missing %v", got, want)
				}
			}
		})
	}
}

// A float sum of the same values must not read as a shortfall. The tolerance is relative and
// tiny on purpose: wide enough for arithmetic, far too narrow to swallow a missing child.
func TestValidateFoldedParent_FloatSumIsNotAViolation(t *testing.T) {
	t.Parallel()

	parent := Candle{Open: 1, High: 2, Low: 1, Close: 2, Volume: 0.1 + 0.2}
	kids := []Candle{
		{High: 2, Low: 1, Close: 2, Volume: 0.1},
		{High: 2, Low: 1, Close: 2, Volume: 0.2},
	}
	if got := ValidateFoldedParent(parent, kids); len(got) != 0 {
		t.Fatalf("violations = %v, want none for an exact sum", got)
	}
}
