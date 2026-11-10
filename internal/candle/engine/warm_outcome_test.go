package engine

import (
	"errors"
	"testing"
)

// TestWarmOutcome pins the distinction the warm boot could not make for thirteen hours.
//
// # WHY THIS RULE EXISTS
//
// WarmCandleEngine must not conflate two operationally opposite situations:
//
//	empty        the symbol genuinely has no history yet. Expected, benign.
//	unavailable  the tier could not be reached. The buffer boots EMPTY, the REST seeder then
//	             fills it from scratch, and that re-anchors the absolute CVD level and
//	             persists the new baseline into the historical record.
//
// A cold-tier outage once booted the entire fleet this way. The only trace was a summary line
// reporting 0 buffers seeded — which reads exactly like a quiet, empty system.
func TestWarmOutcome(t *testing.T) {
	t.Parallel()

	boom := errors.New("dial tcp 192.0.2.10:8812: connect: connection refused")

	tests := []struct {
		name     string
		source   string
		coldErr  error
		redisErr error
		want     string
	}{
		{
			name:   "seeded from cold",
			source: "questdb", want: "questdb",
		},
		{
			name:   "seeded from redis after cold failed — NOT a failure",
			source: "redis", coldErr: boom, want: "redis",
		},
		{
			// The whole point: reachable tiers, no history. A new symbol looks like this and
			// must not raise an alarm.
			name: "both tiers answered, neither had history",
			want: warmOutcomeEmpty,
		},
		{
			name:    "cold errored and redis had nothing",
			coldErr: boom,
			want:    warmOutcomeUnavailable,
		},
		{
			name:     "redis errored and cold had nothing",
			redisErr: boom,
			want:     warmOutcomeUnavailable,
		},
		{
			name:    "both tiers errored — the shape of the 13-hour outage",
			coldErr: boom, redisErr: boom,
			want: warmOutcomeUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := warmOutcome(tt.source, tt.coldErr, tt.redisErr); got != tt.want {
				t.Errorf("warmOutcome(%q, %v, %v) = %q, want %q — an unreachable tier reported as "+
					"'empty' is how a cold-tier outage boots the whole fleet onto re-anchored "+
					"buffers with nothing in the log pointing at the cause",
					tt.source, tt.coldErr, tt.redisErr, got, tt.want)
			}
		})
	}
}

// TestFirstNonNilErr covers the sample the boot summary carries. A count alone tells an
// operator how wide the damage is but not where to look; the first concrete error is what
// turns the warning into something actionable.
func TestFirstNonNilErr(t *testing.T) {
	t.Parallel()

	first := errors.New("first")
	second := errors.New("second")

	if got := firstNonNilErr(nil, first, second); !errors.Is(got, first) {
		t.Errorf("firstNonNilErr = %v, want %v — the summary must carry the earliest concrete cause", got, first)
	}
	if got := firstNonNilErr(nil, nil); got != nil {
		t.Errorf("firstNonNilErr = %v, want nil", got)
	}
}
