package questdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/derivatives"
)

// TestWriteDerivativesBatch_PreservationIsCountedNotOnlyLogged.
//
// Degradation is rare and silent: it happens when one of the cycle's calls does not return,
// and the symptom is a column that disappears from an old bucket. Narrated in text, it tells
// the episode once and does not sum with the others — there is no way to ask a log file whether
// this is getting worse.
func TestWriteDerivativesBatch_PreservationIsCountedNotOnlyLogged(t *testing.T) {
	bucket := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)
	prior := &recordingPriorSource{rows: []*derivatives.DerivativesSnapshot{{
		Timestamp:      bucket.UnixMilli(),
		FundingRate:    fp(0.0001),
		LongShortRatio: fp(0.9554),
	}}}
	store := &Store{ilpSender: newFakeLineSender(), tableName: "candles", priors: prior}
	preserved := 0
	store.SetMetrics(Metrics{DerivativesColdDegraded: func(outcome string, n float64) {
		if outcome == outcomePreserved {
			preserved += int(n)
		}
	}})

	parcial := samplePoints(1)
	parcial[0].Timestamp = bucket
	parcial[0].FundingRate = nil
	parcial[0].LongShortRatio = nil

	if err := store.WriteDerivativesBatch(context.Background(), parcial); err != nil {
		t.Fatalf("WriteDerivativesBatch: %v", err)
	}

	if delta := preserved; delta != 1 {
		t.Fatalf("preserved rows counted = %v, wanted 1 — degradation only came out in a text "+
			"log, and a text log does not sum across cycles", delta)
	}
}

// TestWriteDerivativesBatch_TheHoleIsCountedApartFromThePreservation — the two choices have
// OPPOSITE consequences, so they cannot fall into the same number: preserved means the old data
// survived; hole means the NEW data did not enter.
func TestWriteDerivativesBatch_TheHoleIsCountedApartFromThePreservation(t *testing.T) {
	prior := &recordingPriorSource{err: errors.New("questdb down")}
	store := &Store{ilpSender: newFakeLineSender(), tableName: "candles", priors: prior}
	holes, preserved := 0, 0
	store.SetMetrics(Metrics{DerivativesColdDegraded: func(outcome string, n float64) {
		switch outcome {
		case outcomeHole:
			holes += int(n)
		case outcomePreserved:
			preserved += int(n)
		}
	}})

	parcial := samplePoints(2)
	parcial[0].FundingRate = nil
	parcial[1].FundingRate = nil

	if err := store.WriteDerivativesBatch(context.Background(), parcial); err != nil {
		t.Fatalf("WriteDerivativesBatch: %v", err)
	}

	if delta := holes; delta != 2 {
		t.Fatalf("holes counted = %v, wanted 2", delta)
	}
	if delta := preserved; delta != 0 {
		t.Fatalf("preserved rows counted = %v in a cycle in which nothing could be read", delta)
	}
}

// TestWriteDerivativesBatch_AHealthyCycleCountsNothing — a counter that moves in the healthy
// case distinguishes nothing: it would become the number of written rows under another name.
func TestWriteDerivativesBatch_AHealthyCycleCountsNothing(t *testing.T) {
	store := newFakeColdStore(newFakeLineSender())
	holes, preserved := 0, 0
	store.SetMetrics(Metrics{DerivativesColdDegraded: func(outcome string, n float64) {
		switch outcome {
		case outcomeHole:
			holes += int(n)
		case outcomePreserved:
			preserved += int(n)
		}
	}})

	if err := store.WriteDerivativesBatch(context.Background(), samplePoints(3)); err != nil {
		t.Fatalf("WriteDerivativesBatch: %v", err)
	}

	if delta := holes; delta != 0 {
		t.Fatalf("holes counted = %v in a cycle in which the table was empty", delta)
	}
	if delta := preserved; delta != 0 {
		t.Fatalf("preserved rows counted = %v in a cycle in which there was nothing to preserve", delta)
	}
}
