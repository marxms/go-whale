package coinalyze

import (
	"strings"
	"testing"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// The integrity of the history response.
//
// The API answers a window beyond retention with HTTP 200 and what exists: SILENT
// truncation. And it answers a list whose elements carry the symbol in `symbol` — reading
// that list by POSITION attributes one symbol's series to another on the day the provider
// omits one of them, which is what it does when it has no data.
//
// The `history` timestamps arrive in SECONDS (measured: `t: 1683849600`), unlike the
// snapshot, which arrives in milliseconds.

const (
	// 2024-01-01T00:00:00Z and 2025-01-01T00:00:00Z.
	coverageYearStart int64 = 1704067200
	coverageYearEnd   int64 = 1735689600

	coverageDay      = 24 * time.Hour
	coverageInterval = 5 * time.Minute
)

func coveragePoint(sec int64) CoinalyzeHistoryPoint {
	return CoinalyzeHistoryPoint{T: sec, O: 1, H: 2, L: 0.5, C: 1.5}
}

// coverageSeries builds an ASCENDING series of `count` points that ENDS at `lastSec`, which
// is the shape of real truncation: the provider keeps the end of the window and cuts the
// beginning.
func coverageSeries(symbol string, lastSec int64, count int, step time.Duration) CoinalyzeHistorySeries {
	points := make([]CoinalyzeHistoryPoint, 0, count)
	for i := count - 1; i >= 0; i-- {
		points = append(points, coveragePoint(lastSec-int64(i)*int64(step.Seconds())))
	}
	return CoinalyzeHistorySeries{Symbol: symbol, History: points}
}

func coverageRequest(symbols []string, interval time.Duration) CoinalyzeHistoryRequest {
	return CoinalyzeHistoryRequest{
		Symbols:  symbols,
		From:     time.Unix(coverageYearStart, 0).UTC(),
		To:       time.Unix(coverageYearEnd, 0).UTC(),
		Interval: interval,
	}
}

func auditOf(t *testing.T, audits []CoinalyzeSeriesAudit, symbol string) CoinalyzeSeriesAudit {
	t.Helper()
	for _, audit := range audits {
		if audit.Symbol == symbol {
			return audit
		}
	}
	t.Fatalf("%s did not appear in the audit", symbol)
	return CoinalyzeSeriesAudit{}
}

// INDEXING IS BY THE `symbol` FIELD, NEVER BY POSITION. Here the provider omits the first
// requested symbol and returns the other two OUT OF ORDER — a positional read would give
// SOL's series to BTC and the history would be wrong about the asset with nothing in red.
func TestCoinalyzeAuditIndexesBySymbolNeverByPosition(t *testing.T) {
	req := coverageRequest([]string{"BTCUSDT_PERP.A", "ETHUSDT_PERP.A", "SOLUSDT_PERP.A"}, coverageDay)

	audits := CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{
		coverageSeries("SOLUSDT_PERP.A", coverageYearEnd, 3, coverageDay),
		coverageSeries("ETHUSDT_PERP.A", coverageYearEnd, 7, coverageDay),
	})

	if got := auditOf(t, audits, "ETHUSDT_PERP.A"); len(got.Points) != 7 {
		t.Fatalf("ETH has 7 points in the response, the audit gave it %d", len(got.Points))
	}
	if got := auditOf(t, audits, "SOLUSDT_PERP.A"); len(got.Points) != 3 {
		t.Fatalf("SOL has 3 points in the response, the audit gave it %d", len(got.Points))
	}
	if got := auditOf(t, audits, "BTCUSDT_PERP.A"); got.Present {
		t.Fatalf("BTC did not come in the response and was given %d points", len(got.Points))
	}
}

// THE ABSENT SYMBOL IS NAMED. It disappears from the array with no error; without this step,
// it also disappears from the report.
func TestCoinalyzeAuditNamesTheSymbolMissingFromTheResponse(t *testing.T) {
	req := coverageRequest([]string{"BTCUSDT_PERP.A", "ETHUSDT_PERP.A"}, coverageDay)

	audits := CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{
		coverageSeries("ETHUSDT_PERP.A", coverageYearEnd, 367, coverageDay),
	})

	missing := auditOf(t, audits, "BTCUSDT_PERP.A")
	if missing.Present {
		t.Fatal("BTC did not come in the response and was treated as present")
	}
	if len(missing.Points) != 0 {
		t.Fatalf("an absent symbol must not carry points, it carried %d", len(missing.Points))
	}
	// And what came is not affected by what was missing.
	if present := auditOf(t, audits, "ETHUSDT_PERP.A"); !present.Present || present.Partial {
		t.Fatalf("ETH came whole and was audited as present=%v partial=%v", present.Present, present.Partial)
	}
}

// EVERY REQUESTED SYMBOL IS REPORTED, ONCE, IN THE ORDER REQUESTED. A report that speaks
// only of what came back is the same silence under another name.
func TestCoinalyzeAuditReportsEveryRequestedSymbolExactlyOnceInOrder(t *testing.T) {
	requested := []string{"BTCUSDT_PERP.A", "ETHUSDT_PERP.A", "SOLUSDT_PERP.A"}
	req := coverageRequest(requested, coverageDay)

	audits := CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{
		coverageSeries("ETHUSDT_PERP.A", coverageYearEnd, 367, coverageDay),
		// A symbol nobody requested does not enter the report.
		coverageSeries("XRPUSDT_PERP.A", coverageYearEnd, 367, coverageDay),
	})

	if len(audits) != len(requested) {
		t.Fatalf("expected %d audits, got %d", len(requested), len(audits))
	}
	for i, symbol := range requested {
		if audits[i].Symbol != symbol {
			t.Fatalf("position %d is %s, expected %s", i, audits[i].Symbol, symbol)
		}
	}
}

// A YEAR REQUESTED AND THREE WEEKS RECEIVED IS PARTIAL COVERAGE. It is the outcome the
// provider's retention produces, and it arrives with HTTP 200.
func TestCoinalyzeAuditMarksAShortWindowAsPartiallyCovered(t *testing.T) {
	req := coverageRequest([]string{"BTCUSDT_PERP.A"}, coverageInterval)

	// Three weeks of 5 minutes, ending at the end of the requested window.
	threeWeeks := int(21 * 24 * 60 / 5)
	audits := CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{
		coverageSeries("BTCUSDT_PERP.A", coverageYearEnd, threeWeeks, coverageInterval),
	})

	got := auditOf(t, audits, "BTCUSDT_PERP.A")
	if !got.Present {
		t.Fatal("the symbol came in the response")
	}
	if !got.Partial {
		t.Fatal("a year requested and three weeks received is partial coverage")
	}
}

// AND THE GAP IS RECORDED WITH THE MISSING INTERVAL — not with a bare "something was
// missing".
func TestCoinalyzeAuditRecordsTheMissingIntervalItself(t *testing.T) {
	req := coverageRequest([]string{"BTCUSDT_PERP.A"}, coverageInterval)

	threeWeeks := int(21 * 24 * 60 / 5)
	series := coverageSeries("BTCUSDT_PERP.A", coverageYearEnd, threeWeeks, coverageInterval)
	firstPoint := time.Unix(series.History[0].T, 0).UTC()

	audits := CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{series})
	got := auditOf(t, audits, "BTCUSDT_PERP.A")

	if len(got.Gaps) != 1 {
		t.Fatalf("expected ONE gap before the series, got %d: %+v", len(got.Gaps), got.Gaps)
	}
	if !got.Gaps[0].From.Equal(req.From) {
		t.Fatalf("the gap starts where the requested window starts, got %s", got.Gaps[0].From.UTC())
	}
	if !got.Gaps[0].To.Equal(firstPoint) {
		t.Fatalf("the gap ends at the first received point (%s), got %s", firstPoint, got.Gaps[0].To.UTC())
	}
}

// PARTIAL COVERAGE IS NOT DISCARDING: the marked series still delivers the points the
// provider gave. Marking and discarding would be the same loss with a label on top.
func TestCoinalyzePartialSeriesStillCarriesItsPoints(t *testing.T) {
	req := coverageRequest([]string{"BTCUSDT_PERP.A"}, coverageInterval)

	threeWeeks := int(21 * 24 * 60 / 5)
	audits := CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{
		coverageSeries("BTCUSDT_PERP.A", coverageYearEnd, threeWeeks, coverageInterval),
	})

	got := auditOf(t, audits, "BTCUSDT_PERP.A")
	if len(got.Points) != threeWeeks {
		t.Fatalf("the partial series should deliver the %d received points, delivered %d", threeWeeks, len(got.Points))
	}
}

// AND A COVERED WINDOW IS NOT MARKED. A detector that always accuses, accuses nothing.
func TestCoinalyzeAuditDoesNotFlagAFullyCoveredWindow(t *testing.T) {
	req := coverageRequest([]string{"BTCUSDT_PERP.A"}, coverageDay)

	// 2024 is a leap year: 366 days of window, 367 points including both ends.
	audits := CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{
		coverageSeries("BTCUSDT_PERP.A", coverageYearEnd, 367, coverageDay),
	})

	got := auditOf(t, audits, "BTCUSDT_PERP.A")
	if got.Partial {
		t.Fatalf("the window came whole and was marked partial: %+v", got.Gaps)
	}
	if len(got.Gaps) != 0 {
		t.Fatalf("no gap expected, got %d: %+v", len(got.Gaps), got.Gaps)
	}
}

// THE OPEN BUCKET AT THE END IS NOT A GAP. Coinalyze labels the START of the interval: the
// last daily point is today 00:00 UTC, a bucket still filling. Charging it for the whole day
// would mark every daily series partial on every cycle.
func TestCoinalyzeAuditToleratesTheOpenBucketAtTheEnd(t *testing.T) {
	req := coverageRequest([]string{"BTCUSDT_PERP.A"}, coverageDay)
	// The requested window ends at 2025-01-01T00:00Z; the real request ends at "now", which
	// here is eighteen hours after the last label.
	req.To = time.Unix(coverageYearEnd+18*3600, 0).UTC()

	audits := CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{
		coverageSeries("BTCUSDT_PERP.A", coverageYearEnd, 367, coverageDay),
	})

	got := auditOf(t, audits, "BTCUSDT_PERP.A")
	if got.Partial {
		t.Fatalf("the open bucket at the end became a gap: %+v", got.Gaps)
	}
}

// BUT A DELAY LONGER THAN ONE INTERVAL IS A REAL GAP — that is how a feed that stopped
// publishing shows up.
func TestCoinalyzeAuditFlagsATrailingGapBeyondOneInterval(t *testing.T) {
	req := coverageRequest([]string{"BTCUSDT_PERP.A"}, coverageDay)
	lastPoint := coverageYearEnd - 3*86400 // three days before the requested end

	audits := CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{
		coverageSeries("BTCUSDT_PERP.A", lastPoint, 364, coverageDay),
	})

	got := auditOf(t, audits, "BTCUSDT_PERP.A")
	if !got.Partial {
		t.Fatal("three days without publishing is partial coverage")
	}
	if len(got.Gaps) != 1 {
		t.Fatalf("expected ONE gap at the end, got %d: %+v", len(got.Gaps), got.Gaps)
	}
	if !got.Gaps[0].From.Equal(time.Unix(lastPoint, 0).UTC()) {
		t.Fatalf("the gap starts at the last received point, got %s", got.Gaps[0].From.UTC())
	}
	if !got.Gaps[0].To.Equal(req.To) {
		t.Fatalf("the gap ends at the end of the requested window, got %s", got.Gaps[0].To.UTC())
	}
}

// AN EMPTY SERIES IS THE WHOLE WINDOW MISSING, and it is different from an absent symbol:
// the provider answered for it, saying it has nothing. `[]` is its real response for that.
func TestCoinalyzeAuditTreatsAnEmptySeriesAsFullyMissing(t *testing.T) {
	req := coverageRequest([]string{"BTCUSDT_PERP.A"}, coverageDay)

	audits := CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{
		{Symbol: "BTCUSDT_PERP.A", History: nil},
	})

	got := auditOf(t, audits, "BTCUSDT_PERP.A")
	if !got.Present {
		t.Fatal("the provider ANSWERED for this symbol — empty is not absent")
	}
	if !got.Partial {
		t.Fatal("an empty series is the whole window missing")
	}
	if len(got.Gaps) != 1 {
		t.Fatalf("expected ONE gap the size of the window, got %d: %+v", len(got.Gaps), got.Gaps)
	}
	if !got.Gaps[0].From.Equal(req.From) || !got.Gaps[0].To.Equal(req.To) {
		t.Fatalf("the gap must be the whole requested window, got %s..%s", got.Gaps[0].From.UTC(), got.Gaps[0].To.UTC())
	}
}

// THE COVERED SPAN COMES FROM THE VALUES, NOT FROM THE ARRAY ORDER. Reading the first and
// last elements as if they were the smallest and largest makes a descending series look
// entirely missing.
func TestCoinalyzeAuditCoversTheSameSpanRegardlessOfPointOrder(t *testing.T) {
	req := coverageRequest([]string{"BTCUSDT_PERP.A"}, coverageDay)

	ascending := coverageSeries("BTCUSDT_PERP.A", coverageYearEnd, 367, coverageDay)
	descending := CoinalyzeHistorySeries{Symbol: ascending.Symbol}
	for i := len(ascending.History) - 1; i >= 0; i-- {
		descending.History = append(descending.History, ascending.History[i])
	}

	audits := CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{descending})
	got := auditOf(t, audits, "BTCUSDT_PERP.A")

	if got.Partial {
		t.Fatalf("the same window in reverse order became partial: %+v", got.Gaps)
	}
}

// THE REQUIREMENT NAMES THE LEVEL: *"SHALL log at warn level naming the absent symbol"*.
//
// The audit already returns `Present: false` in the structure, and that is asserted above —
// but the consumer of the structure is code, and whoever finds out that a symbol vanished
// from the provider is a person reading logs. `Debug` does not leave at the process's default
// level; `Error` would drown the real alarm in an absence that is routine in a deep window.
func TestCoinalyzeAuditLogsTheMissingSymbolAtWarnLevel(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	req := coverageRequest([]string{"BTCUSDT_PERP.A", "ETHUSDT_PERP.A"}, coverageDay)
	CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{
		coverageSeries("ETHUSDT_PERP.A", coverageYearEnd, 367, coverageDay),
	}, log)

	level, found := sink.LevelOf("BTCUSDT_PERP.A")
	if !found {
		t.Fatalf("the absent symbol was not named in any log; captured: %v", sink.Records())
	}
	if level != logger.LevelWarn {
		t.Errorf("the absent symbol came out at %s, and the requirement demands %s", level, logger.LevelWarn)
	}
}

// NEGATIVE CONTROL: a complete response must not name anyone as absent, otherwise the
// assertion above would pass with a warning fired every time.
func TestCoinalyzeAuditCompleteResponseNamesNobodyAsMissing(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	req := coverageRequest([]string{"BTCUSDT_PERP.A"}, coverageDay)
	CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{
		coverageSeries("BTCUSDT_PERP.A", coverageYearEnd, 367, coverageDay),
	}, log)

	for _, r := range sink.Records() {
		if r.Level == logger.LevelWarn && strings.Contains(r.Text(), "absent from the history response") {
			t.Errorf("a complete response recorded an absence: %q", r.Text())
		}
	}
}

// THE GAP'S INTERVAL IS ASSERTED IN THE LOG, not only in the struct — a mutant that survived
// independent verification.
//
// The requirement says "record the gap WITH the missing interval". A zero-width `gap`
// satisfies the struct (there is an item in the list) and destroys the log: "missing
// <instant>..<instant>" sends the operator looking for a hole of no duration. The value
// traveled only in the struct, and the text the person reads was not checked.
func TestCoinalyzeAuditLogsTheGapWithItsRealInterval(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	// One-year window, provider returns ONE day at the beginning: the gap is almost the whole
	// year.
	req := coverageRequest([]string{"BTCUSDT_PERP.A"}, coverageDay)
	CoinalyzeAuditHistory(req, []CoinalyzeHistorySeries{
		coverageSeries("BTCUSDT_PERP.A", coverageYearEnd, 1, coverageDay),
	}, log)

	var linha string
	for _, r := range sink.Records() {
		if strings.Contains(r.Text(), "partially covered") {
			linha = r.Text()
		}
	}
	if linha == "" {
		t.Fatalf("the gap was not recorded; captured: %v", sink.Records())
	}

	// The RECORDED interval must have width, and the width must match the audit.
	de, ate, ok := gapBoundsFromLine(t, linha)
	if !ok {
		t.Fatalf("the line does not carry a readable interval: %q", linha)
	}
	if !ate.After(de) {
		t.Errorf("the gap was recorded with zero width (%s..%s): it sends the reader looking for a hole of no duration", de, ate)
	}
	if largura := ate.Sub(de); largura < 300*24*time.Hour {
		t.Errorf("the recorded gap has %.0f days; the provider returned 1 point of a ~1-year window",
			largura.Hours()/24)
	}
}

// gapBoundsFromLine reads the "missing A..B" from the recorded line.
func gapBoundsFromLine(t *testing.T, linha string) (time.Time, time.Time, bool) {
	t.Helper()
	i := strings.Index(linha, "gap_from=")
	if i < 0 {
		return time.Time{}, time.Time{}, false
	}
	resto := linha[i+len("gap_from="):]
	j := strings.Index(resto, " gap_to=")
	if j < 0 {
		return time.Time{}, time.Time{}, false
	}
	deRaw := resto[:j]
	resto = resto[j+len(" gap_to="):]
	k := strings.Index(resto, " window_from=")
	if k < 0 {
		return time.Time{}, time.Time{}, false
	}
	ateRaw := resto[:k]
	const layout = "2006-01-02 15:04:05 -0700 MST"
	de, err1 := time.Parse(layout, strings.TrimSpace(deRaw))
	ate, err2 := time.Parse(layout, strings.TrimSpace(ateRaw))
	if err1 != nil || err2 != nil {
		return time.Time{}, time.Time{}, false
	}
	return de, ate, true
}
