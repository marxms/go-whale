package collector

import (
	"context"
	"strings"
	"testing"

	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// A feed that does not declare its label convention is REFUSED before writing.
//
// The refusal lives at ingestion, not at alignment, for a diagnostic reason: here the feed has a
// NAME to put in the log. A silent fallback inside `bucketAlignMs` would be the guess the type
// exists to prevent, applied at the one place nobody would look — and the symptom would be an
// entire series displaced by one bucket with no red line.
//
// BEFORE ANY WRITE is the half that matters. A feed refused after writing has already
// contaminated the ZSET, and cleanup is manual. The double below fails if it is TOUCHED.

// refusingStore fails the test if any write reaches it.
type refusingStore struct {
	recordingStore
	t       *testing.T
	touched bool
}

func (s *refusingStore) SaveSnapshot(_ context.Context, snapshot *derivatives.DerivativesSnapshot) error {
	s.touched = true
	s.t.Fatalf("the store was touched by a feed without a declaration: %s", snapshot.Symbol)
	return nil
}

func (s *refusingStore) SaveSeriesOnly(_ context.Context, snapshot *derivatives.DerivativesSnapshot, timeframe string) error {
	s.touched = true
	s.t.Fatalf("series %s was written by a feed without a declaration: %s", timeframe, snapshot.Symbol)
	return nil
}

var _ pollerStore = (*refusingStore)(nil)

func TestFeedWithoutLabelDeclarationIsRefusedBeforeAnyWrite(t *testing.T) {
	store := &refusingStore{t: t}
	poller := &DerivativesPoller{store: store}

	if err := poller.admitFeed("coinalyze"); err == nil {
		t.Fatal("a feed without a declaration had to be refused")
	}
	if store.touched {
		t.Fatal("the refusal happened AFTER writing, which is too late")
	}
}

// THE LOG IS THE DIAGNOSIS, and it must name the feed: "missing convention" without a name sends
// the maintainer searching through all of them.
func TestRefusalNamesTheFeed(t *testing.T) {
	poller := &DerivativesPoller{store: &recordingStore{}}

	err := poller.admitFeed("coinalyze")
	if err == nil {
		t.Fatal("expected a refusal")
	}
	if !strings.Contains(err.Error(), "coinalyze") {
		t.Fatalf("the refusal did not name the feed: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "unspecified") {
		t.Fatalf("the refusal did not say what was missing: %q", err.Error())
	}
}

// REFUSING A DECLARATION THAT DOES NOT HOLD UP NAMES THE ENDPOINT. With per-endpoint
// declarations, "the feed is wrong" stopped being actionable: there are three rows, and the
// maintainer needs to know which one fell.
func TestRefusalNamesTheEndpointThatDoesNotHold(t *testing.T) {
	broken := []labelDeclaration{{
		venue:    venueBinance,
		feed:     feedOpenInterest,
		endpoint: "/futures/data/openInterestHist",
		nature:   feedNatureUnspecified,
		label:    derivatives.LabelIntervalStart,
		evidence: "anything",
	}}

	err := admitDeclarations(venueBinance, broken)
	if err == nil {
		t.Fatal("a declaration without nature had to be refused")
	}
	if !strings.Contains(err.Error(), "/futures/data/openInterestHist") {
		t.Fatalf("the refusal did not name the endpoint: %q", err.Error())
	}
}

// POSITIVE CONTROL on BOTH conventions: without it, an admission that refused everything would
// pass the cases above and take down the collection that already works. The convention pair is
// exercised in the RULE, which is where it exists — the production table declares one per
// endpoint, and deriving the positive from it would measure the table, not the rule.
func TestDeclaredFeedIsAdmitted(t *testing.T) {
	for _, declared := range []derivatives.LabelConvention{derivatives.LabelIntervalStart, derivatives.LabelIntervalEnd} {
		if err := admitDeclaration(labelDeclaration{
			venue:    venueBinance,
			feed:     feedOpenInterest,
			endpoint: "/futures/data/openInterestHist",
			nature:   feedNaturePointSample,
			label:    declared,
			evidence: "measured point sample",
		}); err != nil {
			t.Fatalf("%s was declared and should be admitted: %v", declared, err)
		}
	}

	// AND THE WHOLE PRODUCTION FEED PASSES THROUGH THE DOOR, with the three rows as they are.
	poller := &DerivativesPoller{store: &recordingStore{}}
	if err := poller.admitFeed(venueBinance); err != nil {
		t.Fatalf("the feed that already runs was refused: %v", err)
	}
}

// THE EXISTING COLLECTOR declares `interval-start` — now ENDPOINT BY ENDPOINT, and the
// enumeration comes from the identity this package already publishes (`declaredFeeds`), never
// from a second hand-written list, which would be the divergence this case exists to catch.
//
// THE BASIS OF THE DECLARATION IS EACH ROW'S EVIDENCE, and the difference is written on every row
// of the table. The retracted measurement does not distinguish between the two conventions for
// these endpoints; the additive falsifier confirms the primary doc for `takerlongshortRatio` —
// which is NOT one of these three endpoints. What is required here is the declaration PER
// ENDPOINT and a NAMED basis per row; not the transfer of a measurement that does not cover these
// endpoints.
func TestBinancePollerDeclaresIntervalStart(t *testing.T) {
	poller := NewDerivativesPoller(t.Context(),
		config.DerivativesConfig{Enabled: true, MaxSeriesSize: 200},
		&recordingStore{}, nil, nil, NewFundingProfileRegistry(), nil, nil,
	)

	if err := poller.admitFeed(venueBinance); err != nil {
		t.Fatalf("the collector that already runs was refused: %v", err)
	}

	declared := declarationsFor(venueBinance)
	var vistos int
	for _, feed := range declaredFeeds() {
		var achadas []labelDeclaration
		for _, d := range declared {
			if d.feed == feed {
				achadas = append(achadas, d)
			}
		}
		if len(achadas) != 1 {
			t.Fatalf("feed %q has %d declaration(s); each ingested endpoint declares its own, once", feed, len(achadas))
		}
		d := achadas[0]
		if d.label != derivatives.LabelIntervalStart {
			t.Errorf("%s (%s) declares %s, and the measured value belongs to the label instant", d.endpoint, feed, d.label)
		}
		if d.endpoint == "" {
			t.Errorf("%s is declared without the path read — without it nobody reaches the primary doc", feed)
		}
		if d.evidence == "" {
			t.Errorf("%s is declared with no named basis", feed)
		}
		vistos++
	}
	if vistos != len(declaredFeeds()) || vistos == 0 {
		t.Fatalf("enumerated %d of %d declared feeds — the comparison would pass by vacuity", vistos, len(declaredFeeds()))
	}

	// AND THE ENDPOINTS ARE DISTINCT: three rows with the same path would be one declaration
	// with three names.
	caminhos := map[string]bool{}
	for _, d := range derivativesLabelDeclarations() {
		if caminhos[d.endpoint] {
			t.Errorf("endpoint %q is declared more than once", d.endpoint)
		}
		caminhos[d.endpoint] = true
	}
}

// THE TWO HALVES OF THE RULE, each with its positive control — otherwise "refuse everything"
// would pass.
//
// THE FIRST IS THE ADDITIVE ONE: a feed whose field is SUMMED over the interval declares the
// convention with a falsifier that separates it from the alternative, because that is where the
// two predict different numbers. Without it, two different natures CANNOT share the declared
// convention — the clause measurement requires.
func TestAdditiveDeclarationWithoutFalsifierIsRefused(t *testing.T) {
	sem := labelDeclaration{
		venue:    venueBinance,
		feed:     feedLongShortRatio,
		endpoint: "/futures/data/takerlongshortRatio",
		nature:   feedNatureAdditive,
		label:    derivatives.LabelIntervalStart,
		evidence: "sum of sub-buckets",
	}
	if err := admitDeclaration(sem); err == nil {
		t.Fatal("an additive feed without a falsifier had to be refused — it is the nature where the two conventions predict different numbers")
	}

	com := sem
	com.falsifier = "sum of sub-buckets equals the bar labeled T (measured deviation 0.111%)"
	if err := admitDeclaration(com); err != nil {
		t.Fatalf("an additive feed WITH a falsifier was refused: %v", err)
	}

	// THE POINT SAMPLE DOES NOT NEED ONE, and this is measurement, not indulgence: no test
	// separates the two conventions when the value belongs to the label instant.
	pontual := sem
	pontual.nature = feedNaturePointSample
	if err := admitDeclaration(pontual); err != nil {
		t.Fatalf("a point sample was refused without a falsifier: %v", err)
	}
}

// THE SECOND HALF: nature and basis are required. A declaration without nature would pass as a
// point sample without anyone asking, and it is precisely that one carrying the additive rule.
func TestDeclarationWithoutNatureOrEvidenceIsRefused(t *testing.T) {
	base := labelDeclaration{
		venue:    venueBinance,
		feed:     feedFundingRate,
		endpoint: "/fapi/v1/fundingRate",
		nature:   feedNaturePointSample,
		label:    derivatives.LabelIntervalStart,
		evidence: "settlement event",
	}
	if err := admitDeclaration(base); err != nil {
		t.Fatalf("a complete declaration was refused: %v", err)
	}

	semNatureza := base
	semNatureza.nature = feedNatureUnspecified
	if err := admitDeclaration(semNatureza); err == nil {
		t.Fatal("a declaration without nature had to be refused")
	}

	semBase := base
	semBase.evidence = ""
	if err := admitDeclaration(semBase); err == nil {
		t.Fatal("a declaration without a named basis had to be refused")
	}

	// AND THE MISSING CONVENTION IS STILL REFUSED — the nature and basis checks do not replace it.
	semConvencao := base
	semConvencao.label = derivatives.LabelUnspecified
	if err := admitDeclaration(semConvencao); err == nil {
		t.Fatal("a convention without a declaration had to be refused")
	}
}

// THE REFUSAL CLOSES THE WHOLE CYCLE, not just the write.
//
// A cycle that only discovered the missing convention at write time would already have paid the
// REST call and held the point in hand — and the temptation to "use it since we fetched it" is
// exactly how a series aligned by guess enters production. The refusal is a DOOR, not an output
// filter, and it does not count as a symbol error: nothing was attempted, and counting an error
// would read as "the venue is down" when the venue was never called.
func TestUndeclaredFeedStopsTheCycleBeforeFetching(t *testing.T) {
	store := &refusingStore{t: t}
	poller := &DerivativesPoller{
		cfg:     config.DerivativesConfig{Enabled: true, PollIntervalSec: 60},
		store:   store,
		symbols: []SymbolScope{{ExchangeID: "coinalyze", MarketType: "future", Symbol: "BTC/USDT"}},
	}

	stats := poller.pollAll(context.Background())

	if !stats.refused {
		t.Fatal("the cycle should have been refused")
	}
	if stats.ok != 0 || stats.errors != 0 {
		t.Fatalf("nothing was attempted, so ok and errors must be zero: ok=%d errors=%d", stats.ok, stats.errors)
	}
	if store.touched {
		t.Fatal("the store was touched by a refused cycle")
	}
}

// THE LEVEL IS PART OF THE CONTRACT: *"SHALL refuse ingestion of that feed and log at error level
// naming the feed"*.
//
// The returned error is already asserted above, and it rises through the stack — but whoever reads
// the process in production reads the log, and the default level is `Error`. A downgrade here
// would leave a refused feed disappearing silently: no series written, no visible line.
func TestRefusalLogsAtErrorLevel(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	poller := &DerivativesPoller{store: &recordingStore{}, log: log}
	if err := poller.admitFeed("coinalyze"); err == nil {
		t.Fatal("expected a refusal")
	}

	level, found := sink.LevelOf("coinalyze")
	if !found {
		t.Fatalf("the refusal logged nothing; captured: %v", sink.Records())
	}
	if level != logger.LevelError {
		t.Errorf("the refusal came out at %s, and the contract requires %s", level, logger.LevelError)
	}
}

// NEGATIVE CONTROL: a declared feed must log nothing at ERROR, otherwise the assertion above does
// not distinguish refusal from admission.
func TestAdmittedFeedLogsNothingAtErrorLevel(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)

	poller := &DerivativesPoller{store: &recordingStore{}, log: log}
	if err := poller.admitFeed("binance"); err != nil {
		t.Fatalf("declared feed was refused: %v", err)
	}

	for _, r := range sink.Records() {
		if r.Level == logger.LevelError {
			t.Errorf("admitted feed logged at ERROR: %q", r.Text())
		}
	}
}
