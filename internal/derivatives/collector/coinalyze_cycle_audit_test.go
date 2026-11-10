package collector

import (
	"context"
	"strings"
	"testing"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// THE AUDIT MEASURED IN THE CYCLE, NOT IN THE FUNCTION.
//
// The audit had ten unit cases and none proved the CYCLE calls it: the call existed and nothing
// asserted it. That is the conjunction rule failing — each half green, the whole uncovered. And
// the second half was in fact broken: `fetchLongShortInto` iterated the raw response without
// auditing, on a third of the cycle's history requests.
//
// These cases exercise `PollOnce` end to end against a provider that omits or truncates, and
// assert what COMES OUT: the log line, with its level and the symbol name.

// ausenteBody is a well-formed response that simply does not carry the requested symbol — the
// silent failure mode the audit exists to catch. It is not an error, not a 404, not an empty
// array: it is an array with another symbol inside.
const ausenteBody = `[{"symbol":"ETHUSDT_PERP.A","history":[
	{"t":1787270400,"o":1.0,"h":1.0,"l":1.0,"c":1.0}
]}]`

const ausenteLongShortBody = `[{"symbol":"ETHUSDT_PERP.A","history":[
	{"t":1787270400,"r":0.9,"l":47.0,"s":53.0}
]}]`

// countWarns counts WARN lines naming the symbol AND the reason.
func countWarns(t *testing.T, sink *logger.Sink, symbol, reason string) int {
	t.Helper()
	n := 0
	for _, r := range sink.Records() {
		if r.Level == logger.LevelWarn && strings.Contains(r.Text(), symbol) && strings.Contains(r.Text(), reason) {
			n++
		}
	}
	return n
}

// TestCoinalyzeCycle_AuditsEveryHistoryEndpoint is the case that closes the conjunction.
//
// The provider answers ALL THREE history requests without the requested symbol. A cycle auditing
// only two of them would produce two warnings; the requirement makes no endpoint exception, and
// long/short was exactly the missing one.
func TestCoinalyzeCycle_AuditsEveryHistoryEndpoint(t *testing.T) {
	poller, _, _, _, sink := newCoinalyzeHarness(t, []string{"1d"}, map[string]string{
		"/open-interest-history":    ausenteBody,
		"/funding-rate-history":     ausenteBody,
		"/long-short-ratio-history": ausenteLongShortBody,
	})

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	got := countWarns(t, sink, "BTCUSDT_PERP.A", "absent from the history response")
	if got != 3 {
		t.Fatalf("the cycle audited %d of the 3 history requests; captured: %v", got, sink.Records())
	}
}

// TestCoinalyzeCycle_AuditsLongShortSpecifically isolates the endpoint that was not audited.
//
// Only long/short omits the symbol; the other two answer normally. A cycle ignoring this endpoint
// would produce ZERO warnings and write the series with no long/short ratio at all, silently.
func TestCoinalyzeCycle_AuditsLongShortSpecifically(t *testing.T) {
	poller, _, _, _, sink := newCoinalyzeHarness(t, []string{"1d"}, map[string]string{
		"/long-short-ratio-history": ausenteLongShortBody,
	})

	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	if got := countWarns(t, sink, "BTCUSDT_PERP.A", "absent from the history response"); got != 1 {
		t.Fatalf("the missing long/short produced %d warning(s), expected 1; captured: %v", got, sink.Records())
	}
}

// TestCoinalyzeCycle_MarksTheTruncatedWindowAsPartial: the requested window is 72h and the provider
// returns ONE point at its start. The gap must come out with the interval, not as a vague "something
// was missing".
func TestCoinalyzeCycle_MarksTheTruncatedWindowAsPartial(t *testing.T) {
	poller, _, _, _, sink := newCoinalyzeHarness(t, []string{"1d"}, nil)
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	if got := countWarns(t, sink, "BTCUSDT_PERP.A", "partially covered"); got == 0 {
		t.Fatalf("the truncated window was not marked as partial; captured: %v", sink.Records())
	}
	// The gap names the missing interval — "partially covered" without dates sends the operator
	// hunting for the hole by hand.
	var withRange bool
	for _, r := range sink.Records() {
		if strings.Contains(r.Text(), "partially covered") && strings.Contains(r.Text(), "gap_from=") &&
			strings.Contains(r.Text(), "gap_to=") {
			withRange = true
		}
	}
	if !withRange {
		t.Errorf("the gap came out without the missing interval: %v", sink.Records())
	}
}

// NEGATIVE CONTROL: a response carrying the requested symbol must not produce an absence warning,
// otherwise the cases above would pass with a warning fired every time.
func TestCoinalyzeCycle_CompleteResponseNamesNobodyAsMissing(t *testing.T) {
	poller, _, _, _, sink := newCoinalyzeHarness(t, []string{"1d"}, nil)
	if err := poller.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	if got := countWarns(t, sink, "BTCUSDT_PERP.A", "absent from the history response"); got != 0 {
		t.Errorf("a complete response reported absence %d time(s): %v", got, sink.Records())
	}
}
