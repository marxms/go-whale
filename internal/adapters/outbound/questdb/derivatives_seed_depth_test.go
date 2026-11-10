package questdb

import "testing"

// THE READ THAT MAKES THE SEEDED MARK SURVIVE A DEPLOY.
//
// The question is DEPTH, and that is what the query must ask: `min(timestamp)` per symbol.
// Counting rows would answer something else — a symbol with 2000 tail cycles has 2000 rows and
// no deep history, and would be taken as seeded.
func TestDerivativesOldestBySymbolQuery_AsksForDepthAndBindsTheWholeScope(t *testing.T) {
	query := derivativesOldestBySymbolQuery

	for _, want := range []string{
		"min(timestamp)",
		"GROUP BY symbol",
		"FROM derivatives",
		"source = $1",
		"exchange = $2",
		"market_type = $3",
		"timeframe = $4",
	} {
		if !contains(query, want) {
			t.Fatalf("query does not contain %q:\n%s", want, query)
		}
	}
	// COUNTING IS NOT DEPTH: if the aggregation becomes `count()`, the mark starts being granted
	// to whoever has only a tail.
	if contains(query, "count(") {
		t.Fatalf("the query counts rows instead of measuring depth:\n%s", query)
	}
	// AND THE SCOPE NEVER BECOMES SQL TEXT.
	for _, forbidden := range []string{"coinalyze", "binance", "future", "'1d'"} {
		if contains(query, forbidden) {
			t.Fatalf("query interpolates %q instead of binding it:\n%s", forbidden, query)
		}
	}
}
