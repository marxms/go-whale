package collector

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/marxms/go-whale/internal/adapters/outbound/coinalyze"
	"github.com/marxms/go-whale/internal/market"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// CoinalyzeCycleInterval is the collection cadence. It is one hour because the value
// this collector brings is HISTORY (the existing source already serves real time), and
// pulling history faster than the shortest bucket it fills spends budget rewriting the
// same point.
const CoinalyzeCycleInterval = time.Hour

// CoinalyzeFleetWait is how long the first pass waits for the fleet to appear. Three
// minutes: the LAN discovery publishes in seconds, and the ceiling exists for the case
// where there is no discovery at all — not to accommodate a slow one.
const CoinalyzeFleetWait = 3 * time.Minute

// CoinalyzeFleetSettle is the interval between two fleet reads. Five seconds: shorter
// than the gap between the static list and discovery (measured ~20s on the LAN), and
// long enough not to turn the wait into a flood of SMEMBERS.
const CoinalyzeFleetSettle = 5 * time.Second

// CoinalyzeScope is one configured exchange scope as the second-provenance
// collector sees it. It is typed so this package does not import platform/config.
type CoinalyzeScope struct {
	ExchangeID       string
	MarketType       market.MarketType
	DiscoveryEnabled bool
	MaxAssets        int
	Symbols          []string
}

// CoinalyzeSymbolsOf collects the future symbols the operator configured,
// deduplicated. Futures is the only market this collector serves.
func CoinalyzeSymbolsOf(scopes []CoinalyzeScope) []string {
	seen := make(map[string]struct{})
	symbols := make([]string, 0, 16)
	for _, scope := range scopes {
		if scope.MarketType != market.Future {
			continue
		}
		for _, symbol := range scope.Symbols {
			if _, dup := seen[symbol]; dup {
				continue
			}
			seen[symbol] = struct{}{}
			symbols = append(symbols, symbol)
		}
	}
	return symbols
}

// CoinalyzeAdmission says whether the collection starts, and why.
type CoinalyzeAdmission struct {
	Start   bool
	Reason  string
	Symbols []string
}

// DecideCoinalyze is the admission policy: absent key degrades instead of taking
// the process down, because the streamer serves the existing source.
func DecideCoinalyze(enabled, apiKeyPresent bool, scopes []CoinalyzeScope) CoinalyzeAdmission {
	if !enabled {
		return CoinalyzeAdmission{Reason: "disabled by configuration (GOWHALE_COINALYZE_ENABLED)"}
	}
	if !apiKeyPresent {
		return CoinalyzeAdmission{Reason: "enabled without GOWHALE_COINALYZE_API_KEY — the existing source keeps serving"}
	}
	symbols := CoinalyzeSymbolsOf(scopes)
	if len(symbols) == 0 {
		return CoinalyzeAdmission{Reason: "no futures symbol configured"}
	}
	return CoinalyzeAdmission{Start: true, Symbols: symbols}
}

// CoinalyzeDeclaredCadence returns the cadence the LOAD imposes:
// symbols x timeframes x endpoints / ceiling.
//
// The configured interval is the FLOOR: a load that fits keeps the declared cadence,
// and a load that does not fit cannot be declared as if it did.
func CoinalyzeDeclaredCadence(configured time.Duration, symbols, timeframes int) time.Duration {
	if configured <= 0 || symbols <= 0 || timeframes <= 0 {
		return configured
	}
	calls := symbols * timeframes * CoinalyzeCallsPerBatch
	needed := time.Duration(float64(calls) / float64(coinalyze.CoinalyzeCallsPerMinute) * float64(time.Minute))
	if needed > configured {
		return needed
	}
	return configured
}

// ReportCoinalyzeCadence publishes the derived cadence with both numbers: the load's
// pass duration and the configured interval it violates. A load that does not fit is
// a WARN because the effective cadence becomes the pass duration.
func ReportCoinalyzeCadence(configured, declared time.Duration, symbols, timeframes int, logs ...*slog.Logger) {
	log := logger.OrDiscard(logs...)
	if declared > configured {
		log.Warn("coinalyze declared cadence does not fit the configured interval; the effective cadence is the pass duration",
			"declared", declared.Round(time.Minute),
			"symbols", symbols,
			"timeframes", timeframes,
			"calls_per_batch", CoinalyzeCallsPerBatch,
			"calls", symbols*timeframes*CoinalyzeCallsPerBatch,
			"calls_per_minute_ceiling", coinalyze.CoinalyzeCallsPerMinute,
			"configured", configured,
			"ratio", float64(declared)/float64(configured))
		return
	}
	log.Info("coinalyze declared cadence fits the configured interval",
		"declared", declared.Round(time.Second),
		"symbols", symbols,
		"timeframes", timeframes,
		"configured", configured)
}

// FleetScope is one exchange's contribution to fleet readiness.
type FleetScope struct {
	ExchangeID       string
	MarketType       market.MarketType
	DiscoveryEnabled bool
	MaxAssets        int
}

// FleetReady says whether EVERY discovery-driven futures exchange has published its
// fleet.
//
// What separates early from ready is not size nor time — it is WHO is missing. The
// configuration says exactly which exchanges are discovered, and one of them without
// published symbols is the definition of "not yet". A declared target is what decides;
// `counts > 0` gave a two-of-thirty fleet a pass.
func FleetReady(scopes []FleetScope, counts map[string]int) bool {
	for _, scope := range scopes {
		if scope.MarketType != market.Future || !scope.DiscoveryEnabled {
			continue
		}
		target := scope.MaxAssets
		if target <= 0 {
			target = 1
		}
		if counts[scope.ExchangeID] < target {
			return false
		}
	}
	return true
}

// ReadCoinalyzeFleet reads every FUTURES scope's active symbols, deduplicated and
// sorted. One unreadable scope must not blank the rest: a momentary registry failure
// in one of them cannot zero the collection of half the fleet.
func ReadCoinalyzeFleet(
	ctx context.Context,
	scopes []FleetScope,
	read func(ctx context.Context, exchange, marketType string) ([]string, error),
	logs ...*slog.Logger,
) []string {
	log := logger.OrDiscard(logs...)
	var all []string
	for _, scope := range scopes {
		if scope.MarketType != market.Future {
			continue
		}
		members, err := read(ctx, scope.ExchangeID, scope.MarketType.String())
		if err != nil {
			log.Warn("coinalyze fleet unreadable in this cycle",
				"exchange", scope.ExchangeID, "market_type", scope.MarketType, "error", err)
			continue
		}
		all = append(all, members...)
	}
	return SortedFleet(all)
}

// FleetCounts reads how many symbols each FUTURES exchange published. A spot scope
// sharing an exchange ID must not contribute to a derivatives collector's readiness.
// One unreadable exchange does not zero the others.
func FleetCounts(
	ctx context.Context,
	scopes []FleetScope,
	read func(ctx context.Context, exchange, marketType string) ([]string, error),
) map[string]int {
	counts := make(map[string]int, len(scopes))
	for _, scope := range scopes {
		if scope.MarketType != market.Future {
			continue
		}
		members, err := read(ctx, scope.ExchangeID, scope.MarketType.String())
		if err != nil {
			continue
		}
		counts[scope.ExchangeID] = len(members)
	}
	return counts
}

// SortedFleet returns the live fleet sorted and deduplicated. It exists so callers
// cannot accidentally rely on registry iteration order.
func SortedFleet(symbols []string) []string {
	seen := make(map[string]struct{}, len(symbols))
	out := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		if _, dup := seen[symbol]; dup {
			continue
		}
		seen[symbol] = struct{}{}
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}
