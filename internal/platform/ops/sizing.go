// Package ops owns operational policy and reporting that is not a domain rule and
// not a transport adapter: the sizing sheet, its gate-facing tables, and the durable
// disk budget published next to it.
package ops

// sizing_sheet.go — the streamer's SIZING SHEET.
//
// The sheet exists because the configured value is not the effective one. Each row carries the
// effective value, its ORIGIN (the config key that produced it) and its DERIVATION (how it got
// there), and the total is printed SIDE BY SIDE with the measured `used_memory`.
//
// A = SCARD active_symbols:{exchange}:{marketType}, NOT `discovery.max_assets`.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	redisstore "github.com/marxms/go-whale/internal/adapters/outbound/redis"
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/derivatives/collector"
	"github.com/marxms/go-whale/internal/platform/config"
	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// notAvailable is the printable marker for a value the process cannot measure. It is
// deliberately not a zero: zero is a measurement.
const notAvailable = "n/d"

// KeyPrefix is where each service publishes its own line so that the TOTAL exists
// somewhere.
const KeyPrefix = "sizing:v1:"

// ServiceGoWhale names this service's line.
const ServiceGoWhale = "gowhale"

// CandleCapConfigKey names the key that RETAINS the candles family.
const CandleCapConfigKey = "candle_engine.redis_cap_by_timeframe"

const (
	// CoefficientSample is how many keys the coefficient fit reads.
	CoefficientSample = 40
	// DepthSample is how many keys it reads per timeframe to estimate available depth.
	DepthSample = 20
)

// Redis is the slice of the Redis client the sheet needs. *redis.Client satisfies it.
type Redis interface {
	redisstore.CardinalityMeasurer
	redisstore.KeyScanner
	ZCard(ctx context.Context, key string) *redis.IntCmd
	SCard(ctx context.Context, key string) *redis.IntCmd
	TTL(ctx context.Context, key string) *redis.DurationCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
}

// FamilyLine is one family's row of the sheet.
type FamilyLine struct {
	Family string
	// Origin names the config key the effective number came out of.
	Origin string
	// Derivation says HOW the effective number was arrived at.
	Derivation string
	// Notes carry the per-timeframe surprises.
	Notes      []string
	Projection redisstore.FamilyProjection
}

// Sheet is one service's declaration of what it intends to keep resident.
type Sheet struct {
	Service        string
	PublishedAtMs  int64
	Universe       int64
	UniverseSource string
	Facts          redisstore.ServerFacts
	Families       []FamilyLine
	TotalMiB       float64
	// WarnRatio is retention.budget_warn_ratio. Zero means UNDECLARED.
	WarnRatio float64
	// DurableTimeframes is the derived durable coverage.
	DurableTimeframes []string
	// DerivativesRetainedWindow is the depth retention actually leaves in `derivatives`.
	DerivativesRetainedWindow time.Duration
}

// SheetInput carries everything the sheet is built from, so the build is testable
// without a whole App.
type SheetInput struct {
	Service           string
	Client            Redis
	UniverseCount     int64
	SampleBudget      int
	Families          []FamilyInput
	Facts             redisstore.ServerFacts
	WarnRatio         float64
	NowMs             int64
	UniverseSource    string
	DurableTimeframes []string
	DerivativesWindow time.Duration
}

// FamilyInput describes one family to project.
type FamilyInput struct {
	Family string
	// KeyPattern is the SCAN pattern used both to sample cost coefficients and to
	// sample the available depth per timeframe.
	KeyPattern string
	// KeysPerSymbol is m_F(tf).
	KeysPerSymbol map[string]float64
	// CapByTimeframe is the EFFECTIVE cap — already floored, already defaulted.
	CapByTimeframe map[string]int64
	// AvailableByTimeframe is disp_F(tf), sampled from the fleet.
	AvailableByTimeframe map[string]int64
	Origin               string
	Derivation           string
	Notes                []string
}

// ResolveUniverse answers A the way both services can: SCARD on the active-symbol set.
func ResolveUniverse(ctx context.Context, c Redis, key string) (int64, string) {
	n, err := c.SCard(ctx, key).Result()
	source := fmt.Sprintf("SCARD %s", key)
	if err != nil {
		return 0, source + " (unreadable)"
	}
	return n, source
}

// BuildSheetFrom assembles the sheet. It measures the coefficients of every family
// from that family's OWN keys — never from a constant — and projects.
func BuildSheetFrom(ctx context.Context, in SheetInput) Sheet {
	sheet := Sheet{
		Service:                   in.Service,
		PublishedAtMs:             in.NowMs,
		Universe:                  in.UniverseCount,
		UniverseSource:            in.UniverseSource,
		Facts:                     in.Facts,
		WarnRatio:                 in.WarnRatio,
		DurableTimeframes:         in.DurableTimeframes,
		DerivativesRetainedWindow: in.DerivativesWindow,
	}

	for _, fam := range in.Families {
		line := FamilyLine{
			Family:     fam.Family,
			Origin:     fam.Origin,
			Derivation: fam.Derivation,
			Notes:      fam.Notes,
		}

		keys, err := redisstore.SampleFamilyKeys(ctx, in.Client, fam.KeyPattern, in.SampleBudget)
		if err != nil || len(keys) == 0 {
			line.Notes = append(line.Notes, fmt.Sprintf("coefficients NOT measured (empty sample in %q)", fam.KeyPattern))
			sheet.Families = append(sheet.Families, line)
			continue
		}
		coef, err := redisstore.MeasureCoefficients(ctx, in.Client, fam.Family, keys)
		if err != nil {
			// THERE IS NO LITERAL FALLBACK. Without measurement there is no projection.
			line.Notes = append(line.Notes, fmt.Sprintf("coefficients NOT measured: %v", err))
			sheet.Families = append(sheet.Families, line)
			continue
		}

		line.Projection = redisstore.ProjectFamily(redisstore.FamilyProjectionInput{
			Family:               fam.Family,
			Universe:             sheet.Universe,
			KeysPerSymbol:        fam.KeysPerSymbol,
			CapByTimeframe:       fam.CapByTimeframe,
			AvailableByTimeframe: fam.AvailableByTimeframe,
			Coefficients:         coef,
		})
		sheet.TotalMiB += line.Projection.TotalMiB
		sheet.Families = append(sheet.Families, line)
	}

	return sheet
}

// Lines renders the sheet: effective value, origin, derivation, horizon, projection —
// and the measured occupancy next to the projected one.
func (s Sheet) Lines() []string {
	out := []string{
		fmt.Sprintf("[SIZING][%s] A=%d (%s) · maxmemory=%s · used_memory=%s · container=%s",
			s.Service, s.Universe, s.UniverseSource,
			FormatMiB(s.Facts.MaxMemoryBytes), FormatMiB(s.Facts.UsedMemoryBytes),
			FormatMiB(s.Facts.ContainerLimitBytes)),
	}
	if len(s.Facts.Unreadable) > 0 {
		out = append(out, fmt.Sprintf("[SIZING][%s]   facts NOT read (remain zero, never a literal): %s",
			s.Service, strings.Join(s.Facts.Unreadable, ", ")))
	}

	for _, fam := range s.Families {
		out = append(out, fmt.Sprintf("[SIZING][%s] %s  projection=%.1f MiB", s.Service, fam.Family, fam.Projection.TotalMiB))
		out = append(out, fmt.Sprintf("[SIZING][%s]   origin: %s · derivation: %s", s.Service, fam.Origin, fam.Derivation))
		out = append(out, fmt.Sprintf("[SIZING][%s]   coefficients: %s", s.Service, fam.Projection.Coefficients.String()))
		for _, row := range fam.Projection.ByTimeframe {
			out = append(out, fmt.Sprintf("[SIZING][%s]   %-4s cap=%d avail=%d effective=%d horizon=%s projection=%.1f MiB",
				s.Service, row.Timeframe, row.Cap, row.Available, row.EffectiveDepth,
				FormatHorizon(row.HorizonSeconds), row.MiB))
		}
		for _, note := range fam.Notes {
			out = append(out, fmt.Sprintf("[SIZING][%s]   NOTE: %s", s.Service, note))
		}
	}

	out = append(out, DurableCoverageLines(s.Service, DurableFieldCoverage())...)
	out = append(out, DerivativesBudgetLines(s.Service, s.DurableTimeframes, s.DerivativesRetainedWindow)...)

	total := fmt.Sprintf("[SIZING][%s] TOTAL projected=%.1f MiB", s.Service, s.TotalMiB)
	if ratio := s.Facts.OccupancyRatio(); ratio > 0 {
		total += fmt.Sprintf(" · used_memory MEASURED=%s (%.1f%% of maxmemory)",
			FormatMiB(s.Facts.UsedMemoryBytes), ratio*100)
	}
	if ratio := s.Facts.ContainerOccupancyRatio(); ratio > 0 {
		total += fmt.Sprintf(" · %.1f%% of the container ceiling", ratio*100)
	}
	if s.WarnRatio > 0 {
		total += fmt.Sprintf(" · TARGET<=%.0f%%", s.WarnRatio*100)
	} else {
		total += " · TARGET not declared (retention.budget_warn_ratio missing)"
	}
	out = append(out, total)
	return out
}

// DurableCoverageLines prints the durable coverage at boot, PER FIELD. Coverage is written
// AND readable: classification uses the same predicate the startup gate consumes.
func DurableCoverageLines(service string, coverage config.DurableCellCoverage) []string {
	coverage.DurableTimeframes = nil
	findings := coverage.UncoveredMemberFields("")

	uncovered := func(field string) bool {
		for _, finding := range findings {
			if strings.HasPrefix(finding, field+" (") {
				return true
			}
		}
		return false
	}

	columns := coverage.FieldColumn
	covered := make([]string, 0, len(columns))
	for field, column := range columns {
		if uncovered(field) {
			continue
		}
		covered = append(covered, field+"→"+column)
	}
	sort.Strings(covered)
	sort.Strings(findings)

	return []string{
		fmt.Sprintf("[SIZING][%s] derivatives:series  PER-FIELD durable tier (L-DERIV-COLD): %d of %d quantities are WRITTEN AND READABLE",
			service, len(covered), len(columns)),
		fmt.Sprintf("[SIZING][%s]   written AND readable: %s", service, strings.Join(covered, ", ")),
		fmt.Sprintf("[SIZING][%s]   NOT covered (the cap removes the whole member, and they go with it): %s",
			service, strings.Join(findings, " | ")),
	}
}

// DurableFieldCoverage assembles the two sides the gate requires, each from its source:
// the declaration comes from `storage`, and the readable list from the query that
// actually reads.
func DurableFieldCoverage() config.DurableCellCoverage {
	return config.DurableCellCoverage{
		FieldColumn:     redisstore.DerivativesDurableCoverage(),
		ReadableColumns: derivatives.DurableReadColumns(),
	}
}

// FormatMiB renders a byte count for the Redis facts.
func FormatMiB(bytes int64) string {
	if bytes <= 0 {
		return notAvailable
	}
	return fmt.Sprintf("%.0f MiB", float64(bytes)/float64(1<<20))
}

// FormatHorizon renders a duration in a human-readable horizon unit.
func FormatHorizon(seconds int64) string {
	switch {
	case seconds <= 0:
		return notAvailable
	case seconds < 86400:
		return fmt.Sprintf("%.1fh", float64(seconds)/3600)
	case seconds < 86400*365:
		return fmt.Sprintf("%.1fd", float64(seconds)/86400)
	default:
		return fmt.Sprintf("%.1fa", float64(seconds)/(86400*365))
	}
}

// publishedLine is the payload each service leaves at sizing:v1:{service}.
type publishedLine struct {
	Service       string  `json:"service"`
	PublishedAtMs int64   `json:"publishedAtMs"`
	Universe      int64   `json:"universe"`
	Source        string  `json:"universeSource"`
	TotalMiB      float64 `json:"totalMiB"`
}

// PublishSheet leaves this service's line where the other one can find it.
func PublishSheet(ctx context.Context, c Redis, sheet Sheet) error {
	payload, err := json.Marshal(publishedLine{
		Service:       sheet.Service,
		PublishedAtMs: sheet.PublishedAtMs,
		Universe:      sheet.Universe,
		Source:        sheet.UniverseSource,
		TotalMiB:      sheet.TotalMiB,
	})
	if err != nil {
		return fmt.Errorf("marshal sizing line: %w", err)
	}
	// No expiration: see the retention note on KeyPrefix.
	return c.Set(ctx, KeyPrefix+sheet.Service, payload, 0).Err()
}

// AggregateMiB reads every service's published line and returns the total plus a
// human-readable breakdown that carries the AGE of each line — a stale line is
// reported as stale rather than deleted behind the operator's back.
func AggregateMiB(ctx context.Context, c Redis, nowMs int64) (float64, []string, error) {
	keys, err := redisstore.SampleFamilyKeys(ctx, c, KeyPrefix+"*", 64)
	if err != nil {
		return 0, nil, err
	}
	if len(keys) == 0 {
		return 0, nil, nil
	}
	sort.Strings(keys)

	values, err := c.MGet(ctx, keys...).Result()
	if err != nil {
		return 0, nil, fmt.Errorf("mget sizing lines: %w", err)
	}

	var total float64
	parts := make([]string, 0, len(values))
	for _, raw := range values {
		text, ok := raw.(string)
		if !ok {
			continue
		}
		var line publishedLine
		if err := json.Unmarshal([]byte(text), &line); err != nil {
			continue
		}
		total += line.TotalMiB
		ageMin := float64(nowMs-line.PublishedAtMs) / 60000.0
		parts = append(parts, fmt.Sprintf("%s=%.1f MiB (A=%d, published %.0f min ago)",
			line.Service, line.TotalMiB, line.Universe, ageMin))
	}
	return total, parts, nil
}

// CandleCapByTimeframe resolves the EFFECTIVE candle ZSET cap per timeframe, with
// origin and derivation notes.
//
// THE ZSET CAP AND THE RING CAP ARE DIFFERENT KEYS: resolution is RedisCapFor, the SAME
// authority redis_store.go applies. Reimplementing the order here, or reading
// `buffer_sizes` directly, is how the sheet started publishing 800 where the store applies 521.
func CandleCapByTimeframe(ce config.CandleEngineConfig, timeframes []string) (map[string]int64, []string) {
	caps := make(map[string]int64, len(timeframes))
	declared := ce.DeclaredRedisCaps()

	var notes []string
	var inherited []string
	for _, tf := range timeframes {
		caps[tf] = int64(ce.RedisCapFor(tf))
		if _, ok := declared[normalizeTimeframeKey(tf)]; !ok {
			inherited = append(inherited, tf)
		}
	}
	if len(inherited) > 0 {
		pairs := make([]string, 0, len(inherited))
		for _, tf := range inherited {
			pairs = append(pairs, fmt.Sprintf("%s=%d", tf, caps[tf]))
		}
		notes = append(notes, fmt.Sprintf(
			"%s WITHOUT its own entry in %s — they inherit candle_engine.buffer_sizes (%s)",
			strings.Join(inherited, ","), CandleCapConfigKey, strings.Join(pairs, ", ")))
	}
	return caps, notes
}

// CandleSizingFamily assembles the candles family from the EFFECTIVE config.
func CandleSizingFamily(
	ce config.CandleEngineConfig,
	timeframes []string,
	keyPattern string,
	available map[string]int64,
) FamilyInput {
	caps, notes := CandleCapByTimeframe(ce, timeframes)
	return FamilyInput{
		Family:               "candles",
		KeyPattern:           keyPattern,
		KeysPerSymbol:        UniformKeysPerSymbol(timeframes),
		CapByTimeframe:       caps,
		AvailableByTimeframe: available,
		Origin:               CandleCapConfigKey,
		Derivation: "ZSET cap resolved by CandleEngineConfig.RedisCapFor, the SAME authority the store " +
			"applies (" + CandleCapConfigKey + "[tf] → [default] → candle_engine.buffer_sizes[tf] → [default]); " +
			"chart depth is another key and does not move resident bytes",
		Notes: notes,
	}
}

// CandleCapEntries are the entries WRITTEN in the dedicated key, which is the
// question of the `timeframe-without-explicit-cap-entry` gate.
func CandleCapEntries(ce config.CandleEngineConfig) map[string]int64 {
	declared := ce.DeclaredRedisCaps()
	out := make(map[string]int64, len(declared))
	for tf, v := range declared {
		out[tf] = int64(v)
	}
	return out
}

// normalizeTimeframeKey mirrors the config package's normalizer: keys come from
// hand-written YAML, so `1D` and ` 1d` must reach the same timeframe as `1d`.
func normalizeTimeframeKey(timeframe string) string {
	return strings.ToLower(strings.TrimSpace(timeframe))
}

// DerivativesCapSource is the store's own effective-cap authority.
type DerivativesCapSource interface {
	SeriesHorizonMs(timeframe string) int64
}

// DerivativesCapByTimeframe resolves the EFFECTIVE derivatives cap per timeframe and
// says where each number came from — including the floor that raised a declared cap.
func DerivativesCapByTimeframe(src DerivativesCapSource, declared func(string) int, timeframes []string) (map[string]int64, []string) {
	caps := make(map[string]int64, len(timeframes))
	var notes []string
	for _, tf := range timeframes {
		sec := candle.ParseTimeframeToSeconds(tf)
		if sec <= 0 {
			continue
		}
		effective := src.SeriesHorizonMs(tf) / (sec * 1000)
		caps[tf] = effective
		if d := int64(declared(tf)); d != effective {
			notes = append(notes, fmt.Sprintf("%s: declared=%d → EFFECTIVE=%d (raised to the baseline floor)", tf, d, effective))
		}
	}
	return caps, notes
}

// SampleAvailableDepth measures disp_F(tf) — what the fleet actually holds — by
// sampling the family's own keys.
func SampleAvailableDepth(ctx context.Context, c Redis, pattern string, timeframes []string, budget int) map[string]int64 {
	out := make(map[string]int64, len(timeframes))
	for _, tf := range timeframes {
		keys, err := redisstore.SampleFamilyKeys(ctx, c, pattern+":"+tf, budget)
		if err != nil || len(keys) == 0 {
			continue
		}
		var sum, n int64
		for _, k := range keys {
			card, err := c.ZCard(ctx, k).Result()
			if err != nil || card <= 0 {
				continue
			}
			sum += card
			n++
		}
		if n > 0 {
			out[tf] = sum / n
		}
	}
	return out
}

// Families assembles the resident families THIS service projects. It is separate
// because it is the MEASURED side of the retention declaration: ResidentFamilies says
// which families the streamer retains and by which mechanism, and a test crosses the
// two lists in both directions.
func Families(
	ctx context.Context,
	client Redis,
	ce config.CandleEngineConfig,
	derivDeclaredCap func(string) int,
	derivStore DerivativesCapSource,
	timeframes []string,
	scope string,
) []FamilyInput {
	candlePattern := "candles:" + scope + ":*"
	families := []FamilyInput{CandleSizingFamily(
		ce,
		timeframes,
		candlePattern,
		SampleAvailableDepth(ctx, client, candlePattern, timeframes, DepthSample),
	)}

	if derivStore != nil {
		derivCaps, derivNotes := DerivativesCapByTimeframe(derivStore, derivDeclaredCap, timeframes)
		derivPattern := "derivatives:series:" + scope + ":*"
		families = append(families, FamilyInput{
			Family:               "derivatives:series",
			KeyPattern:           derivPattern,
			KeysPerSymbol:        UniformKeysPerSymbol(timeframes),
			CapByTimeframe:       derivCaps,
			AvailableByTimeframe: SampleAvailableDepth(ctx, client, derivPattern, timeframes, DepthSample),
			Origin:               "derivatives.series_cap_by_timeframe / derivatives.max_series_size",
			Derivation:           "declared, raised to the baselineFetchCount(tf) floor when lower — the floor is not configurable downward",
			Notes:                derivNotes,
		})
	}
	return families
}

// ResidentFamily is ONE declared resident family.
type ResidentFamily struct {
	Name string
	// PatternFor returns the family's SCAN pattern within a scope.
	PatternFor func(scope string) string
	// Owner is the service that WRITES the family — and therefore imposes its retention.
	Owner string
	// Retention names the mechanism. Every family must declare one.
	Retention string
}

// ResidentFamilies is THE DECLARATION of retention per family.
//
// It is the DECLARATION, not the filter: SampleImmortalFamilies walks it instead of
// repeating a second literal, and sizing_sheet_test.go crosses the families the
// streamer PROJECTS against the ones it DECLARES in both directions.
func ResidentFamilies() []ResidentFamily {
	return []ResidentFamily{
		{
			Name:       "candles",
			PatternFor: func(scope string) string { return "candles:" + scope + ":*" },
			Owner:      ServiceGoWhale,
			Retention:  "ZSET cap per timeframe (candle_engine.redis_cap_by_timeframe), WITHOUT TTL — the absence is a declared decision",
		},
		{
			Name:       "derivatives:series",
			PatternFor: func(scope string) string { return "derivatives:series:" + scope + ":*" },
			Owner:      ServiceGoWhale,
			Retention:  "ZSET cap per timeframe (derivatives.series_cap_by_timeframe), raised to the baseline floor, WITHOUT TTL",
		},
	}
}

// SampleImmortalFamilies names the families whose sampled key carries no TTL. It
// samples ONE key per family: the question is "does this family carry a TTL at all",
// which is a property of the write path, not of an individual key.
func SampleImmortalFamilies(ctx context.Context, c Redis, scope string) []string {
	var immortal []string
	for _, family := range ResidentFamilies() {
		keys, err := redisstore.SampleFamilyKeys(ctx, c, family.PatternFor(scope), 1)
		if err != nil || len(keys) == 0 {
			continue
		}
		ttl, err := c.TTL(ctx, keys[0]).Result()
		if err != nil {
			continue
		}
		if IsImmortalTTL(ttl) {
			immortal = append(immortal, family.Name)
		}
	}
	sort.Strings(immortal)
	return immortal
}

// IsImmortalTTL separates "exists and never expires" (-1) from "does not exist" (-2).
// A key can expire between the SCAN that found it and the TTL that interrogates it,
// and `ttl < 0` would classify an ABSENT key as immortal.
func IsImmortalTTL(ttl time.Duration) bool {
	return ttl == -1*time.Nanosecond
}

// UniformKeysPerSymbol is m_F(tf)=1 — one key per symbol per timeframe.
func UniformKeysPerSymbol(timeframes []string) map[string]float64 {
	out := make(map[string]float64, len(timeframes))
	for _, tf := range timeframes {
		out[tf] = 1
	}
	return out
}

// DerivativesBudgetLines publishes the disk function and three instantiations of it.
// The FORMULA goes along: three numbers without it leave nobody able to instantiate a fourth.
func DerivativesBudgetLines(service string, durableTimeframes []string, window time.Duration) []string {
	if len(durableTimeframes) == 0 {
		return []string{fmt.Sprintf(
			"[SIZING][%s] derivatives (disk)  WITHOUT durable coverage — there is no cost to project", service)}
	}
	if window <= 0 {
		return []string{fmt.Sprintf(
			"[SIZING][%s] derivatives (disk)  WITHOUT declared retention and WITHOUT a deliberate boundary — "+
				"the table accumulates at the tail without a ceiling, and a budget here would be fiction", service)}
	}

	base := collector.NewDerivativesDiskBudget(1, durableTimeframes, window)

	perTF := make([]string, 0, len(base.Series))
	for _, s := range base.Series {
		perTF = append(perTF, fmt.Sprintf("%s=%d", s.Timeframe, s.Buckets))
	}

	out := []string{
		fmt.Sprintf("[SIZING][%s] derivatives (disk) FUNCTION of A: bytes(A) = A × Σ_tf(retained window ÷ bucket) × %.1f B/row",
			service, collector.DerivativesBytesPerRow),
		fmt.Sprintf("[SIZING][%s]   RETAINED window=%dd (%s) — the table accumulates at the TAIL up to it; the footprint of ONE seed is NOT the regime",
			service, int64(window/(24*time.Hour)), FormatHorizon(int64(window.Seconds()))),
		fmt.Sprintf("[SIZING][%s]   Σ_tf=%d buckets per asset across %d timeframes — %s",
			service, base.BucketsPerAsset(), len(base.Series), strings.Join(perTF, " ")),
		fmt.Sprintf("[SIZING][%s]   B/row MEASURED on the LAN 2026-08-25 (table_storage: diskSize/rowCount) — CEILING PER ROW, because the fixed partition cost is spread over a lower density; the composite below is NOT a ceiling, it is the steady regime",
			service),
	}
	// THE SEED APPEARS NEXT TO THE REGIME, LABELED.
	if seed := collector.DurableSeedPoints(); seed > 0 {
		out = append(out, fmt.Sprintf("[SIZING][%s]   seed=%d buckets per series on the FIRST pass "+
			"(provider response ceiling) — it is the footprint of ONE pass, NOT the regime: on the finest timeframe "+
			"the table reaches %.0f× that at the tail",
			service, seed, float64(base.DeepestSeries())/float64(seed)))
	}
	if len(base.Unparsed) > 0 {
		out = append(out, fmt.Sprintf("[SIZING][%s]   timeframe(s) covered and WITHOUT a derivable duration, left out of the count: %s",
			service, strings.Join(base.Unparsed, ", ")))
	}
	for _, assets := range collector.DerivativesBudgetAssetPoints() {
		b := collector.NewDerivativesDiskBudget(assets, durableTimeframes, window)
		out = append(out, fmt.Sprintf("[SIZING][%s]   A=%d → %d rows → %s",
			service, assets, b.Rows(), FormatBudgetSize(b.Bytes())))
	}
	out = append(out, fmt.Sprintf("[SIZING][%s]   THE WINDOW IS THE OWNER'S DECISION, not this function's: it inherits the deepest "+
		"timeframe's boundary and applies it to the finest — the same shape the lane analysis "+
		"REJECTED for `candles` (\"going back 2,395 days because of 1w means retaining 5m for 6.5 years\", 55.1 GB, does not fit). "+
		"Until it is reopened, the number above is what the volume must support", service))
	return out
}

// FormatBudgetSize renders the disk budget in the unit where it is legible.
func FormatBudgetSize(bytes int64) string {
	const mib = float64(1 << 20)
	switch {
	case bytes <= 0:
		return notAvailable
	case float64(bytes) < 1024*mib:
		return fmt.Sprintf("%.0f MiB", float64(bytes)/mib)
	default:
		return fmt.Sprintf("%.1f GiB", float64(bytes)/(1024*mib))
	}
}

// LogSheet prints a sheet line by line at Info.
func LogSheet(log *slog.Logger, sheet Sheet) {
	log = logger.OrDiscard(log)
	for _, line := range sheet.Lines() {
		log.Info(line)
	}
}
