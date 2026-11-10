package config

import "strings"

// ServerConfig defines HTTP server settings.
type ServerConfig struct {
	Port      string `yaml:"port" json:"port"`
	LogLevel  string `yaml:"log_level" json:"log_level"`
	LogFormat string `yaml:"log_format" json:"log_format"`
}

// RedisConfig defines Redis connection settings.
type RedisConfig struct {
	Addr          string `yaml:"addr" json:"addr"`
	Password      string `yaml:"password" json:"password"`
	DB            int    `yaml:"db" json:"db"`
	PoolSize      int    `yaml:"pool_size" json:"pool_size"`             // Max open connections (default: 256)
	MinIdleConns  int    `yaml:"min_idle_conns" json:"min_idle_conns"`   // Warm connections kept idle (default: 32)
	DialTimeoutS  int    `yaml:"dial_timeout_s" json:"dial_timeout_s"`   // Connection timeout (default: 5)
	ReadTimeoutS  int    `yaml:"read_timeout_s" json:"read_timeout_s"`   // Per-command read timeout (default: 3)
	WriteTimeoutS int    `yaml:"write_timeout_s" json:"write_timeout_s"` // Per-command write timeout (default: 3)
	PoolTimeoutS  int    `yaml:"pool_timeout_s" json:"pool_timeout_s"`   // Wait for pool slot (default: 4)
	MaxRetries    int    `yaml:"max_retries" json:"max_retries"`         // Client-level retries (default: 2)
}

// StreamConfig defines WebSocket streaming settings.
type StreamConfig struct {
	MaxSymbolsPerInstance int `yaml:"max_symbols_per_instance" json:"max_symbols_per_instance"`
	MaxConnectionsPerIP   int `yaml:"max_connections_per_ip" json:"max_connections_per_ip"`
	TradeBufferSize       int `yaml:"trade_buffer_size" json:"trade_buffer_size"` // Per-symbol trade buffer (default: 5000)
}

// ExchangeLimitsConfig defines per-exchange/market stream balancing limits.
type ExchangeLimitsConfig struct {
	MaxSymbolsPerInstance int `yaml:"max_symbols_per_instance" json:"max_symbols_per_instance"`
	MaxConnectionsPerIP   int `yaml:"max_connections_per_ip" json:"max_connections_per_ip"`
}

// DiscoveryConfig defines asset discovery settings.
// max_assets follows standard YAML inheritance: base config.yaml sets the
// production ceiling, and per-environment files (e.g. config.local.yaml) may
// lower it. This is intentional — local dev needs fewer assets than production.
type DiscoveryConfig struct {
	Enabled         bool   `yaml:"enabled" json:"enabled"`
	MaxAssets       int    `yaml:"max_assets" json:"max_assets"`
	RefreshInterval string `yaml:"refresh_interval" json:"refresh_interval"` // e.g. "6h", "30m" — periodic re-discovery (default: disabled)
}

// ExchangeConfig defines configuration for a single exchange stream.
type ExchangeConfig struct {
	ID         string               `yaml:"id" json:"id"`                               // binance, bingx, bybit, etc
	Enabled    bool                 `yaml:"enabled" json:"enabled"`                     // enable/disable this exchange
	MarketType string               `yaml:"market_type" json:"market_type"`             // spot, swap, future
	Symbols    []string             `yaml:"symbols,omitempty" json:"symbols,omitempty"` // explicit symbol list
	Discovery  DiscoveryConfig      `yaml:"discovery" json:"discovery"`                 // OR use discovery
	Limits     ExchangeLimitsConfig `yaml:"limits,omitempty" json:"limits,omitempty"`
}

// YAMLConfig represents the complete YAML configuration structure.
type YAMLConfig struct {
	Server       ServerConfig       `yaml:"server" json:"server"`
	Redis        RedisConfig        `yaml:"redis" json:"redis"`
	Stream       StreamConfig       `yaml:"stream" json:"stream"`
	Exchanges    []ExchangeConfig   `yaml:"exchanges" json:"exchanges"`
	Derivatives  DerivativesConfig  `yaml:"derivatives" json:"derivatives"`
	CandleEngine CandleEngineConfig `yaml:"candle_engine" json:"candle_engine"`
	QuestDB      QuestDBConfig      `yaml:"questdb" json:"questdb"`
	Retention    RetentionConfig    `yaml:"retention,omitempty" json:"retention,omitempty"`
}

// RetentionConfig holds the budget POLICY ratios — the thresholds at which the sizing sheet
// and the startup gates start complaining about occupancy.
//
// THEY ARE RATIOS, NOT DEPTHS. Nothing here says how much data to keep; they say when the
// declared plan stops being comfortable. They live in YAML rather than in code because a
// policy threshold written as a literal is a decision nobody can revise without a rebuild.
//
// A RATIO LEFT UNDECLARED IS ZERO, AND ZERO MEANS "NOT DECLARED" — never "always fine" and
// never a code default. The sheet prints the target as undeclared instead of inventing one,
// for the same reason ServerFacts.Unreadable exists: a verdict with no evidence is worse
// than no verdict.
type RetentionConfig struct {
	// BudgetWarnRatio is the projected/measured occupancy above which the sizing sheet flags
	// the plan as tight. Target: 0.60.
	BudgetWarnRatio float64 `yaml:"budget_warn_ratio,omitempty" json:"budget_warn_ratio,omitempty"`
	// BudgetErrorRatio is the occupancy above which the sheet reports an error-level line.
	// It is NEVER fatal: without hot reload, a fatal gate over an estimate turns "wrong
	// config" into "service that will not boot", with no way back.
	BudgetErrorRatio float64 `yaml:"budget_error_ratio,omitempty" json:"budget_error_ratio,omitempty"`
	// OccupancyAlertRatio is the MEASURED occupancy at which continuous observability alerts.
	// 0.70, and it applies to BOTH occupancies — maxmemory and the container ceiling, because
	// the second one is what kills the process.
	OccupancyAlertRatio float64 `yaml:"occupancy_alert_ratio,omitempty" json:"occupancy_alert_ratio,omitempty"`
	// MeasuredStartupWarnRatio is the MEASURED occupancy at startup above which the gate warns
	// loudly. It is separate from OccupancyAlertRatio because this one is read once, on a
	// number that is measured and not estimated, and it fires later (0.90).
	MeasuredStartupWarnRatio float64 `yaml:"measured_startup_warn_ratio,omitempty" json:"measured_startup_warn_ratio,omitempty"`
	// BudgetSampleIntervalSeconds is how often the continuous observer takes a sample. It is a
	// CADENCE, not a retention: it buys resolution on the eviction derivative at the cost of a
	// few INFO round trips. Zero means undeclared, and then the periodic observer does not run —
	// declared silence beats an invented cadence.
	BudgetSampleIntervalSeconds int `yaml:"budget_sample_interval_seconds,omitempty" json:"budget_sample_interval_seconds,omitempty"`
	// RedisContainerMemoryLimitMiB is the ceiling of the REDIS container — the second
	// occupancy, and the one that actually kills the process.
	//
	// WHY IT IS DECLARED HERE AND NOT MEASURED. The streamer and Redis live in DIFFERENT
	// containers (3 GiB and 4 GiB, measured in the compose files), so the streamer's own
	// cgroup answers a different question. The runtime source for another container's ceiling
	// is `HostConfig.Memory` on the Docker socket, and that socket is not mounted into this
	// service — mounting it would trade an observability signal for a privilege escalation.
	// The ceiling is DECLARED here, and a conformance test cross-reads the compose file and
	// fails on divergence. When the socket is available, liveness.DockerHostConfigMemory
	// reads the live value and this declaration is not used.
	RedisContainerMemoryLimitMiB int `yaml:"redis_container_memory_limit_mib,omitempty" json:"redis_container_memory_limit_mib,omitempty"`
}

// DerivativesConfig defines derivatives polling settings.
// No TTL — retention is managed by ZSET cap (MaxSeriesSize), identical to candle redisstore.
type DerivativesConfig struct {
	Enabled         bool `yaml:"enabled" json:"enabled"`
	PollIntervalSec int  `yaml:"poll_interval_sec" json:"poll_interval_sec"`
	// MaxSeriesSize is the cap applied to every timeframe that has no entry in
	// SeriesCapByTimeframe below.
	MaxSeriesSize int `yaml:"max_series_size" json:"max_series_size"`
	// SeriesCapByTimeframe declares the ZSET cap for individual timeframes, e.g.
	// {"1d": 400, "5m": 900}.
	//
	// WHY A SINGLE NUMBER WAS NOT ENOUGH. The cap IS the retention mechanism (there
	// is no TTL), so it decides how far back each series reaches — and a member of
	// the 1d series is 288× the wall time of a member of the 5m series. Under one
	// global number an operator who wanted a deeper daily horizon had to raise the
	// 5m cap by the same factor and pay for it in memory across the whole fleet;
	// and lowering the daily one was impossible, because the global value acted as
	// a floor under every timeframe at once.
	//
	// A value below the timeframe's own baseline window is raised to it — the ruler
	// must be able to mature, and that floor is not configurable away. See
	// redisstore.seriesCapFor, which resolves the effective cap and logs the resulting
	// horizon per timeframe at construction.
	SeriesCapByTimeframe map[string]int `yaml:"series_cap_by_timeframe,omitempty" json:"series_cap_by_timeframe,omitempty"`
	BootstrapLimit       int            `yaml:"bootstrap_limit" json:"bootstrap_limit"`
	BootstrapConcurrency int            `yaml:"bootstrap_concurrency" json:"bootstrap_concurrency"` // Max goroutines for parallel bootstrap (default: 10)
}

// DeclaredSeriesCap returns the cap the OPERATOR declared for one timeframe: the
// per-timeframe override when there is one, the global value otherwise.
//
// It is deliberately the configured number alone, with no floor applied. The floor is
// a property of the baseline window and belongs to whoever owns the ruler, so keeping
// the two apart is what lets the poller and the store ask the same question — "what
// did the operator ask for here" — and get the same answer.
func (c DerivativesConfig) DeclaredSeriesCap(timeframe string) int {
	// Both sides are normalized, and the map is walked rather than indexed: the KEYS
	// come from a hand-written YAML file, so `1D` and ` 1d` have to reach the same
	// timeframe as `1d`. The map holds one entry per timeframe at most — single
	// digits — so the walk is cheaper than keeping a second, derived copy in sync.
	want := normalizeTimeframeKey(timeframe)
	for declaredTF, declared := range c.SeriesCapByTimeframe {
		if declared > 0 && normalizeTimeframeKey(declaredTF) == want {
			return declared
		}
	}
	return c.MaxSeriesSize
}

// DerivedBootstrapLimit returns how deep the boot fetch has to go: the DEEPEST cap declared
// across the timeframes this deployment runs.
//
// WHY IT IS DERIVED AND NOT WRITTEN DOWN. Fetching more than the deepest retained series is
// pure waste — REST calls, CPU and boot time spent on points that the very next
// ZREMRANGEBYRANK throws away. Once the intraday timeframes were capped at their baseline
// floor, a hand-written 800 meant asking the exchange for 2,7× what three of the eight
// timeframes would keep, and nothing in the file said why. A number nobody can justify is a
// number nobody will revise, so it stops being a number and becomes a consequence.
//
// IT READS THE DECLARED CAPS, NOT THE EFFECTIVE ONES, AND THAT IS DELIBERATE. The baseline
// floor lives in redisstore.seriesCapFor, which owns the ruler; re-deriving it here would be a
// second implementation of the same law and the next expired citation. The floor can only ever
// RAISE a timeframe's retention, so the derived depth falls short only when EVERY timeframe is
// declared below its own floor — and that configuration is exactly what the startup gate
// `declared-cap-vs-baseline-floor` names, timeframe by timeframe.
func (c DerivativesConfig) DerivedBootstrapLimit(timeframes []string) int {
	deepest := 0
	for _, timeframe := range timeframes {
		if declared := c.DeclaredSeriesCap(timeframe); declared > deepest {
			deepest = declared
		}
	}
	if deepest <= 0 {
		return c.MaxSeriesSize
	}
	return deepest
}

// normalizeTimeframeKey makes the YAML key forgiving about case and padding, so
// `1D` in a config file is not silently a different timeframe from `1d`.
func normalizeTimeframeKey(timeframe string) string {
	return strings.ToLower(strings.TrimSpace(timeframe))
}

// CandleEngineConfig defines in-process candle aggregation settings.
type CandleEngineConfig struct {
	Enabled            bool           `yaml:"enabled" json:"enabled"`
	Timeframes         []string       `yaml:"timeframes" json:"timeframes"`
	BufferSizes        map[string]int `yaml:"buffer_sizes" json:"buffer_sizes"` // per-TF override or "default" key; also used as seed depth
	RedisWriteThrough  bool           `yaml:"redis_write_through" json:"redis_write_through"`
	SnapshotIntervalMS int            `yaml:"snapshot_interval_ms" json:"snapshot_interval_ms"`
	BingxMinIntervalMS int            `yaml:"bingx_min_interval_ms" json:"bingx_min_interval_ms"` // Milliseconds between BingX trade updates (default: 300)

	// RedisCapByTimeframe is the RESIDENT RETENTION of the candle ZSET, per timeframe. It is
	// NOT the chart depth, and that separation is the whole point of the key existing.
	//
	// WHY BufferSizes COULD NOT CARRY IT. `BufferSizes` sizes the in-process ring and the seed
	// depth: candle_engine.bufferSize and bootstrap both resolve BufferSizes[tf] before
	// BufferSizes["default"]. Reusing its `default` sub-key would make one key mean two
	// different things, told apart only by the sub-key name; an operator who wants a deeper
	// chart edits candle_engine.buffer_sizes, touches the line called `default` first, and the
	// resident candle family grows by hundreds of MiB in a gesture read as "make the chart
	// longer". A single scalar is also structurally unable to be per timeframe, which is the
	// only shape that can shrink 5m..4h without touching 1d/1w.
	//
	// The vocabulary mirrors derivatives.series_cap_by_timeframe, and it is a YAML key.
	RedisCapByTimeframe map[string]int `yaml:"redis_cap_by_timeframe,omitempty" json:"redis_cap_by_timeframe,omitempty"`
}

// RedisCapFor returns the DECLARED resident cap of the candle ZSET for one timeframe, or 0
// when nothing declares one — never an invented default. Zero means "undeclared", and the
// caller decides what to do about it, the same discipline RetentionConfig follows.
//
// The resolution order is the contract that libs/common's cross-service conformance test
// reads from the other side, so the two services answer the same question the same way:
//
//	redis_cap_by_timeframe[tf] → redis_cap_by_timeframe[default] → buffer_sizes[tf] → buffer_sizes[default]
//
// The fall-through to buffer_sizes is deliberate and is what leaves 1d and 1w alone: they get
// NO entry in the dedicated map, so they keep the depth they have today. Capping them would
// destroy the 129.190 members measured as sole copies — older than the durable tier's own
// coverage — to reclaim ~75 MiB.
func (c CandleEngineConfig) RedisCapFor(timeframe string) int {
	want := normalizeTimeframeKey(timeframe)

	// The map is walked rather than indexed for the same reason DeclaredSeriesCap walks its
	// own: the keys come from hand-written YAML, so `1D` and ` 1d` have to reach `1d`.
	for _, source := range []map[string]int{c.RedisCapByTimeframe, c.BufferSizes} {
		for declaredTF, declared := range source {
			if declared > 0 && normalizeTimeframeKey(declaredTF) == want {
				return declared
			}
		}
		if fallback, ok := source["default"]; ok && fallback > 0 {
			return fallback
		}
	}
	return 0
}

// DeclaredRedisCaps returns ONLY the caps written into redis_cap_by_timeframe, keyed by the
// normalized timeframe. It is a different question from RedisCapFor: that one answers "what cap
// applies", walking the fall-through; this one answers "which timeframes were DECIDED here".
//
// The startup gate needs the second question, and needs it against this key: pointed at
// buffer_sizes — where production declares only `default` — it warns about every registered
// timeframe on every boot and tells the operator to edit the key that sizes the chart.
func (c CandleEngineConfig) DeclaredRedisCaps() map[string]int {
	out := make(map[string]int, len(c.RedisCapByTimeframe))
	for declaredTF, declared := range c.RedisCapByTimeframe {
		if declared > 0 {
			out[normalizeTimeframeKey(declaredTF)] = declared
		}
	}
	return out
}

// QuestDBConfig defines QuestDB connection and storage settings.
type QuestDBConfig struct {
	Enabled       bool    `yaml:"enabled" json:"enabled"`
	ILPAddress    string  `yaml:"ilp_address" json:"ilp_address"`
	PGAddress     string  `yaml:"pg_address" json:"pg_address"`
	PGUser        string  `yaml:"pg_user" json:"pg_user"`
	PGPassword    *string `yaml:"pg_password" json:"pg_password,omitempty"`
	PGDatabase    string  `yaml:"pg_database" json:"pg_database"`
	PGPoolSize    int     `yaml:"pg_pool_size" json:"pg_pool_size"`
	RetentionDays int     `yaml:"retention_days" json:"retention_days"`
	// DerivativesRetentionDays is the retention window of the `derivatives` table, which
	// would otherwise have NO retention: NewRetentionManager receives `table`, and this
	// table is not it.
	//
	// It is SEPARATE from `retention_days` because the two tables hold data with different
	// cost and replenishment: candles are re-fetchable over REST, while the deep derivatives
	// series comes from a provider that returns at most ~2,000 points per response.
	//
	// ZERO means UNDECLARED, and the value is then DERIVED from the depth the seed reaches on
	// purpose (collector.DurableSeedHorizon) — never from a literal written here. The
	// derivation lives in internal/app because internal/platform/config cannot import
	// internal/derivatives/collector, and the effective value is logged at startup.
	DerivativesRetentionDays int    `yaml:"derivatives_retention_days" json:"derivatives_retention_days"`
	Table                    string `yaml:"table" json:"table"`
	PartitionBy              string `yaml:"partition_by" json:"partition_by"`
	WALEnabled               bool   `yaml:"wal_enabled" json:"wal_enabled"`
}

// Secret is a string that REFUSES TO PRINT ITSELF.
//
// A rule to "never log the key" lasts only until someone adds a `log.Printf("cfg=%+v", cfg)`
// to debug something else. At that point the key reaches the container's stdout and then the
// log aggregator, and it cannot be unpublished — the only remedy is rotating the credential.
//
// A String() that redacts closes that path where it opens: %v, %+v and %s all go through it,
// so the accident prints "(redigido)". Callers that NEED the value use the explicit
// `string(secret)` conversion, which is visible in review — the opposite of a field that
// leaks by default.
type Secret string

// String redacts. It is the method fmt calls.
func (s Secret) String() string {
	if s == "" {
		return "(vazio)"
	}
	return "(redigido)"
}

// CoinalyzeConfig configures collection from the second derivatives provenance.
//
// There is NO YAML TAG, deliberately: the key is a credential, and credentials do not belong
// in a versioned file. It arrives through the environment on both load paths — see
// applyCoinalyzeEnv — just as the WebSocket ticket secret does.
type CoinalyzeConfig struct {
	// Enabled turns collection on. The default is FALSE: a new collector that started by
	// itself would spend an external provider's budget without anyone asking.
	Enabled bool
	// APIKey is the credential. It travels in the `api_key` header and nowhere else.
	APIKey Secret
}
