// Package config loads, overlays and validates the process configuration.
package config

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/marxms/go-whale/internal/market"
)

// Config is the resolved process configuration after YAML, environment and defaults.
type Config struct {
	Port                  string
	RedisAddr             string
	RedisPassword         string
	RedisDB               int
	RedisPoolSize         int
	RedisMinIdleConns     int
	RedisDialTimeoutS     int
	RedisReadTimeoutS     int
	RedisWriteTimeoutS    int
	RedisPoolTimeoutS     int
	RedisMaxRetries       int
	LogLevel              string
	LogFormat             string
	MaxSymbolsPerInstance int
	MaxConnectionsPerIP   int
	BingxMinIntervalMS    int
	TradeBufferSize       int
	// RealtimeChannelPrefix isolates the outbound realtime event bus (candle.close /
	// candle.revised) per environment/instance on the shared Redis — Pub/Sub ignores
	// the db number, so isolation is by channel-name prefix.
	RealtimeChannelPrefix string
	// Realtime chart WebSocket edge.
	RealtimeWSTicketSecret   string   // HMAC key for connection tickets; empty disables the WS edge
	RealtimeWSMintToken      string   // server-to-gateway secret required by the ticket-mint endpoint
	RealtimeWSAllowedOrigins []string // exact browser origins allowed (CSWSH defense)
	RealtimeWSTicketTTLS     int      // ticket lifetime, seconds
	RealtimeWSMaxConnAgeS    int      // bounded connection lifetime, seconds (0 = unbounded)
	RealtimeWSMaxConnections int      // global connection cap; 0 means the default
	RealtimeWSMaxScopes      int      // per-connection subscription cap; 0 means the default
	// LOCAL/INTRANET escape hatch (GOWHALE_REALTIME_WS_AUTH_DISABLED). Default FALSE = fail-closed
	// (ticket + origin required). When EXPLICITLY true (dev-only compose override), the WS edge
	// mounts without a secret, skips ticket validation, and accepts any Origin — network trust
	// replaces app auth on a trusted LAN. It is NEVER derived from an empty secret (that path
	// stays disabled/fail-closed), so a prod deploy that merely forgets the secret can never
	// become open.
	RealtimeWSAuthDisabled bool
	// Multi-exchange support
	Exchanges []ExchangeConfig
	// Binance endpoint policy (env > yaml > safe defaults)
	// Derivatives polling config
	Derivatives DerivativesConfig
	// Coinalyze is the second derivatives provenance. The credential is environment-only.
	Coinalyze CoinalyzeConfig
	// Candle engine config
	CandleEngine CandleEngineConfig
	// QuestDB config
	QuestDB QuestDBConfig
	// Retention/budget policy ratios. No code defaults: a ratio left out of the YAML stays
	// zero, and zero means "not declared".
	Retention RetentionConfig
}

// Load resolves one canonical configuration. An explicitly selected file is
// authoritative: a read, parse, or validation error is returned and never
// replaced by environment values. With no file, the environment-only path is
// used. The function performs no process exit and applies no legacy version
// mode.
func Load(path string) (Config, error) {
	if path == "" {
		path = os.Getenv("GOWHALE_CONFIG_FILE")
	}

	var cfg Config
	if path != "" {
		var err error
		cfg, err = loadYAML(path)
		if err != nil {
			return Config{}, fmt.Errorf("config: reading %s: %w", path, err)
		}
	} else {
		cfg = loadFromEnv()
	}

	applyRealtimeEnv(&cfg)
	applyCoinalyzeEnv(&cfg)
	applyQuestDBEnv(&cfg)
	applyDefaults(&cfg)
	if err := Validate(cfg); err != nil {
		return Config{}, fmt.Errorf("config: validation: %w", err)
	}
	return cfg, nil
}

// LoadFile reads one declared file without validating it. It is used by the
// doctor probe, which must report the declaration it read instead of exiting.
func LoadFile(path string) (Config, error) {
	cfg, err := loadYAML(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: reading %s: %w", path, err)
	}
	applyDefaults(&cfg)
	return cfg, nil
}

// loadFromEnv constructs a Config exclusively from environment variables. It is used
// when no configuration file is selected.
func loadFromEnv() Config {
	var pgPassword *string
	if password := os.Getenv("GOWHALE_QUESTDB_PG_PASSWORD"); password != "" {
		pgPassword = &password
	}
	return Config{
		Port:                  getEnv("GOWHALE_SERVER_PORT", "3000"),
		RedisAddr:             getRedisAddr("localhost", "6379"),
		RedisPassword:         getEnv("GOWHALE_REDIS_PASSWORD", ""),
		RedisDB:               getEnvInt("GOWHALE_REDIS_DB", 0),
		LogLevel:              getEnv("GOWHALE_LOG_LEVEL", "info"),
		LogFormat:             getEnv("GOWHALE_LOG_FORMAT", "text"),
		MaxSymbolsPerInstance: getEnvInt("GOWHALE_WEBSOCKET_MAX_ASSETS_PER_CONNECTION", 50),
		MaxConnectionsPerIP:   getEnvInt("GOWHALE_WEBSOCKET_MAX_CONNECTIONS_PER_IP", 60),
		QuestDB:               QuestDBConfig{PGPassword: pgPassword, PGUser: os.Getenv("GOWHALE_QUESTDB_PG_USER")},
		// Realtime edge config is applied centrally by applyRealtimeEnv.
		Exchanges: []ExchangeConfig{},
	}
}

func loadYAML(filename string) (Config, error) {
	data, err := os.ReadFile(filename) //nolint:gosec // G304: path comes from the operator (CLI, env or mounted config), not untrusted input.
	if err != nil {
		return Config{}, err
	}

	var yamlCfg YAMLConfig
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&yamlCfg); err != nil {
		return Config{}, err
	}

	cfg := Config{
		Port:                  yamlCfg.Server.Port,
		LogLevel:              yamlCfg.Server.LogLevel,
		LogFormat:             yamlCfg.Server.LogFormat,
		RedisAddr:             yamlCfg.Redis.Addr,
		RedisPassword:         yamlCfg.Redis.Password,
		RedisDB:               yamlCfg.Redis.DB,
		MaxSymbolsPerInstance: yamlCfg.Stream.MaxSymbolsPerInstance,
		MaxConnectionsPerIP:   yamlCfg.Stream.MaxConnectionsPerIP,
		TradeBufferSize:       yamlCfg.Stream.TradeBufferSize,
		Exchanges:             yamlCfg.Exchanges,
		Derivatives:           yamlCfg.Derivatives,
		CandleEngine:          yamlCfg.CandleEngine,
		Retention:             yamlCfg.Retention,
		QuestDB:               yamlCfg.QuestDB,
	}

	// BingxMinIntervalMS is sourced only from the candle_engine section.
	cfg.BingxMinIntervalMS = yamlCfg.CandleEngine.BingxMinIntervalMS

	// Override with environment variables if present
	if envPort := os.Getenv("GOWHALE_SERVER_PORT"); envPort != "" {
		cfg.Port = envPort
	}
	if envRedis := os.Getenv("GOWHALE_REDIS_ADDR"); envRedis != "" {
		cfg.RedisAddr = envRedis
	}
	if envPwd := os.Getenv("GOWHALE_REDIS_PASSWORD"); envPwd != "" {
		cfg.RedisPassword = envPwd
	}
	if envLogLevel := os.Getenv("GOWHALE_LOG_LEVEL"); envLogLevel != "" {
		cfg.LogLevel = envLogLevel
	}
	if envLogFormat := os.Getenv("GOWHALE_LOG_FORMAT"); envLogFormat != "" {
		cfg.LogFormat = envLogFormat
	}
	// REDIS_DB from shared .env.redis always wins over YAML
	if envDB := os.Getenv("GOWHALE_REDIS_DB"); envDB != "" {
		cfg.RedisDB = getEnvInt("GOWHALE_REDIS_DB", cfg.RedisDB)
	}

	// QuestDB credentials — env vars override YAML (never hardcode secrets)
	if envPass := os.Getenv("GOWHALE_QUESTDB_PG_PASSWORD"); envPass != "" {
		cfg.QuestDB.PGPassword = &envPass
	}
	if envUser := os.Getenv("GOWHALE_QUESTDB_PG_USER"); envUser != "" {
		cfg.QuestDB.PGUser = envUser
	}

	return cfg, nil
}

// applyRealtimeEnv populates the env-driven realtime edge config on BOTH the YAML and
// env-only load paths (the ticket secret and origins are never stored in YAML).
func applyRealtimeEnv(cfg *Config) {
	cfg.RealtimeChannelPrefix = getEnv("GOWHALE_REALTIME_CHANNEL_PREFIX", "gowhale:realtime")
	cfg.RealtimeWSTicketSecret = getEnv("GOWHALE_REALTIME_WS_TICKET_SECRET", "")
	cfg.RealtimeWSMintToken = getEnv("GOWHALE_REALTIME_WS_MINT_TOKEN", "")
	cfg.RealtimeWSAllowedOrigins = getEnvCSV("GOWHALE_REALTIME_WS_ALLOWED_ORIGINS")
	cfg.RealtimeWSTicketTTLS = getEnvInt("GOWHALE_REALTIME_WS_TICKET_TTL_S", 30)
	cfg.RealtimeWSMaxConnAgeS = getEnvInt("GOWHALE_REALTIME_WS_MAX_CONN_AGE_S", 3600)
	cfg.RealtimeWSMaxConnections = getEnvInt("GOWHALE_REALTIME_WS_MAX_CONNECTIONS", 4096)
	cfg.RealtimeWSMaxScopes = getEnvInt("GOWHALE_REALTIME_WS_MAX_SCOPES_PER_CONNECTION", 128)
	cfg.RealtimeWSAuthDisabled = getEnvBool("GOWHALE_REALTIME_WS_AUTH_DISABLED", false)
}

// applyQuestDBEnv lets container deployments point at services without copying a
// private YAML. Credentials remain env-only; addresses and the enabled switch are
// ordinary operational overrides.
func applyQuestDBEnv(cfg *Config) {
	if value := os.Getenv("GOWHALE_QUESTDB_ILP_ADDRESS"); value != "" {
		cfg.QuestDB.ILPAddress = value
	}
	if value := os.Getenv("GOWHALE_QUESTDB_PG_ADDRESS"); value != "" {
		cfg.QuestDB.PGAddress = value
	}
	if value := os.Getenv("GOWHALE_QUESTDB_ENABLED"); value != "" {
		cfg.QuestDB.Enabled = getEnvBool("GOWHALE_QUESTDB_ENABLED", cfg.QuestDB.Enabled)
	}
}

// applyCoinalyzeEnv populates the Coinalyze collection config on BOTH load paths.
//
// There are two load paths — YAML when a file exists, environment otherwise — and a field
// read on only one of them exists in half the deployments. This is the point after that
// fork, where applyRealtimeEnv lives for the same reason: the WebSocket ticket secret
// cannot live in YAML either.
//
// The key has NO default. A credential default is a wrong credential that runs.
func applyCoinalyzeEnv(cfg *Config) {
	cfg.Coinalyze.Enabled = getEnvBool("GOWHALE_COINALYZE_ENABLED", false)
	cfg.Coinalyze.APIKey = Secret(getEnv(coinalyzeAPIKeyVar, ""))
}

// coinalyzeAPIKeyVar is the NAME of the variable, declared once.
//
// It exists as a constant because the refusal message must NAME it: an error that says
// "missing credential" sends the operator looking for which one, and a broken boot is
// exactly when nobody has time to look.
const coinalyzeAPIKeyVar = "GOWHALE_COINALYZE_API_KEY" //nolint:gosec // G101: this is the environment-variable NAME, not its value.

func getRedisAddr(defaultHost, defaultPort string) string {
	if value := os.Getenv("GOWHALE_REDIS_ADDR"); value != "" {
		return value
	}
	host := getEnv("GOWHALE_REDIS_HOST", defaultHost)
	port := getEnv("GOWHALE_REDIS_PORT", defaultPort)
	return fmt.Sprintf("%s:%s", host, port)
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func getEnvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	var parsed int
	_, err := fmt.Sscanf(value, "%d", &parsed)
	if err != nil {
		return fallback
	}
	return parsed
}

func getEnvBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	switch strings.ToLower(value) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func getEnvCSV(key string) []string {
	value := os.Getenv(key)
	if value == "" {
		return []string{}
	}
	parts := strings.Split(value, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			items = append(items, trimmed)
		}
	}
	return items
}

// GetExchangeLimits resolves effective stream balancing limits for an exchange+market pair.
// Priority:
// 1) Per-exchange limits in YAML (exchanges[].limits)
// 2) Global stream defaults (stream.max_*).
func (c Config) GetExchangeLimits(exchangeID, marketType string) (int, int) {
	maxSymbols := c.MaxSymbolsPerInstance
	maxConnections := c.MaxConnectionsPerIP

	for _, ex := range c.Exchanges {
		if ex.ID != exchangeID || ex.MarketType != marketType {
			continue
		}

		if ex.Limits.MaxSymbolsPerInstance > 0 {
			maxSymbols = ex.Limits.MaxSymbolsPerInstance
		}
		if ex.Limits.MaxConnectionsPerIP > 0 {
			maxConnections = ex.Limits.MaxConnectionsPerIP
		}
		break
	}

	// applyDefaults ensures these are never zero when Config originates from Load().
	return maxSymbols, maxConnections
}

// applyDefaults sets production-ready defaults for zero-value fields. It is the single
// place where those fallbacks live.
func applyDefaults(cfg *Config) {
	if cfg.Port == "" {
		cfg.Port = "3000"
	}
	if cfg.RealtimeWSMaxConnections <= 0 {
		cfg.RealtimeWSMaxConnections = 4096
	}
	if cfg.RealtimeWSMaxScopes <= 0 {
		cfg.RealtimeWSMaxScopes = 128
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	cfg.LogFormat = strings.ToLower(strings.TrimSpace(cfg.LogFormat))
	if cfg.LogFormat == "" {
		cfg.LogFormat = "text"
	}
	if cfg.RedisAddr == "" {
		cfg.RedisAddr = "localhost:6379"
	}

	// Redis pool defaults (sized for 500-asset streaming)
	if cfg.RedisPoolSize == 0 {
		cfg.RedisPoolSize = 256
	}
	if cfg.RedisMinIdleConns == 0 {
		cfg.RedisMinIdleConns = 32
	}
	if cfg.RedisDialTimeoutS == 0 {
		cfg.RedisDialTimeoutS = 5
	}
	if cfg.RedisReadTimeoutS == 0 {
		cfg.RedisReadTimeoutS = 3
	}
	if cfg.RedisWriteTimeoutS == 0 {
		cfg.RedisWriteTimeoutS = 3
	}
	if cfg.RedisPoolTimeoutS == 0 {
		cfg.RedisPoolTimeoutS = 4
	}
	if cfg.RedisMaxRetries == 0 {
		cfg.RedisMaxRetries = 2
	}

	// Stream defaults
	if cfg.MaxSymbolsPerInstance == 0 {
		cfg.MaxSymbolsPerInstance = 50
	}
	if cfg.MaxConnectionsPerIP == 0 {
		cfg.MaxConnectionsPerIP = 60
	}
	if cfg.TradeBufferSize == 0 {
		cfg.TradeBufferSize = 5000
	}

	// Derivatives defaults — NO TTL, retention by ZSET cap only.
	if cfg.Derivatives.PollIntervalSec == 0 {
		cfg.Derivatives.PollIntervalSec = 300
	}
	if cfg.Derivatives.MaxSeriesSize == 0 {
		cfg.Derivatives.MaxSeriesSize = DefaultResidentDepth
	}
	if cfg.Derivatives.BootstrapConcurrency == 0 {
		cfg.Derivatives.BootstrapConcurrency = 10
	}

	// CandleEngine defaults — SINGLE SOURCE OF TRUTH for supported timeframes.
	if len(cfg.CandleEngine.Timeframes) == 0 {
		// OVERRIDABLE DEFAULT: candle_engine.timeframes in the mounted YAML replaces this
		// entire list, and that list is what runs in production. The literal exists because
		// "no timeframe at all" is not a state the service can operate in — and it exists once.
		cfg.CandleEngine.Timeframes = []string{"5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"}
	}
	if cfg.CandleEngine.BufferSizes == nil {
		cfg.CandleEngine.BufferSizes = map[string]int{"default": DefaultResidentDepth}
	}
	if cfg.CandleEngine.SnapshotIntervalMS == 0 {
		cfg.CandleEngine.SnapshotIntervalMS = 5000
	}
	if cfg.CandleEngine.BingxMinIntervalMS == 0 {
		cfg.CandleEngine.BingxMinIntervalMS = 300
	}

	// Derivatives bootstrap depth is a CONSEQUENCE of retention, not a parallel choice:
	// fetching deeper than the deepest retained series spends REST, CPU and boot time on
	// points the next ZREMRANGEBYRANK discards. It is resolved here, after the candle_engine
	// block, because the timeframe list is the domain over which the maximum is taken and it
	// receives its own default immediately above.
	if cfg.Derivatives.BootstrapLimit == 0 {
		cfg.Derivatives.BootstrapLimit = cfg.Derivatives.DerivedBootstrapLimit(cfg.CandleEngine.Timeframes)
	}

	// QuestDB defaults
	if cfg.QuestDB.Table == "" {
		cfg.QuestDB.Table = "candles"
	}
	if cfg.QuestDB.PartitionBy == "" {
		cfg.QuestDB.PartitionBy = "DAY"
	}
	if cfg.QuestDB.PGPoolSize == 0 {
		cfg.QuestDB.PGPoolSize = 5
	}
	if cfg.QuestDB.RetentionDays == 0 {
		cfg.QuestDB.RetentionDays = 90
	}

	// Sync top-level BingxMinIntervalMS from CandleEngine (kept for backward compat)
	if cfg.BingxMinIntervalMS == 0 {
		cfg.BingxMinIntervalMS = cfg.CandleEngine.BingxMinIntervalMS
	}
}

// Validate rejects configuration that cannot start safely. It never exits and
// never downgrades a failure to a warning; callers own the process exit code.
func Validate(cfg Config) error {
	if cfg.QuestDB.Enabled && (cfg.QuestDB.PGPassword == nil || *cfg.QuestDB.PGPassword == "") {
		return fmt.Errorf("QuestDB PG password must be provided via GOWHALE_QUESTDB_PG_PASSWORD env var")
	}
	if cfg.Port == "" {
		return fmt.Errorf("server port is required")
	}
	if cfg.RedisAddr == "" {
		return fmt.Errorf("redis address is required")
	}
	if cfg.LogFormat != "" && cfg.LogFormat != "text" && cfg.LogFormat != "json" {
		return fmt.Errorf("server.log_format must be text or json, got %q", cfg.LogFormat)
	}
	if !cfg.RealtimeWSAuthDisabled {
		ticketSet := cfg.RealtimeWSTicketSecret != ""
		mintSet := cfg.RealtimeWSMintToken != ""
		if ticketSet || mintSet {
			if !ticketSet || !mintSet {
				return fmt.Errorf("the WebSocket edge requires BOTH GOWHALE_REALTIME_WS_TICKET_SECRET and GOWHALE_REALTIME_WS_MINT_TOKEN; set GOWHALE_REALTIME_WS_AUTH_DISABLED=true only for local use")
			}
			if err := validateWSSecret("GOWHALE_REALTIME_WS_TICKET_SECRET", cfg.RealtimeWSTicketSecret); err != nil {
				return err
			}
			if err := validateWSSecret("GOWHALE_REALTIME_WS_MINT_TOKEN", cfg.RealtimeWSMintToken); err != nil {
				return err
			}
		}
	}
	if cfg.MaxSymbolsPerInstance < 0 {
		return fmt.Errorf("max_symbols_per_instance must be >= 0")
	}
	if cfg.MaxConnectionsPerIP < 0 {
		return fmt.Errorf("max_connections_per_ip must be >= 0")
	}
	if len(cfg.CandleEngine.Timeframes) == 0 {
		return fmt.Errorf("candle_engine.timeframes must contain at least one interval")
	}
	for _, tf := range cfg.CandleEngine.Timeframes {
		if _, err := market.ParseTimeframe(tf); err != nil {
			return fmt.Errorf("candle_engine.timeframes: %w", err)
		}
	}

	seen := make(map[string]bool)
	for _, ex := range cfg.Exchanges {
		exchange, err := market.ParseExchange(ex.ID)
		if err != nil {
			return fmt.Errorf("exchanges[%s].id: %w", ex.ID, err)
		}
		marketType, err := market.ParseMarketType(ex.MarketType)
		if err != nil {
			return fmt.Errorf("exchanges[%s].market_type: %w", ex.ID, err)
		}
		for i, symbol := range ex.Symbols {
			if _, err := market.ParseSymbol(symbol); err != nil {
				return fmt.Errorf("exchanges[%s].symbols[%d]: %w", ex.ID, i, err)
			}
		}
		key := exchange.String() + ":" + marketType.String()
		if seen[key] {
			return fmt.Errorf("duplicate exchange configuration: %s", key)
		}
		seen[key] = true
	}

	// Advisory trade buffer memory guard — never fatal.
	// Candle struct ≈ 136 bytes (8 float64 + 4 int64 + 1 bool + padding).
	// 10_000_000 slots ≈ 1.36 GB. Warn at ~370 MB (2_700_000 slots).
	tradeBufferUsage := cfg.TradeBufferSize * cfg.MaxSymbolsPerInstance
	if tradeBufferUsage > 2_700_000 {
		memMB := tradeBufferUsage * 136 / (1024 * 1024)
		log.Printf("WARN: high trade buffer memory usage estimated: %d slots (~%d MB) (trade_buffer_size=%d * max_symbols=%d)", tradeBufferUsage, memMB, cfg.TradeBufferSize, cfg.MaxSymbolsPerInstance)
	}

	return nil
}

// validateWSSecret rejects short and obviously copied secrets. The edge is
// server-to-gateway for the mint token and HMAC for the ticket; both need real
// entropy, and a placeholder published in the repository is not a secret.
func validateWSSecret(name, value string) error {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) < 32 {
		return fmt.Errorf("%s must be at least 32 bytes", name)
	}
	lower := strings.ToLower(trimmed)
	for _, prefix := range []string{"replace-with-", "change-me", "changeme", "noauth", "example"} {
		if strings.HasPrefix(lower, prefix) {
			return fmt.Errorf("%s must not be a placeholder", name)
		}
	}
	return nil
}
