package config

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// removedEnvNames returns the environment variables removed from the loader. They are assembled from
// fragments so this file does not itself contain the forbidden literals: the scanner below
// reads every Go source file in the module, including this one.
func removedEnvNames() []string {
	return []string{
		"GOWHALE_" + "BINANCE_COMBINED_URL_CONNECT",
		"GOWHALE_" + "LIVENESS_LEDGER",
		"GOWHALE_" + "GAPRECOVERY_BREAKER",
		"GOWHALE_" + "READ_TIER_RECONCILE",
		"GOWHALE_" + "HEALTH_PER_SYMBOL_LIVENESS",
		"GOWHALE_" + "AUTO_SUBSCRIBE_ENABLED",
		"GOWHALE_" + "AUTO_SUBSCRIBE_EXCHANGE",
		"GOWHALE_" + "AUTO_SUBSCRIBE_SYMBOLS",
		"GOWHALE_" + "ASSET_DISCOVERY_ENABLED",
		"GOWHALE_" + "STREAM_HEALTH_CHECK_INTERVAL",
		"GOWHALE_" + "MAX_STREAM_INACTIVITY_TIME",
		"GOWHALE_" + "STREAM_AUTH_DISABLED",
	}
}

func removedConfigFields() []string {
	return []string{
		"AutoSubscribeEnabled",
		"AutoSubscribeSymbols",
		"AssetDiscoveryEnabled",
		"AutoSubscribeExchange",
		"StreamHealthIntervalMS",
		"MaxStreamInactivityMS",
	}
}

// TestRemovedConfigFieldsStayRemoved fails if any removed field is declared again. The
// loader's dead surface was deleted; a field that no code reads must not come back
// silently on the struct either.
func TestRemovedConfigFieldsStayRemoved(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeOf(Config{}), reflect.TypeOf(StreamConfig{})} {
		for _, name := range removedConfigFields() {
			if _, ok := typ.FieldByName(name); ok {
				t.Errorf("%s declares removed configuration field %s", typ, name)
			}
		}
	}
}

// TestRemovedYAMLKeysAreRejected proves the keys left the strict YAML schema. Accepted and
// ignored keys are exactly the residual surface the loader removes: a config file that still
// declares one must fail loudly instead of loading a value nobody consumes.
func TestRemovedYAMLKeysAreRejected(t *testing.T) {
	keys := []string{
		"health_check_interval_ms",
		"max_inactivity_ms",
		"bingx_min_interval_ms",
	}
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			document := "server:\n  port: \"3000\"\nredis:\n  addr: \"localhost:6379\"\nstream:\n  " + key + ": 1\n"
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			_, err := loadYAML(path)
			if err == nil {
				t.Fatalf("loadYAML accepted removed stream.%s", key)
			}
			if !strings.Contains(err.Error(), key) {
				t.Fatalf("loadYAML refused without naming stream.%s: %v", key, err)
			}
		})
	}
}

// TestRemovedEnvVarsAreNotReadAgain walks the module's Go sources and fails on any
// occurrence of a removed variable name. The forbidden names cover both the flags that were
// never read and the env-only fields whose Config slots were deleted.
func TestRemovedEnvVarsAreNotReadAgain(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	removed := removedEnvNames()

	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".runtime", "bin":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		body, readErr := os.ReadFile(path) //nolint:gosec // G304: fixed module root, test-only scan.
		if readErr != nil {
			return readErr
		}
		for _, name := range removed {
			if bytes.Contains(body, []byte(name)) {
				t.Errorf("%s references removed environment variable %s", path, name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk module sources: %v", err)
	}
}

// TestRemovedSurfaceIsNotDocumented keeps the examples and the published tables honest:
// documenting a variable or a YAML key that the loader rejects is worse than omitting it.
func TestRemovedSurfaceIsNotDocumented(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	documents := []string{
		filepath.Join(root, "docs", "schemas", "env-config.md"),
		filepath.Join(root, "docs", "configuration.md"),
		filepath.Join(root, "config.example.yaml"),
		filepath.Join(root, ".env.example"),
		filepath.Join(root, "compose.example.yaml"),
		filepath.Join(root, "compose.integration.yaml"),
	}

	for _, path := range documents {
		body, err := os.ReadFile(path) //nolint:gosec // G304: fixed module-relative paths, test-only scan.
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(body)
		for _, name := range removedEnvNames() {
			if strings.Contains(text, name) {
				t.Errorf("%s documents removed environment variable %s", path, name)
			}
		}
		for _, key := range []string{"health_check_interval_ms", "max_inactivity_ms"} {
			if strings.Contains(text, key) {
				t.Errorf("%s documents removed YAML key stream.%s", path, key)
			}
		}
	}
}
