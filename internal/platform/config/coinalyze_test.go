package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// coinalyzeTestKey is a test value shaped like a credential. It is NOT a real key — and that is
// exactly why it works: the test asserts this value does not appear in the output, and a value
// that appeared here would be a leak of nothing.
const coinalyzeTestKey = "chave-de-teste-0000-1111-2222"

// TestApplyCoinalyzeEnv_ReadsTheVariableOnBothLoadPaths.
//
// There are TWO load paths — YAML when there is a file, environment when there is not. A field
// read on only one of them exists in half the deployments, and the half where it is missing is
// silent. `applyCoinalyzeEnv` runs AFTER the fork, so covering both is structural instead of
// repeated.
func TestApplyCoinalyzeEnv_ReadsTheVariableOnBothLoadPaths(t *testing.T) {
	// The test goes through Load(), not the helper: what is asserted is that BOTH paths deliver
	// the variable, and calling the helper directly would only prove that it works — not that
	// the loader applies it.
	yamlFile := filepath.Join(t.TempDir(), "config.yaml")
	const minimalYAML = `server:
  port: "3000"
redis:
  addr: "localhost:6379"
`
	if err := os.WriteFile(yamlFile, []byte(minimalYAML), 0o600); err != nil {
		t.Fatalf("write test YAML: %v", err)
	}

	tests := []struct {
		name       string
		configFile string
	}{
		{name: "YAML path", configFile: yamlFile},
		{name: "environment path", configFile: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GOWHALE_CONFIG_FILE", "")
			t.Setenv("GOWHALE_QUESTDB_PG_PASSWORD", "quest")
			t.Setenv("GOWHALE_COINALYZE_ENABLED", "true")
			t.Setenv(coinalyzeAPIKeyVar, coinalyzeTestKey)

			cfg, err := Load(tc.configFile)
			if err != nil {
				t.Fatalf("Load(%q): %v", tc.configFile, err)
			}
			if !cfg.Coinalyze.Enabled {
				t.Fatal("GOWHALE_COINALYZE_ENABLED=true did not turn collection on")
			}
			if got := string(cfg.Coinalyze.APIKey); got != coinalyzeTestKey {
				t.Fatalf("read key = %q, want the environment variable value", got)
			}
		})
	}
}

// TestApplyCoinalyzeEnv_TheKeyHasNoDefault: a credential default is a wrong credential that
// runs — it authenticates against nothing and turns the configuration error into a 401 every
// cycle.
func TestApplyCoinalyzeEnv_TheKeyHasNoDefault(t *testing.T) {
	t.Setenv("GOWHALE_COINALYZE_ENABLED", "")
	t.Setenv(coinalyzeAPIKeyVar, "")

	var cfg Config
	applyCoinalyzeEnv(&cfg)

	if cfg.Coinalyze.APIKey != "" {
		t.Fatalf("the missing key became %q", string(cfg.Coinalyze.APIKey))
	}
	if cfg.Coinalyze.Enabled {
		t.Fatal("collection started enabled without anyone asking")
	}
}

// TestValidate_MissingKeyWithCollectionEnabledDoesNotKillTheBoot.
//
// A missing credential for a SECOND derivatives source must not kill the boot — the existing
// source is perfectly capable of serving price, funding and open interest.
//
// Both layers must agree: wiring has a branch for exactly this case ("enabled without
// GOWHALE_COINALYZE_API_KEY — the existing source keeps serving"), and it is unreachable only if
// validation kills the process first. Two layers describing mutually exclusive behaviors, with
// no test forcing the choice, is the defect this test prevents.
func TestValidate_MissingKeyWithCollectionEnabledDoesNotKillTheBoot(t *testing.T) {
	cfg := validatableConfig()
	cfg.Coinalyze.Enabled = true
	cfg.Coinalyze.APIKey = ""

	if err := Validate(cfg); err != nil {
		t.Fatalf("the missing Coinalyze key killed the boot: %v", err)
	}
}

// THE COUNTERWEIGHT — the absence cannot become silence. Instead of killing the process, a boot
// WARN NAMES the variable; without this assertion, removing the guard would have traded a loud
// crash for a silent degradation: HTTP 401 every cycle, the series not growing, and nothing
// saying why.
//
// The assertion lives in `internal/app` (`TestCoinalyzeAdmission_*`), where the decision is made;
// here is the other side: a credential the process truly REQUIRES still kills the boot, so the
// removal above is not read as "credentials do not matter".
func TestValidate_RequiredCredentialStillKillsTheBoot(t *testing.T) {
	cfg := validatableConfig()
	cfg.QuestDB.Enabled = true
	cfg.QuestDB.PGPassword = nil

	err := Validate(cfg)
	if err == nil {
		t.Fatal("the boot accepted QuestDB without a password")
	}
	if !strings.Contains(err.Error(), "GOWHALE_QUESTDB_PG_PASSWORD") {
		t.Fatalf("the message does not name the variable: %q", err.Error())
	}
}

// TestValidate_CollectionDisabledNeedsNoKey: a deployment that does not collect from this source
// must not need a credential to start.
func TestValidate_CollectionDisabledNeedsNoKey(t *testing.T) {
	cfg := validatableConfig()
	cfg.Coinalyze.Enabled = false
	cfg.Coinalyze.APIKey = ""

	if err := Validate(cfg); err != nil {
		t.Fatalf("a deployment without Coinalyze collection was refused: %v", err)
	}
}

// TestSecret_TheKeyIsNeverPrinted is about the ACCIDENT.
//
// "Never log the key" lasts until someone adds a `log.Printf("cfg=%+v")` to debug something
// else. At that point the credential reaches the container's stdout and then the log aggregator,
// and the remedy stops being a commit and becomes a rotation. The type closes the path where it
// opens.
func TestSecret_TheKeyIsNeverPrinted(t *testing.T) {
	cfg := validatableConfig()
	cfg.Coinalyze.Enabled = true
	cfg.Coinalyze.APIKey = Secret(coinalyzeTestKey)

	renderings := map[string]string{
		"%v of the whole config":  fmt.Sprintf("%v", cfg),
		"%+v of the whole config": fmt.Sprintf("%+v", cfg),
		"%v of the block":         fmt.Sprintf("%v", cfg.Coinalyze),
		"%+v of the block":        fmt.Sprintf("%+v", cfg.Coinalyze),
		"%v of the field":         fmt.Sprintf("%v", cfg.Coinalyze.APIKey),
		// `%s` does not appear here as Sprintf because staticcheck rejects
		// `fmt.Sprintf("%%s", stringer)` — and it is right: `%%s` over a Stringer IS the
		// call below, guaranteed by `fmt` itself. The measured property is the same.
		"String() directly": cfg.Coinalyze.APIKey.String(),
	}

	for name, rendered := range renderings {
		if strings.Contains(rendered, coinalyzeTestKey) {
			t.Fatalf("%s printed the credential: %s", name, rendered)
		}
	}

	// And the redaction SAYS SOMETHING: a field that prints empty hides the difference between
	// "configured" and "forgotten", which is exactly what the operator needs to see.
	if got := fmt.Sprintf("%v", cfg.Coinalyze.APIKey); got != "(redigido)" {
		t.Fatalf("redaction of a present key = %q", got)
	}
	if got := fmt.Sprintf("%v", Secret("")); got != "(vazio)" {
		t.Fatalf("redaction of a missing key = %q", got)
	}
}

// TestValidate_TheRefusalCarriesNoKeyMaterial: the message the operator sees on a broken boot
// cannot itself be a leak.
func TestValidate_TheRefusalCarriesNoKeyMaterial(t *testing.T) {
	cfg := validatableConfig()
	cfg.Coinalyze.Enabled = true
	cfg.Coinalyze.APIKey = Secret(coinalyzeTestKey)
	// A present key passes; the refusal at issue is from another field, to prove no error path
	// in this package drags the credential along.
	cfg.Port = ""

	if err := Validate(cfg); err != nil && strings.Contains(err.Error(), coinalyzeTestKey) {
		t.Fatalf("an error message carried the credential: %q", err.Error())
	}
}

// validatableConfig is the minimal config that passes Validate, so each test above measures ONE
// rule instead of tripping over the others.
func validatableConfig() Config {
	password := "quest"
	return Config{
		Port:         "3000",
		RedisAddr:    "localhost:6379",
		QuestDB:      QuestDBConfig{PGPassword: &password},
		CandleEngine: CandleEngineConfig{Timeframes: []string{"5m"}},
	}
}
