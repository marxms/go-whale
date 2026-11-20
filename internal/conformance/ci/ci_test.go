// Package cicontract is the single owner of what this repository asserts about
// `.github/workflows/`.
//
// # WHY A TEST-ONLY PACKAGE
//
// The gate this package guards is the repository's own CI. Asserting it from a second
// implementation (a shell grep, a lint rule) makes the two drift apart silently; the package has
// no production file, so the only thing it can become is the assertion itself.
//
// # WHY THE READ PATH MATTERS
//
// `../../../.github/workflows/` is INSIDE this module. `cmd/go` invalidates the test cache from
// the files the test binary opens under the module root, so a workflow edited after the last
// green run is re-read. The caller still runs `go test -count=1` (scripts/ci-checks.sh) because
// the gate claims a verdict about today's content, not about the previous execution.
package cicontract

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type step struct {
	Name            string `yaml:"name"`
	Run             string `yaml:"run"`
	Uses            string `yaml:"uses"`
	ContinueOnError any    `yaml:"continue-on-error"`
}

type service struct {
	Image string `yaml:"image"`
}

type job struct {
	Name     string             `yaml:"name"`
	Services map[string]service `yaml:"services"`
	Steps    []step             `yaml:"steps"`
}

type workflow struct {
	Name string         `yaml:"name"`
	Jobs map[string]job `yaml:"jobs"`
}

// workflows is the complete set this repository ships. Absence is refusal, never
// "nothing to check": a deleted workflow is the most complete disarmament there is.
var workflows = []string{"ci.yml", "integration.yml", "schema.yml", "security.yml"}

func load(t *testing.T, name string) workflow {
	t.Helper()

	path := filepath.Join("..", "..", "..", ".github", "workflows", name)
	raw, err := os.ReadFile(path) //nolint:gosec // G304: fixed workflow selected by this test.
	if err != nil {
		t.Fatalf("reading %s: %v — the workflow must exist and be tracked", path, err)
	}
	var parsed workflow
	if err := yaml.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	return parsed
}

func (w workflow) allSteps() []step {
	names := make([]string, 0, len(w.Jobs))
	for name := range w.Jobs {
		names = append(names, name)
	}
	var steps []step
	for _, name := range names {
		steps = append(steps, w.Jobs[name].Steps...)
	}
	return steps
}

func (w workflow) runs() []string {
	var out []string
	for _, s := range w.allSteps() {
		if s.Run != "" {
			out = append(out, s.Run)
		}
	}
	return out
}

func (w workflow) uses() []string {
	var out []string
	for _, s := range w.allSteps() {
		if s.Uses != "" {
			out = append(out, s.Uses)
		}
	}
	return out
}

func anyContains(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}

// TestCiContractPackageRuns proves the package is wired: every workflow parses, declares at
// least one job, one step, and one step that executes or uses something.
func TestCiContractPackageRuns(t *testing.T) {
	for _, name := range workflows {
		t.Run(name, func(t *testing.T) {
			parsed := load(t, name)
			if len(parsed.Jobs) == 0 {
				t.Fatalf("%s declares no jobs", name)
			}
			steps := parsed.allSteps()
			if len(steps) == 0 {
				t.Fatalf("%s declares no steps", name)
			}
			executables := 0
			for _, s := range steps {
				if s.Run != "" || s.Uses != "" {
					executables++
				}
			}
			if executables == 0 {
				t.Fatalf("%s declares no executable step", name)
			}
		})
	}
}

// TestCiRunsTheSuiteWithoutCache asserts the three properties that keep the test step from
// becoming a shape without effect: race, a fresh run (`-count=1`), and a shuffled order — plus the
// vet/build gates the pipeline claims.
func TestCiRunsTheSuiteWithoutCache(t *testing.T) {
	parsed := load(t, "ci.yml")
	runs := strings.Join(parsed.runs(), "\n")
	for _, want := range []string{"go test -race", "-count=1", "-shuffle=on"} {
		if !strings.Contains(runs, want) {
			t.Fatalf("ci.yml does not run %q: the suite is cached or the race detector was dropped", want)
		}
	}
	for _, want := range []string{"make vet", "make build"} {
		if !anyContains(parsed.runs(), want) {
			t.Fatalf("ci.yml no longer executes %q", want)
		}
	}
}

// TestIntegrationWorkflowRunsAgainstPinnedEngines asserts the live proof is against the
// production engine tags, with a cache-free run and the WAL case actually selected.
func TestIntegrationWorkflowRunsAgainstPinnedEngines(t *testing.T) {
	parsed := load(t, "integration.yml")
	found := map[string]bool{}
	for _, j := range parsed.Jobs {
		for name, svc := range j.Services {
			if !strings.Contains(svc.Image, "@sha256:") {
				t.Fatalf("service %q is not pinned by digest: %q", name, svc.Image)
			}
			found[name] = true
		}
	}
	for _, name := range []string{"redis", "questdb"} {
		if !found[name] {
			t.Fatalf("integration.yml no longer starts the %q service", name)
		}
	}
	if !anyContains(parsed.runs(), "-tags=integration") {
		t.Fatal("integration.yml no longer runs the integration-tagged suite")
	}
	if !anyContains(parsed.runs(), "TestWALBarrier_IsolatedQuestDB823") {
		t.Fatal("integration.yml no longer selects the live WAL case")
	}
}

// TestSchemaWorkflowProvesTheProductionEngine asserts the schema proof runs against QuestDB
// 8.2.3 and still selects the live schema case.
func TestSchemaWorkflowProvesTheProductionEngine(t *testing.T) {
	parsed := load(t, "schema.yml")
	pinned := false
	for _, j := range parsed.Jobs {
		for _, svc := range j.Services {
			if strings.Contains(svc.Image, "questdb/questdb:8.2.3") &&
				strings.Contains(svc.Image, "@sha256:") {
				pinned = true
			}
		}
	}
	if !pinned {
		t.Fatal("schema.yml does not pin the QuestDB 8.2.3 service by digest")
	}
	if !anyContains(parsed.runs(), `8\.2\.3`) {
		t.Fatal("schema.yml no longer asserts the live engine version")
	}
	if !anyContains(parsed.runs(), "TestEnsureColumnsLive") {
		t.Fatal("schema.yml no longer selects the live schema case")
	}
}

// TestSecurityWorkflowScansSecretsAndVulnerabilities asserts both security jobs still run.
func TestSecurityWorkflowScansSecretsAndVulnerabilities(t *testing.T) {
	parsed := load(t, "security.yml")
	if !anyContains(parsed.uses(), "gitleaks/gitleaks-action") {
		t.Fatal("security.yml no longer scans for secrets")
	}
	if !anyContains(parsed.runs(), "make vuln") {
		t.Fatal("security.yml no longer runs govulncheck")
	}
}

// TestGateStepsDoNotSwallowFailure asserts that no step declares tolerance. A
// `continue-on-error` in a gate is a refusal to gate, whatever its expression evaluates to.
func TestGateStepsDoNotSwallowFailure(t *testing.T) {
	for _, name := range workflows {
		for _, s := range load(t, name).allSteps() {
			if s.ContinueOnError != nil {
				t.Fatalf("%s step %q declares continue-on-error", name, s.Name)
			}
		}
	}
}
