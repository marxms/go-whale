package doctor

import (
	"bytes"
	"strings"
	"testing"
)

func TestBootVerdict_MeasuredSoleCopyStillEnforcesGate(t *testing.T) {
	for _, tc := range []struct {
		name, count string
		want        int
	}{
		{"known-zero", "0", exitOK},
		{"known-risk", "1", exitFatalVerdict},
		{"unknown", "-1", exitUncertifiable},
		{"invalid", "-2", exitUnreadable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, err bytes.Buffer
			args := []string{"-compose", composeDeclarando(t, "allkeys-lru", 512), "-config", configMinima(t), "-indicator-cap", "400", "-sole-copy-keys-at-risk", tc.count}
			if got := Run(args, &out, &err); got != tc.want {
				t.Fatalf("exit=%d want%d\n%s\n%s", got, tc.want, out.String(), err.String())
			}
			if tc.count == "0" && !strings.Contains(out.String(), "SoleCopyKeysAtRisk (flag):  0") {
				t.Fatal("measured zero was not reported")
			}
			if tc.count == "1" && !strings.Contains(err.String(), "sole-copy-vs-evict-everything-policy") {
				t.Fatal("positive risk did not name its fatal guard")
			}
		})
	}
}
