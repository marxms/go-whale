package binancesdk

import "testing"

func TestWSEdgesFromEnv(t *testing.T) {
	t.Setenv("GOWHALE_TEST_WS_EDGES", " 13.1.1.1:443 , , 13.2.2.2:443 ")
	got := wsEdgesFromEnv("GOWHALE_TEST_WS_EDGES")
	if len(got) != 2 || got[0] != "13.1.1.1:443" || got[1] != "13.2.2.2:443" {
		t.Fatalf("edges = %v, want the two trimmed host:port entries", got)
	}
	if got := wsEdgesFromEnv("GOWHALE_TEST_WS_EDGES_UNSET"); got != nil {
		t.Fatalf("unset edges = %v, want nil", got)
	}
}
