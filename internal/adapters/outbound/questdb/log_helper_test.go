package questdb

import (
	"strings"
	"testing"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// linhaCom returns the record of the first captured line containing `substr`.
func linhaCom(t *testing.T, sink *logger.Sink, substr string) logger.Record {
	t.Helper()
	for _, r := range sink.Records() {
		if strings.Contains(r.Text(), substr) {
			return r
		}
	}
	t.Fatalf("no captured line contains %q; captured: %v", substr, sink.Records())
	return logger.Record{}
}
