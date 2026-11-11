package exchange

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestWithRetryWrapsAllAttemptsFailed(t *testing.T) {
	attempts := 0
	err := WithRetry(context.Background(), nil, RetryConfig{MaxAttempts: 1}, "venue", func() error {
		attempts++
		return errors.New("429 Too Many Requests")
	})

	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if !errors.Is(err, ErrAllAttemptsFailed) {
		t.Fatalf("error = %v, want errors.Is(ErrAllAttemptsFailed)", err)
	}
	if !strings.Contains(err.Error(), "venue") || !strings.Contains(err.Error(), "429") {
		t.Fatalf("error = %q, want the label and the cause", err)
	}
}
