package binancews

import (
	"testing"
	"time"
)

func TestReconnectBackoffJitterStableAfter(t *testing.T) {
	policy := Backoff{Base: time.Second, Max: 8 * time.Second, StableAfter: 30 * time.Second}

	if got := policy.Delay(0, 1); got != time.Second {
		t.Errorf("attempt 0 = %v, want 1s", got)
	}
	if got := policy.Delay(3, 1); got != 8*time.Second {
		t.Errorf("attempt 3 = %v, want 8s", got)
	}
	if got := policy.Delay(10, 1); got != 8*time.Second {
		t.Errorf("attempt 10 = %v, want the 8s cap", got)
	}
	if got := policy.Delay(3, 0.5); got != 4*time.Second {
		t.Errorf("attempt 3 at half jitter = %v, want 4s", got)
	}
	if got := policy.Delay(3, 0); got != 0 {
		t.Errorf("zero jitter = %v, want 0", got)
	}
	if policy.Stable(29 * time.Second) {
		t.Error("29s of health must not reset the failure count")
	}
	if !policy.Stable(30 * time.Second) {
		t.Error("30s of health must reset the failure count")
	}
}
