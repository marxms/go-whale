package ingestion

import "testing"

// ABSENT AND ZERO ARE DIFFERENT ANSWERS, AND HERE THE DIFFERENCE IS THE WHOLE POINT.
//
// The admitted/applied pair reads "admitted > applied => the shortfall is OURS". Ownership is
// declared after subscription, so both counters accumulate during the boot window and then
// freeze unequal. The series is therefore DELETED when ownership is declared. This test pins
// the distinction: the series must stop being exported, not merely stop moving.
func TestForgetAdmittedVolumeRemovesTheSeriesEntirely(t *testing.T) {
	const ex, mt = "forgettest", "future"

	m := newTestMetrics()
	h := &StreamTradeHandler{metrics: m.port()}

	h.PreregisterAdmittedVolume(ex, mt)
	if !m.preregAdmitt[labels(ex, mt)] {
		t.Fatal("setup did not create the admitted-volume series")
	}
	m.port().AdmittedVolume(ex, mt, 1234.5)
	if got := m.admitted[labels(ex, mt)]; got != 1234.5 {
		t.Fatalf("admitted volume = %v, want 1234.5", got)
	}

	h.ForgetAdmittedVolume(ex, mt)

	if _, still := m.admitted[labels(ex, mt)]; still {
		t.Fatal("series still exported after ForgetAdmittedVolume — a frozen boot-window value " +
			"is still being read as a verdict")
	}
}
