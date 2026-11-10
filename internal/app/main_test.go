package app

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain asserts that the whole package leaves no goroutine behind.
//
// The app package owns the lifecycle: it starts the HTTP and pprof servers, the hybrid
// manager, gap recovery, the kline watchdog and the realtime producer. A test that
// exercises any of them and does not let Shutdown stop them would otherwise pass while
// leaking exactly the goroutines this package exists to bound.
//
// There are no ignores. Every goroutine this package starts is its own — none belongs to
// a library — so ignoring one by top function would hide a lifecycle defect instead of a
// third-party detail. If a library goroutine ever has to be ignored, the ignore must name
// the library function and why it cannot be stopped from here.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
