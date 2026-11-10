package health

import (
	"math"
	"runtime/debug"
	"runtime/metrics"
)

// GCHeadroomTightAbove is the fraction of GOMEMLIMIT above which /health reports
// degraded.
//
// 0.85 sits above the projected healthy regime (live heap of ~1.5 GB over 2560MiB =
// 0.58) and below 1.0, where the spiral is ALREADY installed. It is an ANTECEDENT
// indicator: when the ratio approaches 1, the pacer can no longer satisfy the target
// and starts restarting collection as soon as it finishes — at that point the signal
// arrives too late to be useful.
const GCHeadroomTightAbove = 0.85

const (
	memTotalMetric    = "/memory/classes/total:bytes"
	memReleasedMetric = "/memory/classes/heap/released:bytes"
)

// gcHeadroomSample is the raw reading, isolated in a seam so the test can inject values.
type gcHeadroomSample struct {
	mappedInUse int64
	limit       int64
	configured  bool
}

// readGCHeadroomFromRuntime measures what the GC pacer actually limits.
//
// GOMEMLIMIT does not limit the live heap: it limits EVERYTHING the runtime maps as
// read-write minus what it has already returned to the OS. That is why the numerator is
// `total - released`, not HeapAlloc.
func readGCHeadroomFromRuntime() gcHeadroomSample {
	limit := debug.SetMemoryLimit(-1)
	if limit <= 0 || limit == math.MaxInt64 {
		return gcHeadroomSample{configured: false}
	}

	sample := []metrics.Sample{{Name: memTotalMetric}, {Name: memReleasedMetric}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 || sample[1].Value.Kind() != metrics.KindUint64 {
		return gcHeadroomSample{configured: false}
	}

	total := sample[0].Value.Uint64()
	released := sample[1].Value.Uint64()
	var inUse uint64
	if total > released {
		inUse = total - released
	}
	return gcHeadroomSample{mappedInUse: int64(inUse), limit: limit, configured: true}
}

// GCHeadroomRatio returns mapped memory in use over GOMEMLIMIT, and whether a limit is
// configured. It is the scrape-time source for go_gc_headroom_ratio.
func GCHeadroomRatio() (ratio float64, configured bool) {
	return gcHeadroomRatio(readGCHeadroomFromRuntime())
}

// gcHeadroomRatio derives the reported ratio from one reading. It is separate from the
// runtime probe so tests can inject a sample without swapping package state.
func gcHeadroomRatio(s gcHeadroomSample) (float64, bool) {
	if !s.configured || s.limit <= 0 {
		return 0, false
	}
	return float64(s.mappedInUse) / float64(s.limit), true
}
