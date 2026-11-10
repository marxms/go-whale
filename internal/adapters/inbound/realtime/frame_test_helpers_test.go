package realtime

import (
	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/distribution"
)

// outFrame and snapshotPayload keep the historical fixture vocabulary the wire goldens
// and benchmarks were written with. The production frame type now lives in
// distribution; these are test literals with a single conversion point.
type outFrame struct {
	env  Envelope
	snap *snapshotPayload
}

type snapshotPayload struct {
	scope        Scope
	baseline     uint64
	baselineTime int64
	open         *candle.Candle
}

func (f outFrame) frame() distribution.Frame {
	if f.snap != nil {
		return distribution.Frame{Snapshot: &distribution.Snapshot{
			Scope:        f.snap.scope,
			Baseline:     f.snap.baseline,
			BaselineTime: f.snap.baselineTime,
			Open:         f.snap.open,
		}}
	}
	return distribution.Frame{Envelope: f.env}
}
