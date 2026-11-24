package realtime

import "time"

// Metrics is the realtime edge's observability port. The zero value is a safe no-op.
type Metrics struct {
	PublishDropped          func(channel string)
	PublishQueueDepth       func(channel string, depth int)
	PublishOldestPendingAge func(channel string, age time.Duration)
}

func (m Metrics) publishDropped(channel string) {
	if m.PublishDropped != nil {
		m.PublishDropped(channel)
	}
}

func (m Metrics) publishQueueDepth(channel string, depth int) {
	if m.PublishQueueDepth != nil {
		m.PublishQueueDepth(channel, depth)
	}
}

func (m Metrics) publishOldestPendingAge(channel string, age time.Duration) {
	if m.PublishOldestPendingAge != nil {
		m.PublishOldestPendingAge(channel, age)
	}
}
