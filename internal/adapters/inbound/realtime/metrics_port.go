package realtime

// Metrics is the realtime edge's observability port. The zero value is a safe no-op.
type Metrics struct {
	PublishDropped func(channel string)
}

func (m Metrics) publishDropped(channel string) {
	if m.PublishDropped != nil {
		m.PublishDropped(channel)
	}
}
