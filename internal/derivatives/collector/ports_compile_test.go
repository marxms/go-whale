package collector

import (
	"context"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/platform/config"
)

// minimalPollerStore implements ONLY the write port plus the previous-snapshot read the poll
// loop uses. It deliberately does not implement the series, baseline or price reads: the
// poller never calls them.
type minimalPollerStore struct{}

func (minimalPollerStore) SaveSnapshot(context.Context, *derivatives.DerivativesSnapshot) error {
	return nil
}

func (minimalPollerStore) SaveSeriesOnly(context.Context, *derivatives.DerivativesSnapshot, string) error {
	return nil
}

func (minimalPollerStore) GetLatestSnapshot(context.Context, string, string, string) (*derivatives.DerivativesSnapshot, error) {
	return nil, nil
}

var _ pollerStore = minimalPollerStore{}

// minimalWriter is the history walk's whole dependency: one write method.
type minimalWriter struct{}

func (minimalWriter) SaveSnapshot(context.Context, *derivatives.DerivativesSnapshot) error {
	return nil
}

func (minimalWriter) SaveSeriesOnly(context.Context, *derivatives.DerivativesSnapshot, string) error {
	return nil
}

var _ derivatives.Writer = minimalWriter{}

func TestMinimalStoreSatisfiesThePoller(t *testing.T) {
	poller := NewDerivativesPoller(t.Context(), config.DerivativesConfig{}, minimalPollerStore{}, nil,
		candle.NewTimeframeRegistry(nil), nil, nil, nil)
	if poller == nil {
		t.Fatal("the poller rejected the minimal store")
	}
}
