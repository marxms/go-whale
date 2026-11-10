package app

import (
	"github.com/marxms/go-whale/internal/adapters/outbound/binancerest"
	"github.com/marxms/go-whale/internal/adapters/outbound/venue"
	"github.com/marxms/go-whale/internal/health"
)

// buildDerivatives constructs the derivatives-side capabilities that do not depend on
// a store: the margin-bracket catalog, the discovery port and the poller health stamp.
func (a *App) buildDerivatives() {
	a.marginBrackets = binancerest.NewMarginBracketCatalog(binancerest.MarginBracketCatalogOptions{}, a.log)
	a.derivPollerHealth = new(health.PollerHealth)
	a.discovery = venue.Discovery{Log: a.log, Options: venue.Options{Budget: a.restBudget}}
}
