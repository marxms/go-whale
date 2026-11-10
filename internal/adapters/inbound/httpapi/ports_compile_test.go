package httpapi

import (
	"context"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
	"github.com/marxms/go-whale/internal/derivatives"
	"github.com/marxms/go-whale/internal/trade"
)

// The fakes below implement ONLY the methods each consumer port declares. If a route ever
// grows a dependency on a broader store, one of these assertions stops compiling — which is
// the point of narrowing the ports. No fake embeds another interface to pad its method set.

type minimalCandleReader struct{}

func (minimalCandleReader) GetCandlesDesc(context.Context, string, string, string, string, int) ([]candle.Candle, error) {
	return nil, nil
}

func (minimalCandleReader) GetCandlesRange(context.Context, string, string, string, string, time.Time, time.Time, int) ([]candle.Candle, error) {
	return nil, nil
}

var _ CandleReader = minimalCandleReader{}

type minimalCandleBatchReader struct{}

func (minimalCandleBatchReader) GetBatchCandlesDesc(context.Context, string, string, []candle.CandleReq) map[candle.CandleReq][]candle.Candle {
	return nil
}

var _ CandleBatchReader = minimalCandleBatchReader{}

type minimalSymbolCatalog struct{}

func (minimalSymbolCatalog) GetActiveSymbols(context.Context, string, string) ([]string, error) {
	return nil, nil
}

var _ SymbolCatalog = minimalSymbolCatalog{}

type minimalAllSymbolsCatalog struct{}

func (minimalAllSymbolsCatalog) GetAllActiveSymbols(context.Context) ([]candle.SymbolInfo, error) {
	return nil, nil
}

var _ AllSymbolsCatalog = minimalAllSymbolsCatalog{}

type minimalLastTradeReader struct{}

func (minimalLastTradeReader) GetLastTrade(context.Context, string, string, string) (*candle.LastTrade, error) {
	return nil, nil
}

var _ trade.LastTradeReader = minimalLastTradeReader{}

type minimalDerivativesReader struct{}

func (minimalDerivativesReader) GetLatestSnapshot(context.Context, string, string, string) (*derivatives.DerivativesSnapshot, error) {
	return nil, nil
}

func (minimalDerivativesReader) GetSnapshotSeries(context.Context, string, string, string, string, int) ([]*derivatives.DerivativesSnapshot, error) {
	return nil, nil
}

var _ derivatives.Reader = minimalDerivativesReader{}

type minimalBaselineReader struct{}

func (minimalBaselineReader) GetBaseline(context.Context, string, string, string, string) (*derivatives.DerivativesBaseline, error) {
	return nil, nil
}

var _ derivatives.BaselineReader = minimalBaselineReader{}

type minimalPriceReader struct{}

func (minimalPriceReader) PriceIncrement(context.Context, string, string, string, string, int64) (*float64, string, error) {
	return nil, "", nil
}

var _ derivatives.PriceReader = minimalPriceReader{}

// TestMinimalFakesSatisfyTheConsumerPorts is the compile-time proof made executable: the
// router and the market-data handler accept fakes that carry nothing beyond the port each
// route consumes.
func TestMinimalFakesSatisfyTheConsumerPorts(t *testing.T) {
	router := NewRouterWithMetrics(
		ReadPorts{
			Candles:      minimalCandleReader{},
			BatchCandles: minimalCandleBatchReader{},
			Symbols:      minimalSymbolCatalog{},
			AllSymbols:   minimalAllSymbolsCatalog{},
			LastTrades:   minimalLastTradeReader{},
		},
		"binance", "future", nil,
		minimalDerivativesReader{}, minimalBaselineReader{}, minimalPriceReader{},
		candle.NewTimeframeRegistry([]string{"5m"}), nil, nil, nil,
	)
	if router == nil {
		t.Fatal("the router rejected the minimal ports")
	}

	handler := NewMarketDataHandler(
		minimalDerivativesReader{}, minimalBaselineReader{}, minimalPriceReader{},
		"binance", "future", candle.NewTimeframeRegistry([]string{"5m"}),
	)
	if handler == nil {
		t.Fatal("the market-data handler rejected the minimal ports")
	}
}
