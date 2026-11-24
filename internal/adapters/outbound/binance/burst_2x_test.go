package binancesdk

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"
	"time"
)

// burstFixture é imutável: nasce do maior burst por símbolo medido na janela
// instrumentada, e o teste replaya exatamente 2× aquela taxa. O arquivo nomeia
// a fonte, o símbolo e a taxa; sem essa origem o número não é uma medição.
type burstFixture struct {
	Source                  string  `json:"source"`
	Exchange                string  `json:"exchange"`
	MarketType              string  `json:"marketType"`
	Symbol                  string  `json:"symbol"`
	Partition               int     `json:"partition"`
	MeasuredPeakBurstPerSec float64 `json:"measuredPeakBurstPerSec"`
	RatePerSec              float64 `json:"ratePerSec"`
	Multiplier              float64 `json:"multiplier"`
	BurstTrades             int     `json:"burstTrades"`
	ResolvedCap             int     `json:"resolvedCap"`
}

// TestBurst2xFromMeasuredPeakHasNoLocalDrops é o contrato de TRD-07 no nível
// em que a perda local acontece: o callback do adapter enche o buffer do
// símbolo. Duas vezes o maior burst medido precisa caber no cap resolvido e
// ser drenado na ordem de chegada, sem um único BUFFER_OVERFLOW.
//
// O mutante que este teste mata é um cap abaixo do burst: basta reduzir
// ResolvedCap para metade de BurstTrades numa cópia isolada do repositório para
// o callback classificar os trades excedentes como BUFFER_OVERFLOW e o teste
// reprovar. O cap de produção não é “generoso por acaso”: ele é o valor medido
// que o fixture registra e o teste exige.
func TestBurst2xFromMeasuredPeakHasNoLocalDrops(t *testing.T) {
	raw, err := os.ReadFile("testdata/burst-2x.json")
	if err != nil {
		t.Fatalf("fixture de burst ausente: %v", err)
	}
	var fixture burstFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("fixture de burst inválida: %v", err)
	}
	if fixture.Source == "" || fixture.Exchange == "" || fixture.Symbol == "" {
		t.Fatalf("fixture sem fonte/venue/símbolo: %+v", fixture)
	}
	if fixture.Multiplier != 2 {
		t.Fatalf("multiplier=%v, want exatamente 2", fixture.Multiplier)
	}
	if math.Abs(fixture.RatePerSec-2*fixture.MeasuredPeakBurstPerSec) > 1e-9 {
		t.Fatalf(
			"ratePerSec=%v não é 2× o pico medido %v",
			fixture.RatePerSec,
			fixture.MeasuredPeakBurstPerSec,
		)
	}
	if fixture.BurstTrades < 1 || fixture.ResolvedCap < fixture.BurstTrades {
		t.Fatalf(
			"burst de %d trades não cabe no cap resolvido %d: o pico medido não foi absorvido",
			fixture.BurstTrades,
			fixture.ResolvedCap,
		)
	}

	client := &SDKClient{buffers: NewBufferManager(fixture.ResolvedCap)}
	sink := &dropCapture{}
	client.metrics = sink.metrics()
	handler := client.tradeCallback(fixture.Symbol, fixture.Partition)

	base := time.Now().Add(-time.Minute).UnixMilli()
	for index := 0; index < fixture.BurstTrades; index++ {
		frame := fmt.Sprintf(
			`{"e":"aggTrade","s":%q,"a":%d,"p":"100.5","q":"1","T":%d,"m":false}`,
			fixture.Symbol,
			index+1,
			base+int64(index),
		)
		handler([]byte(frame))
	}

	if got := client.tradesDropped.Load(); got != 0 {
		t.Fatalf("tradesDropped=%d, want 0 local drops no burst de 2×", got)
	}
	if len(sink.classified) != 0 {
		t.Fatalf("drops classificados no burst: %+v", sink.classified)
	}
	if got := client.tradesIn.Load(); got != uint64(fixture.BurstTrades) {
		t.Fatalf("tradesIn=%d, want %d", got, fixture.BurstTrades)
	}

	buffer := client.buffers.GetOrCreate(fixture.Symbol)
	stats := buffer.stats()
	if stats.Current != fixture.BurstTrades || stats.Max != fixture.BurstTrades {
		t.Fatalf(
			"fill=%d max=%d, want ambos %d — o high-water mark prova que o burst inteiro entrou",
			stats.Current,
			stats.Max,
			fixture.BurstTrades,
		)
	}

	flushed := buffer.Flush()
	if len(flushed) != fixture.BurstTrades {
		t.Fatalf("flush=%d, want %d", len(flushed), fixture.BurstTrades)
	}
	var volume float64
	var previous int64
	for index, item := range flushed {
		id, err := strconv.ParseInt(item.ID, 10, 64)
		if err != nil {
			t.Fatalf("trade %d com id não numérico %q", index, item.ID)
		}
		if id <= previous {
			t.Fatalf("ordem quebrada no índice %d: id %d após %d", index, id, previous)
		}
		previous = id
		volume += item.Amount
	}
	if volume != float64(fixture.BurstTrades) {
		t.Fatalf(
			"volume drenado=%v, want %d — a ordem/volume não correspondem ao burst",
			volume,
			fixture.BurstTrades,
		)
	}
}
