package redisstore

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/marxms/go-whale/internal/candle"
)

// ============================================================================
// A SINGLE CAP AUTHORITY
// ============================================================================

// THE SNAPSHOT SWEEP UNDID THE CUT THE SEED APPLIED.
//
// `WriteSnapshotBatch` cut by `s.candleCap`, the global SCALAR, while `SeedCandlesBatch` and
// `ReplaceCandle` cut by `candleCapFor(timeframe)`. In production the two answer 800 and 521
// for the intraday timeframes — 279 members of divergence, reintroduced every five seconds.
//
// The assertion is about the RESULTING SIZE of the series, not about which function was
// called: a right call producing the wrong size would pass a spy and fails here.
func TestWriteSnapshotBatch_TrimsByTheSingleCapAuthority(t *testing.T) {
	casos := []struct {
		nome        string
		capPorTF    map[string]int
		herdado     int
		timeframe   string
		escritos    int
		querMembros int64
	}{
		{
			// The pair MEASURED in production: a dedicated entry of 521 against the inherited 800.
			nome:        "the dedicated entry rules over the inherited one",
			capPorTF:    map[string]int{"5m": 521},
			herdado:     800,
			timeframe:   "5m",
			escritos:    600,
			querMembros: 521,
		},
		{
			// Kills the "one different scalar" mutant: two cells, two caps, one batch.
			nome:        "each timeframe receives ITS cap, not a scalar",
			capPorTF:    map[string]int{"5m": 100, "1d": 300},
			herdado:     800,
			timeframe:   "1d",
			escritos:    400,
			querMembros: 300,
		},
		{
			// The cell WITHOUT an entry falls back to the inherited one — it is the `1d`/`1w`
			// case in production, and it must keep holding after the resolver swap.
			nome:        "without a dedicated entry, the inherited one rules",
			capPorTF:    map[string]int{"5m": 100},
			herdado:     250,
			timeframe:   "1d",
			escritos:    400,
			querMembros: 250,
		},
	}

	for _, caso := range casos {
		t.Run(caso.nome, func(t *testing.T) {
			store, client := newSeedCapStore(t, caso.capPorTF, caso.herdado)
			ctx := context.Background()

			if err := store.WriteSnapshotBatch(ctx,
				snapshotUpdates(caso.timeframe, caso.escritos)); err != nil {
				t.Fatalf("WriteSnapshotBatch: %v", err)
			}

			chave := candlesKeyV2("binance", "future", "BTC/USDT", caso.timeframe)
			membros, err := client.ZCard(ctx, chave).Result()
			if err != nil {
				t.Fatalf("ZCARD: %v", err)
			}
			if membros != caso.querMembros {
				t.Errorf("the sweep left %d members in %s, want %d — the effective cap diverged from what the other write paths apply",
					membros, caso.timeframe, caso.querMembros)
			}
		})
	}
}

// THE TWO CELLS IN THE SAME BATCH, and the batch is one pipeline. If the cut came from a scalar
// again, both would end equal — that equality is what the case forbids.
func TestWriteSnapshotBatch_TwoCellsInOneBatchKeepTheirOwnCaps(t *testing.T) {
	store, client := newSeedCapStore(t, map[string]int{"5m": 100, "1d": 300}, 800)
	ctx := context.Background()

	lote := append(snapshotUpdates("5m", 400), snapshotUpdates("1d", 400)...)
	if err := store.WriteSnapshotBatch(ctx, lote); err != nil {
		t.Fatalf("WriteSnapshotBatch: %v", err)
	}

	for timeframe, quer := range map[string]int64{"5m": 100, "1d": 300} {
		membros, err := client.ZCard(ctx, candlesKeyV2("binance", "future", "BTC/USDT", timeframe)).Result()
		if err != nil {
			t.Fatalf("ZCARD %s: %v", timeframe, err)
		}
		if membros != quer {
			t.Errorf("%s was left with %d members, want %d", timeframe, membros, quer)
		}
	}
}

// THE GLOBAL SCALAR IS ONLY REACHABLE THROUGH THE RESOLVER.
//
// The half of the requirement the behavior test does NOT cover is the prohibition of a SECOND
// resolver: a third write path reading `s.candleCap` directly would be born divergent and no
// size case would see it, because it would measure the path that is already right.
//
// The sweep is syntactic on purpose — `go/ast` over the package's production files, not text.
// It answers "which functions touch the field", and the declared answer is ONE.
func TestCandleCap_TheGlobalScalarHasExactlyOneReader(t *testing.T) {
	// `parser.ParseDir` is deprecated since Go 1.25 — it does not consider build tags when
	// associating file to package. Here the package association is used for nothing: the sweep
	// wants ALL production files in the directory, and `ParseFile` delivers them without the
	// assumption the deprecation names.
	fset := token.NewFileSet()
	entradas, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("os.ReadDir: %v", err)
	}
	var arquivos []*ast.File
	for _, e := range entradas {
		nome := e.Name()
		if e.IsDir() || !strings.HasSuffix(nome, ".go") || strings.HasSuffix(nome, "_test.go") {
			continue
		}
		arq, err := parser.ParseFile(fset, nome, nil, 0)
		if err != nil {
			t.Fatalf("parser.ParseFile(%s): %v", nome, err)
		}
		arquivos = append(arquivos, arq)
	}

	leitores := map[string]struct{}{}
	{
		for _, arquivo := range arquivos {
			for _, decl := range arquivo.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					sel, ok := n.(*ast.SelectorExpr)
					if ok && sel.Sel != nil && sel.Sel.Name == "candleCap" {
						leitores[fn.Name.Name] = struct{}{}
					}
					return true
				})
			}
		}
	}

	nomes := make([]string, 0, len(leitores))
	for nome := range leitores {
		nomes = append(nomes, nome)
	}
	sort.Strings(nomes)

	if len(nomes) != 1 || nomes[0] != "candleCapFor" {
		t.Fatalf("the global scalar is read by %v — the cap authority became more than one again; every write path must resolve through candleCapFor", nomes)
	}
}

// snapshotUpdates builds n consecutive closed buckets of a timeframe, from oldest to newest —
// the shape the snapshot sweep delivers.
func snapshotUpdates(timeframe string, n int) []CandleUpdate {
	const fimMs int64 = 1_760_000_000_000
	passoMs := candle.ParseTimeframeToSeconds(timeframe) * 1000

	out := make([]CandleUpdate, 0, n)
	for i := n; i >= 1; i-- {
		out = append(out, CandleUpdate{
			Exchange:   "binance",
			MarketType: "future",
			Symbol:     "BTC/USDT",
			Timeframe:  timeframe,
			Candle: candle.Candle{
				Timestamp: fimMs - int64(i)*passoMs,
				Open:      100, High: 110, Low: 90, Close: 105, Volume: 1,
			},
		})
	}
	return out
}
