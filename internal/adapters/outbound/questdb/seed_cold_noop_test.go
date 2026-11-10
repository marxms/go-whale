package questdb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/marxms/go-whale/internal/candle"
)

func coldNoopTestColumns() []string {
	return []string{
		"timestamp", "exchange", "market_type", "symbol", "timeframe",
		"open", "high", "low", "close", "volume", "taker_buy_volume", "taker_sell_volume", "legacy_extra",
	}
}

func coldNoopTestRow(key candle.SeriesKey, c candle.Candle) []any {
	return []any{
		time.UnixMilli(c.Timestamp), key.Scope.Exchange.String(), key.Scope.MarketType.String(),
		key.Scope.Symbol.String(), key.Timeframe.String(),
		c.Open, c.High, c.Low, c.Close, c.Volume, c.TakerBuyVolume, c.TakerSellVolume, nil,
	}
}

func coldNoopTestEntry(key candle.SeriesKey, c candle.Candle, candidate bool) candleEntry {
	return candleEntry{
		Exchange: key.Scope.Exchange.String(), MarketType: key.Scope.MarketType.String(),
		Symbol: key.Scope.Symbol.String(), Timeframe: key.Timeframe.String(), Candle: c, coldNoopCandidate: candidate,
	}
}

func coldNoopTestStore(read func(context.Context, candle.SeriesKey, time.Time, time.Time, int) ([]string, [][]any, error)) *Store {
	return &Store{
		walPosition: func(context.Context) (coldWALPosition, error) {
			return coldWALPosition{writer: 7, sequencer: 7}, nil
		},
		rawSeedRead: read,
	}
}

func TestRawColdMatchesRequiresPhysicalWriteEffect(t *testing.T) {
	key := mustSeriesKey("binance", "future", "BTC/USDT", "5m")
	c := candle.Candle{Timestamp: 1_760_000_000_000, Open: 1, High: 3, Low: 1, Close: 2, Volume: 5, TakerBuyVolume: 0, TakerSellVolume: 5}
	final := map[int64]candle.Candle{c.Timestamp: c}
	for _, tc := range []struct {
		name string
		edit func([]any) []any
		want bool
	}{
		{name: "exact including literal zero", want: true},
		{name: "NULL taker is not a measured zero", edit: func(row []any) []any { row[11] = nil; return row }},
		{name: "legacy extra column would be overwritten", edit: func(row []any) []any { row[12] = float64(9); return row }},
		{name: "financial difference", edit: func(row []any) []any { row[9] = float64(6); return row }},
		{name: "submillisecond physical timestamp", edit: func(row []any) []any { row[0] = time.UnixMicro(c.Timestamp*1000 + 1); return row }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := coldNoopTestRow(key, c)
			if tc.edit != nil {
				row = tc.edit(row)
			}
			matches, certain := rawColdMatches(coldNoopTestColumns(), [][]any{row}, final, key, 2)
			if !certain || matches[c.Timestamp] != tc.want {
				t.Fatalf("physical match=%v certain=%v, want %v", matches[c.Timestamp], certain, tc.want)
			}
		})
	}
	if _, certain := rawColdMatches(coldNoopTestColumns(), [][]any{coldNoopTestRow(key, c), coldNoopTestRow(key, c)}, final, key, 2); certain {
		t.Fatal("a result at LIMIT is truncated or ambiguous, not no-op proof")
	}
	if _, certain := rawColdMatches(coldNoopTestColumns()[:11], nil, final, key, 2); certain {
		t.Fatal("missing taker_sell column must not claim physical equality")
	}
}

func TestFilterNewColdBatch_RemovesWholeRevisionChainOnlyWhenFinalApplied(t *testing.T) {
	key := mustSeriesKey("binance", "future", "BTC/USDT", "5m")
	base := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC).UnixMilli()
	old := candle.Candle{Timestamp: base, Open: 1, High: 3, Low: 1, Close: 2, Volume: 4, TakerBuyVolume: 1, TakerSellVolume: 3}
	corrected := old
	corrected.Volume = 5
	corrected.TakerSellVolume = 4
	later := corrected
	later.Timestamp += int64(24 * time.Hour / time.Millisecond)
	entries := []candleEntry{
		coldNoopTestEntry(key, old, true),
		coldNoopTestEntry(key, corrected, true),
		coldNoopTestEntry(key, later, true),
	}
	reads := 0
	store := coldNoopTestStore(func(_ context.Context, gotKey candle.SeriesKey, from, to time.Time, limit int) ([]string, [][]any, error) {
		reads++
		if gotKey != key || from.UnixMilli() != old.Timestamp || to.UnixMilli() != later.Timestamp || limit != 4 {
			t.Fatalf("unbounded or wrong raw read: %+v %v %v limit=%d", gotKey, from, to, limit)
		}
		return coldNoopTestColumns(), [][]any{coldNoopTestRow(key, corrected)}, nil
	})
	filtered, release, target, skipped := store.filterNewColdBatch(context.Background(), entries)
	if release == nil {
		t.Fatal("comparison must retain DAY permit until first apply")
	}
	defer release()
	if reads != 1 || target != 7 || skipped != 2 || len(filtered) != 3 || !filtered[0].coldNoopSkip || !filtered[1].coldNoopSkip || filtered[2].coldNoopSkip {
		t.Fatalf("reads=%d target=%d marked=%d filtered=%+v", reads, target, skipped, filtered)
	}
	// Original admission tape is immutable: a failed first DAY can still be diagnosed.
	if len(entries) != 3 || entries[0].Candle.Volume != 4 || entries[1].Candle.Volume != 5 {
		t.Fatal("filter mutated original entries")
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if nextRelease, err := store.acquireCandleDayPermit(probeCtx); err == nil {
		nextRelease()
		t.Fatal("another candle writer entered before first DAY apply released the permit")
	}
}

func TestFilterNewColdBatch_FailureAndUnprobedGroupsKeepAllEntries(t *testing.T) {
	key := mustSeriesKey("binance", "future", "ETH/USDT", "1h")
	base := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC).UnixMilli()
	a := candle.Candle{Timestamp: base, Volume: 1}
	b := candle.Candle{Timestamp: base + int64(24*time.Hour/time.Millisecond), Volume: 2}
	entries := []candleEntry{coldNoopTestEntry(key, a, true), coldNoopTestEntry(key, b, true)}
	for _, tc := range []struct {
		name string
		read func(context.Context, candle.SeriesKey, time.Time, time.Time, int) ([]string, [][]any, error)
	}{
		{name: "read error", read: func(context.Context, candle.SeriesKey, time.Time, time.Time, int) ([]string, [][]any, error) {
			return nil, nil, errors.New("cold unavailable")
		}},
		{name: "unknown raw schema", read: func(context.Context, candle.SeriesKey, time.Time, time.Time, int) ([]string, [][]any, error) {
			return []string{"timestamp"}, [][]any{{time.UnixMilli(a.Timestamp)}}, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := coldNoopTestStore(tc.read)
			got, release, _, skipped := store.filterNewColdBatch(context.Background(), entries)
			if release != nil {
				release()
			}
			if skipped != 0 || len(got) != len(entries) || got[0].Candle.Timestamp != a.Timestamp || got[1].Candle.Timestamp != b.Timestamp {
				t.Fatalf("lost original batch on %s: skipped=%d got=%+v", tc.name, skipped, got)
			}
		})
	}
	reads := 0
	store := coldNoopTestStore(func(context.Context, candle.SeriesKey, time.Time, time.Time, int) ([]string, [][]any, error) {
		reads++
		return nil, nil, nil
	})
	store.pendingPlan = &coldDayPlan{days: []*coldDayWrite{{entries: entries}}}
	got, release, _, skipped := store.filterNewColdBatch(context.Background(), entries)
	if release != nil || skipped != 0 || len(got) != 2 || reads != 0 {
		t.Fatalf("predecessor was bypassed: reads=%d skipped=%d got=%+v", reads, skipped, got)
	}
	store.pendingPlan = nil
	got, release, _, skipped = store.filterNewColdBatch(context.Background(), []candleEntry{entries[0]})
	if release != nil || skipped != 0 || len(got) != 1 || reads != 0 {
		t.Fatal("single-bucket recovery issued a raw probe")
	}
}

func TestFilterNewColdBatch_BoundsReadsBySeriesKeyNotDay(t *testing.T) {
	base := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC).UnixMilli()
	entries := make([]candleEntry, 0, 40)
	for i := 0; i < 20; i++ {
		key := mustSeriesKey("binance", "future", fmt.Sprintf("S%d/USDT", i), "5m")
		entries = append(entries,
			coldNoopTestEntry(key, candle.Candle{Timestamp: base}, true),
			coldNoopTestEntry(key, candle.Candle{Timestamp: base + int64(2*24*time.Hour/time.Millisecond)}, true))
	}
	reads := 0
	store := coldNoopTestStore(func(context.Context, candle.SeriesKey, time.Time, time.Time, int) ([]string, [][]any, error) {
		reads++
		return coldNoopTestColumns(), nil, nil
	})
	got, release, _, skipped := store.filterNewColdBatch(context.Background(), entries)
	if release != nil {
		release()
	}
	if reads != maxColdNoopReadsPerBatch || skipped != 0 || len(got) != len(entries) {
		t.Fatalf("reads=%d skipped=%d rows=%d", reads, skipped, len(got))
	}
}

func TestFilterNewColdBatch_PartialReadErrorAndCancellationKeepOriginal(t *testing.T) {
	base := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC).UnixMilli()
	keys := []candle.SeriesKey{mustSeriesKey("binance", "future", "A/USDT", "5m"), mustSeriesKey("binance", "future", "B/USDT", "5m")}
	entries := make([]candleEntry, 0, 4)
	for _, key := range keys {
		entries = append(entries,
			coldNoopTestEntry(key, candle.Candle{Timestamp: base}, true),
			coldNoopTestEntry(key, candle.Candle{Timestamp: base + int64(24*time.Hour/time.Millisecond)}, true))
	}
	reads := 0
	store := coldNoopTestStore(func(_ context.Context, key candle.SeriesKey, _, _ time.Time, _ int) ([]string, [][]any, error) {
		reads++
		if reads == 2 {
			return nil, nil, errors.New("second series read failed")
		}
		return coldNoopTestColumns(), [][]any{coldNoopTestRow(key, entries[0].Candle)}, nil
	})
	got, release, _, skipped := store.filterNewColdBatch(context.Background(), entries)
	if release != nil || skipped != 0 || reads != 2 || len(got) != len(entries) {
		if release != nil {
			release()
		}
		t.Fatalf("partial proof escaped on error: reads=%d skipped=%d rows=%d", reads, skipped, len(got))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, release, _, skipped = store.filterNewColdBatch(ctx, entries)
	if release != nil || skipped != 0 || len(got) != len(entries) || reads != 2 {
		t.Fatal("canceled comparison consumed or omitted original entries")
	}
}

func TestFilterNewColdBatch_KeepsLaterDayPayloadForWALRevalidation(t *testing.T) {
	key := mustSeriesKey("binance", "future", "BTC/USDT", "5m")
	base := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC).UnixMilli()
	a := candle.Candle{Timestamp: base, Volume: 1}
	b := candle.Candle{Timestamp: base + int64(24*time.Hour/time.Millisecond), Volume: 2}
	entries := []candleEntry{coldNoopTestEntry(key, a, true), coldNoopTestEntry(key, b, true)}
	store := coldNoopTestStore(func(context.Context, candle.SeriesKey, time.Time, time.Time, int) ([]string, [][]any, error) {
		return coldNoopTestColumns(), [][]any{coldNoopTestRow(key, a), coldNoopTestRow(key, b)}, nil
	})
	got, release, _, skipped := store.filterNewColdBatch(context.Background(), entries)
	if release == nil {
		t.Fatal("first DAY proof lost its permit")
	}
	defer release()
	if skipped != 2 || len(got) != 2 || !got[0].coldNoopSkip || !got[1].coldNoopSkip || got[1].Candle.Timestamp != b.Timestamp {
		t.Fatalf("later DAY lost its reversible payload: marked=%d got=%+v", skipped, got)
	}
}

func TestColdNoopPipeline_RevalidatesLaterDayAfterMigration(t *testing.T) {
	for _, migrate := range []bool{false, true} {
		t.Run(fmt.Sprintf("migration=%t", migrate), func(t *testing.T) {
			key := mustSeriesKey("binance", "future", "BTC/USDT", "5m")
			base := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC).UnixMilli()
			oldA := candle.Candle{Timestamp: base, Open: 1, High: 2, Low: 1, Close: 2, Volume: 1}
			newA := oldA
			newA.Volume = 2
			b := candle.Candle{Timestamp: base + int64(24*time.Hour/time.Millisecond), Open: 3, High: 4, Low: 3, Close: 4, Volume: 3}
			migrationB := b
			migrationB.Volume = 9
			cold := map[int64]candle.Candle{oldA.Timestamp: oldA, b.Timestamp: b}
			var txn int64
			writes := 0
			q := coldNoopTestStore(func(_ context.Context, _ candle.SeriesKey, _, _ time.Time, _ int) ([]string, [][]any, error) {
				return coldNoopTestColumns(), [][]any{coldNoopTestRow(key, cold[oldA.Timestamp]), coldNoopTestRow(key, cold[b.Timestamp])}, nil
			})
			q.walPosition = func(context.Context) (coldWALPosition, error) {
				return coldWALPosition{writer: txn, sequencer: txn}, nil
			}
			q.writeBatch = func(_ context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
				writes++
				for _, c := range groups[key] {
					cold[c.Timestamp] = c
				}
				txn++
				return nil
			}
			p := NewPipeline(q, 16, time.Second)
			entries := []candleEntry{coldNoopTestEntry(key, newA, true), coldNoopTestEntry(key, b, true)}
			p.flushWorkerTurn(entries)
			if len(p.pendingDays) != 1 || cold[newA.Timestamp].Volume != newA.Volume || !p.pendingDays[0].entries[0].coldNoopSkip {
				t.Fatalf("first DAY did not preserve reversible B: pending=%+v cold=%+v", p.pendingDays, cold)
			}
			if migrate {
				migration := &coldDayPlan{days: []*coldDayWrite{{entries: []candleEntry{coldNoopTestEntry(key, migrationB, false)}}}}
				if _, err := q.applyColdPlan(context.Background(), migration); err != nil {
					t.Fatal(err)
				}
			}
			p.flushWorkerTurn(nil)
			if len(p.pendingDays) != 0 || cold[b.Timestamp].Volume != b.Volume {
				t.Fatalf("later DAY was lost after migration: pending=%+v coldB=%+v", p.pendingDays, cold[b.Timestamp])
			}
			wantWrites := 1
			if migrate {
				wantWrites = 3 // A, migration B=C, restored B
			}
			if writes != wantWrites || p.flushed.Load() != int64(1+boolInt(migrate)) {
				t.Fatalf("writes=%d flushed=%d want writes=%d", writes, p.flushed.Load(), wantWrites)
			}
		})
	}
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func TestColdNoopPipeline_PreSendFailureRevalidatesFirstDayRetry(t *testing.T) {
	key := mustSeriesKey("binance", "future", "ETH/USDT", "5m")
	base := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC).UnixMilli()
	a := candle.Candle{Timestamp: base, Volume: 2}
	b := candle.Candle{Timestamp: base + int64(5*time.Minute/time.Millisecond), Volume: 3}
	c := candle.Candle{Timestamp: base + int64(24*time.Hour/time.Millisecond), Volume: 4}
	oldA := a
	oldA.Volume = 1
	cold := map[int64]candle.Candle{a.Timestamp: oldA, b.Timestamp: b, c.Timestamp: c}
	var txn int64
	failFirst := true
	q := coldNoopTestStore(func(_ context.Context, _ candle.SeriesKey, _, _ time.Time, _ int) ([]string, [][]any, error) {
		return coldNoopTestColumns(), [][]any{coldNoopTestRow(key, cold[a.Timestamp]), coldNoopTestRow(key, cold[b.Timestamp]), coldNoopTestRow(key, cold[c.Timestamp])}, nil
	})
	q.walPosition = func(context.Context) (coldWALPosition, error) {
		return coldWALPosition{writer: txn, sequencer: txn}, nil
	}
	q.writeBatch = func(_ context.Context, groups map[candle.SeriesKey][]candle.Candle) error {
		if failFirst {
			failFirst = false
			return errors.New("unsent")
		}
		for _, row := range groups[key] {
			cold[row.Timestamp] = row
		}
		txn++
		return nil
	}
	p := NewPipeline(q, 16, time.Second)
	p.flushWorkerTurn([]candleEntry{coldNoopTestEntry(key, a, true), coldNoopTestEntry(key, b, true), coldNoopTestEntry(key, c, true)})
	if len(p.pendingDays) != 2 || p.pendingDays[0].acknowledged || !p.pendingDays[0].entries[1].coldNoopSkip {
		t.Fatalf("pre-send failure lost original DAY: %+v", p.pendingDays)
	}
	migrationB := b
	migrationB.Volume = 9
	migration := &coldDayPlan{days: []*coldDayWrite{{entries: []candleEntry{coldNoopTestEntry(key, migrationB, false)}}}}
	if _, err := q.applyColdPlan(context.Background(), migration); err != nil {
		t.Fatal(err)
	}
	p.flushWorkerTurn(nil)
	p.flushWorkerTurn(nil)
	if len(p.pendingDays) != 0 || cold[b.Timestamp].Volume != b.Volume || p.flushed.Load() != 3 {
		t.Fatalf("pre-send retry did not restore originals: pending=%+v coldB=%+v flushed=%d", p.pendingDays, cold[b.Timestamp], p.flushed.Load())
	}
}

func TestColdNoopPipeline_AcknowledgedDayNeverReplaysOnMetadataFailure(t *testing.T) {
	key := mustSeriesKey("binance", "future", "BTC/USDT", "5m")
	base := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC).UnixMilli()
	a := candle.Candle{Timestamp: base, Volume: 2}
	b := candle.Candle{Timestamp: base + int64(5*time.Minute/time.Millisecond), Volume: 3}
	c := candle.Candle{Timestamp: base + int64(24*time.Hour/time.Millisecond), Volume: 4}
	oldA := a
	oldA.Volume = 1
	q := coldNoopTestStore(func(context.Context, candle.SeriesKey, time.Time, time.Time, int) ([]string, [][]any, error) {
		return coldNoopTestColumns(), [][]any{coldNoopTestRow(key, oldA), coldNoopTestRow(key, b), coldNoopTestRow(key, c)}, nil
	})
	writes := 0
	q.writeBatch = func(context.Context, map[candle.SeriesKey][]candle.Candle) error {
		writes++
		return nil // transmission was ACKed; the metadata response is lost next
	}
	q.walPosition = func(context.Context) (coldWALPosition, error) {
		if writes > 0 {
			return coldWALPosition{}, errors.New("WAL metadata unavailable")
		}
		return coldWALPosition{writer: 0, sequencer: 0}, nil
	}
	p := NewPipeline(q, 16, time.Second)
	p.flushWorkerTurn([]candleEntry{coldNoopTestEntry(key, a, true), coldNoopTestEntry(key, b, true), coldNoopTestEntry(key, c, true)})
	if len(p.pendingDays) != 2 || !p.pendingDays[0].acknowledged || !p.pendingDays[0].entries[1].coldNoopSkip || writes != 1 {
		t.Fatalf("ACKed DAY not retained: pending=%+v writes=%d", p.pendingDays, writes)
	}
	p.flushWorkerTurn(nil)
	if writes != 1 || len(p.pendingDays) != 2 || !p.pendingDays[0].acknowledged {
		t.Fatalf("ACKed DAY replayed after metadata error: pending=%+v writes=%d", p.pendingDays, writes)
	}
}
