package questdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/marxms/go-whale/internal/candle"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

type coldWireFixture struct {
	rows      [][][]byte
	legacy    bool
	rowsError bool
}

// Only the transport is a fixture: QueryRange, pgx decoding and the cold fold are real.
// Fixed replies can exceed the requested limit to prove that capacity is not truncation.
func newColdWireStore(t *testing.T, fixture coldWireFixture) *Store {
	t.Helper()
	var listenCfg net.ListenConfig
	listener, err := listenCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	errCh := make(chan error, 4)
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				if !errors.Is(acceptErr, net.ErrClosed) {
					errCh <- acceptErr
				}
				return
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() {
					if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
						errCh <- err
					}
				}()
				if err := serveColdWire(conn, fixture); err != nil && !errors.Is(err, io.EOF) {
					errCh <- err
				}
			}()
		}
	}()
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close cold wire listener: %v", err)
		}
		workers.Wait()
		close(errCh)
		for err := range errCh {
			t.Errorf("cold wire fixture: %v", err)
		}
	})
	pc, err := pgxpool.ParseConfig("postgres://fixture@" + listener.Addr().String() + "/fixture?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pc.MaxConns = 1
	pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(context.Background(), pc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return &Store{pgPool: pool, tableName: "candles"}
}

func serveColdWire(conn net.Conn, fixture coldWireFixture) error {
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	backend := pgproto3.NewBackend(conn, conn)
	if _, err := backend.ReceiveStartupMessage(); err != nil {
		return err
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "14.0"})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return err
	}
	for {
		message, err := backend.Receive()
		if err != nil {
			return err
		}
		switch query := message.(type) {
		case *pgproto3.Terminate:
			return nil
		case *pgproto3.Query:
			if fixture.legacy && strings.Contains(query.String, "taker_sell_volume") {
				backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42703", Message: "column taker_sell_volume does not exist"})
			} else {
				fields := []pgproto3.FieldDescription{{Name: []byte("timestamp"), DataTypeOID: 1114, DataTypeSize: 8}}
				for _, name := range []string{"open", "high", "low", "close", "volume", "taker_buy_volume", "taker_sell_volume"} {
					fields = append(fields, pgproto3.FieldDescription{Name: []byte(name), DataTypeOID: 701, DataTypeSize: 8})
				}
				if fixture.legacy {
					fields = fields[:7]
				}
				backend.Send(&pgproto3.RowDescription{Fields: fields})
				for _, row := range fixture.rows {
					backend.Send(&pgproto3.DataRow{Values: row[:len(fields)]})
				}
				if fixture.rowsError {
					backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "fixture read failed"})
				} else {
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", len(fixture.rows)))})
				}
			}
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if err := backend.Flush(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected fixture protocol message %T", message)
		}
	}
}

func coldWireRows(count int) ([][][]byte, []candle.Candle) {
	base := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	rows := make([][][]byte, count)
	want := make([]candle.Candle, count)
	for i := range rows {
		ts := base.Add(-time.Duration(i) * 5 * time.Minute)
		rows[i] = [][]byte{[]byte(ts.Format("2006-01-02 15:04:05")), []byte("11"), []byte("13"), []byte("9"), []byte("12"), []byte("10"), []byte("4"), []byte("6")}
		want[i] = candle.Candle{Timestamp: ts.UnixMilli(), Open: 11, High: 13, Low: 9, Close: 12, Volume: 10, TakerBuyVolume: 4, TakerSellVolume: 6}
	}
	return rows, want
}

func coldWireQuery(t *testing.T, store *Store, limit int) []candle.Candle {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	to := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	got, err := store.QueryRange(ctx, "binance", "future", "BTC/USDT", "5m", to.Add(-7*24*time.Hour), to, limit)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestQueryRange_ColdCapacityHint(t *testing.T) {
	for _, tc := range []struct {
		limit, capacity int
	}{{3, 3}, {0, 1000}, {-1, 1000}, {1025, 1024}} {
		t.Run(fmt.Sprint(tc.limit), func(t *testing.T) {
			rows, want := coldWireRows(1)
			got := coldWireQuery(t, newColdWireStore(t, coldWireFixture{rows: rows}), tc.limit)
			if cap(got) != tc.capacity || !reflect.DeepEqual(got, want) {
				t.Fatalf("capacity/payload: cap=%d want=%d got=%+v", cap(got), tc.capacity, got)
			}
		})
	}
	if got := coldWireQuery(t, newColdWireStore(t, coldWireFixture{}), 800); got != nil {
		t.Fatalf("empty result must stay nil, got %#v", got)
	}
}

func TestQueryRange_ColdCapacityPreservesPayload(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			rows, want := coldWireRows(3)
			rows[0][6], rows[0][7] = nil, nil
			want[0].TakerBuyVolume, want[0].TakerSellVolume = 0, 0
			rows[1][7] = nil         // Legacy NULL sell derives only when buy is measured.
			rows[2][7] = []byte("0") // A present zero is measured, not NULL.
			if !legacy {
				want[2].TakerSellVolume = 0
			}
			got := coldWireQuery(t, newColdWireStore(t, coldWireFixture{rows: rows, legacy: legacy}), 1)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("hint truncated or altered financial/order/null payload: got %+v want %+v", got, want)
			}
		})
	}
	t.Run("above_initial_reserve", func(t *testing.T) {
		rows, want := coldWireRows(1025)
		got := coldWireQuery(t, newColdWireStore(t, coldWireFixture{rows: rows}), 1025)
		if len(got) != 1025 || !reflect.DeepEqual(got, want) {
			t.Fatalf("reserve must not cap returned rows or change payload: got %d rows, want 1025", len(got))
		}
	})
}

func TestQueryRange_ColdCapacityPropagatesErrors(t *testing.T) {
	for _, kind := range []string{"scan", "rows"} {
		t.Run(kind, func(t *testing.T) {
			rows, _ := coldWireRows(2)
			if kind == "scan" {
				rows[1][0] = []byte("invalid-timestamp")
			}
			store := newColdWireStore(t, coldWireFixture{rows: rows, rowsError: kind == "rows"})
			got, err := store.QueryRange(context.Background(), "binance", "future", "BTC/USDT", "5m", time.Time{}, time.Now(), 800)
			if got != nil || err == nil || !strings.Contains(err.Error(), "questdb "+kind+":") {
				t.Fatalf("must reject partial result with %s error: got=%+v err=%v", kind, got, err)
			}
		})
	}
}

func TestQueryRange_ColdCapacityAllocationSavings(t *testing.T) {
	rows, want := coldWireRows(800)
	store := newColdWireStore(t, coldWireFixture{rows: rows})
	for _, hint := range []int{1, 800} {
		if got := coldWireQuery(t, store, hint); !reflect.DeepEqual(got, want) {
			t.Fatalf("public cold payload differs for hint %d", hint)
		}
	}
	measure := func(limit int) uint64 {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := 0; i < 10; i++ {
			if got := coldWireQuery(t, store, limit); len(got) != 800 {
				t.Fatalf("cold rows %d, want 800", len(got))
			}
		}
		runtime.ReadMemStats(&after)
		return (after.TotalAlloc - before.TotalAlloc) / 10
	}
	narrow, bounded := measure(1), measure(800)
	residentArray := uint64(800 * unsafe.Sizeof(candle.Candle{}))
	t.Logf("public QueryRange: hint1=%dB hint800=%dB resident800=%dB", narrow, bounded, residentArray)
	if narrow < bounded || narrow-bounded < residentArray {
		t.Fatalf("capacity hint must save one resident array: hint1=%d hint800=%d required=%d", narrow, bounded, residentArray)
	}
}

func TestQueryRange_ColdCapacityEmptyDoesNotReserve(t *testing.T) {
	store := newColdWireStore(t, coldWireFixture{})
	for _, hint := range []int{100, 800} {
		if got := coldWireQuery(t, store, hint); got != nil {
			t.Fatalf("empty result for hint %d must be nil", hint)
		}
	}
	// Equal-width limits keep protocol work comparable. Pair both orders and use
	// the median to reduce process-wide noise, without raising the byte limit.
	measure := func(limit int) uint64 {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := 0; i < 256; i++ {
			if got := coldWireQuery(t, store, limit); got != nil {
				t.Fatalf("empty result for hint %d must be nil", limit)
			}
		}
		runtime.ReadMemStats(&after)
		return (after.TotalAlloc - before.TotalAlloc) / 256
	}
	var deltas [9]int64
	for i := range deltas {
		var narrow, wide uint64
		if i%2 == 0 {
			narrow, wide = measure(100), measure(800)
		} else {
			wide, narrow = measure(800), measure(100)
		}
		deltas[i] = int64(wide) - int64(narrow) //nolint:gosec // G115: allocation sizes measured by this test are far below MaxInt64.
	}
	slices.Sort(deltas[:])
	delta := deltas[len(deltas)/2]
	noise := int64(unsafe.Sizeof(candle.Candle{}))
	t.Logf("empty QueryRange: medianDelta=%dB pairedDeltas=%v noise=%dB", delta, deltas, noise)
	if delta > noise || delta < -noise {
		t.Fatalf("empty read must not reserve a discarded array: medianDelta=%d allowedNoise=%d", delta, noise)
	}
}
