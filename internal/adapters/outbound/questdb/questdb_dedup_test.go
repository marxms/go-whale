package questdb

import (
	"errors"
	"strings"
	"testing"
)

// TestValidateDedupMeta locks the boot-time precondition for streaming-cold-tier-durability:
// the cold table MUST have WAL + DEDUP on exactly the upsert keys, or seed/backfill
// re-writes DUPLICATE instead of UPSERT. The pure validator is tested without a DB; the
// SQL fetch is covered by the integration test.
func TestValidateDedupMeta(t *testing.T) {
	allKeys := func() map[string]bool {
		m := map[string]bool{}
		for _, k := range requiredDedupKeys() {
			m[k] = true
		}
		return m
	}

	tests := []struct {
		name      string
		wal       bool
		dedup     bool
		keys      map[string]bool
		wantErr   bool
		errSubstr string
	}{
		{"all good", true, true, allKeys(), false, ""},
		{"WAL disabled", false, true, allKeys(), true, "WAL disabled"},
		{"DEDUP disabled", true, false, allKeys(), true, "DEDUP disabled"},
		{
			name: "missing one upsert key",
			wal:  true, dedup: true,
			keys:      map[string]bool{"timestamp": true, "exchange": true, "market_type": true, "symbol": true}, // no timeframe
			wantErr:   true,
			errSubstr: "missing required DEDUP upsert keys",
		},
		{"no keys at all", true, true, map[string]bool{}, true, "missing required DEDUP upsert keys"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// keysVerifiable=true: the upsert-key set was successfully read.
			err := validateDedupMeta("candles", tt.wal, tt.dedup, tt.keys, requiredDedupKeys(), true)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errSubstr != "" && !strings.Contains(err.Error(), tt.errSubstr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected nil, got %v", err)
			}
		})
	}
}

// TestValidateDedupMeta_KeysUnverifiable is the guard for unverifiable keys. When
// table_columns() is unreadable the upsert KEYS cannot be verified. Returning plain nil would
// be a FALSE "all keys present" that silently skips DEDUP key verification while reporting
// success. validateDedupMeta must instead return the distinct errDedupKeysUnverified sentinel
// so the caller can tell "verified OK" apart from "could not verify" (and log/alert
// accordingly), while STILL enforcing the wal/dedup flags it could read.
func TestValidateDedupMeta_KeysUnverifiable(t *testing.T) {
	// wal+dedup readable & ON, but keys unverifiable -> distinct sentinel, NOT nil.
	err := validateDedupMeta("candles", true, true, nil, nil, false)
	if err == nil {
		t.Fatal("keys unverifiable must NOT report a clean success (silent skip regression)")
	}
	if !errors.Is(err, errDedupKeysUnverified) {
		t.Fatalf("want errDedupKeysUnverified sentinel, got %v", err)
	}

	// wal/dedup OFF must still take precedence over the unverified-keys outcome — a
	// definitively-broken table is a hard failure regardless of key verifiability.
	if err := validateDedupMeta("candles", false, true, nil, nil, false); errors.Is(err, errDedupKeysUnverified) {
		t.Fatalf("wal off must report the WAL error, not the unverified-keys sentinel: %v", err)
	}
	if err := validateDedupMeta("candles", true, false, nil, nil, false); errors.Is(err, errDedupKeysUnverified) {
		t.Fatalf("dedup off must report the DEDUP error, not the unverified-keys sentinel: %v", err)
	}
}
