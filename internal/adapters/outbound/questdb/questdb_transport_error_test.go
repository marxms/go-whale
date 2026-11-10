package questdb

import (
	"errors"
	"testing"
)

// TestIsConnectionError_RecognisesTheHTTPTransportFailure is the guard for THIRTEEN HOURS OF
// SILENT COLD-TIER DATA LOSS.
//
// # THE INCIDENT
//
// QuestDB exhausted its Java heap and its worker threads died while the process stayed up.
// Every ILP write then failed with the string below. GoWhale logged one line per row and
// dropped it — for thirteen hours — and kept dropping rows even AFTER QuestDB was restarted
// and answering /write with 204 in 68 ms, because nothing ever rebuilt the sender.
//
// # WHY THE CLASSIFIER IS THE ROOT CAUSE
//
// The reconnect machinery existed and was correct. It never ran, because isConnectionError
// only knew the phrasings of the TCP transport (broken pipe, connection reset) while the
// store was switched to ILP-over-HTTP, whose client reports the same class of failure as a
// retry/deadline expiry. A classifier that does not recognize a transport failure turns
// every recovery path behind it into dead code.
func TestIsConnectionError_RecognisesTheHTTPTransportFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			// Copied verbatim from the production log that ran for 13 hours.
			name: "the exact production failure",
			err:  errors.New(`retry timeout reached: 10s. Post "http://questdb:9000/write": context deadline exceeded`),
			want: true,
		},
		{
			name: "context deadline on its own",
			err:  errors.New(`Post "http://questdb:9000/write": context deadline exceeded`),
			want: true,
		},
		{
			name: "server closed the connection mid-request",
			err:  errors.New(`Post "http://questdb:9000/write": EOF`),
			want: true,
		},
		{
			name: "container replaced, DNS not yet resolving",
			err:  errors.New(`dial tcp: lookup questdb: no such host`),
			want: true,
		},
		{
			name: "server down",
			err:  errors.New(`dial tcp 192.0.2.10:9000: connect: connection refused`),
			want: true,
		},
		// The TCP-era phrasings must keep working — the classifier is widened, not replaced.
		{name: "broken pipe", err: errors.New("write: broken pipe"), want: true},
		{name: "connection reset", err: errors.New("read: connection reset by peer"), want: true},
		{name: "i/o timeout", err: errors.New("read tcp: i/o timeout"), want: true},

		// A per-ROW defect is not a transport failure. Treating it as one would abort and
		// retry the whole sweep forever on a row that can never be written.
		{
			name: "malformed row is NOT a transport failure",
			err:  errors.New("invalid column name: taker_buy_volume\x00"),
			want: false,
		},
		{
			name: "table schema rejection is NOT a transport failure",
			err:  errors.New("table 'candles' does not exist"),
			want: false,
		},
		{name: "nil", err: nil, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isConnectionError(tt.err); got != tt.want {
				t.Errorf("isConnectionError(%q) = %v, want %v — a misclassified transport failure "+
					"means the reconnect never runs and every subsequent row is dropped silently",
					tt.err, got, tt.want)
			}
		})
	}
}
