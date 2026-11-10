package coinalyze

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// The second-provenance client.
//
// The cases below are written against what was MEASURED on the real API, not against the
// documentation: a 401 returns `{"message":"Invalid/Missing API key"}`, and a symbol that
// does not exist returns HTTP 200 with `[]` — the API does not flag a wrong symbol, which
// makes the distinction "unexpected body != absence of data" a necessity rather than
// pedantry.

func coinalyzeFake(t *testing.T, handler http.HandlerFunc) (*CoinalyzeClient, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return NewCoinalyzeClient("chave-de-teste", WithCoinalyzeBaseURL(server.URL)), server
}

// THE KEY TRAVELS IN THE `api_key` HEADER, and that is how the real API authenticates.
func TestCoinalyzeSendsTheKeyInTheApiKeyHeader(t *testing.T) {
	var seen string
	client, _ := coinalyzeFake(t, func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("api_key")
		_, _ = w.Write([]byte(`[]`))
	})

	var out []any
	if err := client.get(context.Background(), "/exchanges", nil, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen != "chave-de-teste" {
		t.Fatalf("the key did not arrive in the header: %q", seen)
	}
}

// AND NOWHERE ELSE. A key in the URL enters proxy logs and shell history, and nobody takes
// it out afterwards.
func TestCoinalyzeNeverPutsTheKeyInTheURL(t *testing.T) {
	var seenURL string
	client, _ := coinalyzeFake(t, func(w http.ResponseWriter, r *http.Request) {
		seenURL = r.URL.String()
		_, _ = w.Write([]byte(`[]`))
	})

	var out []any
	_ = client.get(context.Background(), "/exchanges", nil, &out)

	if strings.Contains(seenURL, "chave-de-teste") {
		t.Fatalf("the key leaked in the URL: %s", seenURL)
	}
}

// 400 AND 401 ABORT. A malformed window remains malformed on the second attempt, and a
// refused key remains refused until a human replaces it — repeating only burns budget.
func TestCoinalyzeClientErrorsAreNotRetryable(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized} {
		client, _ := coinalyzeFake(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"message":"Invalid/Missing API key"}`))
		})

		var out []any
		err := client.get(context.Background(), "/exchanges", nil, &out)
		if err == nil {
			t.Fatalf("http %d should fail", status)
		}
		if CoinalyzeIsRetryable(err) {
			t.Fatalf("http %d must not be retryable", status)
		}
		var coinErr *CoinalyzeError
		if !errors.As(err, &coinErr) || coinErr.Status != status {
			t.Fatalf("the status must survive the error, got %v", err)
		}
		// And the provider's message reaches the log.
		if !strings.Contains(err.Error(), "Invalid/Missing API key") {
			t.Fatalf("the provider's message vanished: %v", err)
		}
	}
}

// 429 AND 5xx ARE PROVIDER STATE, and state passes.
func TestCoinalyzeProviderErrorsAreRetryable(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable} {
		client, _ := coinalyzeFake(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		})

		var out []any
		err := client.get(context.Background(), "/exchanges", nil, &out)
		if !CoinalyzeIsRetryable(err) {
			t.Fatalf("http %d should be retryable: %v", status, err)
		}
	}
}

// CLASSIFICATION IS BY STATUS, NEVER BY PROSE: a provider that rewrites the message must not
// turn a permanent 401 into a retry loop.
func TestCoinalyzeClassifiesByStatusNotByProse(t *testing.T) {
	client, _ := coinalyzeFake(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		// Prose a text classifier would read as transient.
		_, _ = w.Write([]byte(`{"message":"rate limit exceeded, please retry later"}`))
	})

	var out []any
	if err := client.get(context.Background(), "/exchanges", nil, &out); CoinalyzeIsRetryable(err) {
		t.Fatal("a 401 with rate-limit prose is still a 401")
	}
}

// AN UNEXPECTED BODY IS AN UPSTREAM ERROR, NOT ABSENCE OF DATA. `[]` means "I have nothing in
// this window"; a body that does not decode means "I answered something you do not
// understand". Collapsing the two writes an empty series and calls coverage complete.
func TestCoinalyzeUndecodableBodyIsAnError(t *testing.T) {
	client, _ := coinalyzeFake(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`<html>maintenance</html>`))
	})

	var out []any
	err := client.get(context.Background(), "/exchanges", nil, &out)
	if err == nil {
		t.Fatal("a body that does not decode must be an error")
	}
	if len(out) != 0 {
		t.Fatal("nothing may have been decoded")
	}
}

// AND THE EMPTY LIST IS NOT AN ERROR — it is the response the real API gives for a window
// with no data.
func TestCoinalyzeEmptyListIsNotAnError(t *testing.T) {
	client, _ := coinalyzeFake(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	})

	var out []any
	if err := client.get(context.Background(), "/exchanges", nil, &out); err != nil {
		t.Fatalf("an empty list is not an error: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected an empty list, got %d", len(out))
	}
}

// THE PROVIDER'S `Retry-After` SURVIVES the error, in the two spellings the RFC allows.
func TestCoinalyzeCarriesRetryAfter(t *testing.T) {
	client, _ := coinalyzeFake(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "42")
		w.WriteHeader(http.StatusTooManyRequests)
	})

	var out []any
	err := client.get(context.Background(), "/exchanges", nil, &out)
	var coinErr *CoinalyzeError
	if !errors.As(err, &coinErr) {
		t.Fatalf("expected CoinalyzeError, got %v", err)
	}
	if coinErr.RetryAfter != 42*time.Second {
		t.Fatalf("expected 42s, got %v", coinErr.RetryAfter)
	}
}

func TestCoinalyzeRetryAfterAsHTTPDate(t *testing.T) {
	client, _ := coinalyzeFake(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", time.Now().Add(30*time.Second).UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusTooManyRequests)
	})

	var out []any
	var coinErr *CoinalyzeError
	if !errors.As(client.get(context.Background(), "/exchanges", nil, &out), &coinErr) {
		t.Fatal("expected CoinalyzeError")
	}
	if coinErr.RetryAfter <= 0 || coinErr.RetryAfter > 31*time.Second {
		t.Fatalf("the HTTP date should become a ~30s wait, got %v", coinErr.RetryAfter)
	}
}

// ABSENCE OF `Retry-After` IS ZERO, and zero means "the provider did not say" — which is
// different from "wait zero". The budget, not the client, decides the fallback.
func TestCoinalyzeAbsentRetryAfterIsZero(t *testing.T) {
	client, _ := coinalyzeFake(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	})

	var out []any
	var coinErr *CoinalyzeError
	_ = errors.As(client.get(context.Background(), "/exchanges", nil, &out), &coinErr)
	if coinErr.RetryAfter != 0 {
		t.Fatalf("without a header the wait is zero, got %v", coinErr.RetryAfter)
	}
}

// THE URL OF AN ERROR CAN BE LOGGED, and the secret does not travel with it. An error that
// hides the whole URL cannot be diagnosed; one that shows it raw leaks.
func TestRedactedURLKeepsThePathAndDropsTheSecret(t *testing.T) {
	got := redactedURL("https://api.coinalyze.net/v1/open-interest-history?symbols=BTCUSDT_PERP.A&api_key=segredo")

	if strings.Contains(got, "segredo") {
		t.Fatalf("the secret leaked: %s", got)
	}
	if !strings.Contains(got, "open-interest-history") || !strings.Contains(got, "BTCUSDT_PERP.A") {
		t.Fatalf("the URL became undiagnosable: %s", got)
	}
	if !strings.Contains(got, "REDACTED") {
		t.Fatalf("the redaction does not announce itself: %s", got)
	}
}

// THE CANCELED CONTEXT WINS OVER THE CALL, instead of waiting for the whole timeout.
func TestCoinalyzeHonoursContextCancellation(t *testing.T) {
	client, _ := coinalyzeFake(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Second)
		_, _ = w.Write([]byte(`[]`))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var out []any
	start := time.Now()
	if err := client.get(ctx, "/exchanges", nil, &out); err == nil {
		t.Fatal("expected a cancellation failure")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("cancellation was not honored: %v", elapsed)
	}
}

// POSITIVE CONTROL: the client decodes the REAL shape measured on the API.
func TestCoinalyzeDecodesTheMeasuredShape(t *testing.T) {
	client, _ := coinalyzeFake(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"symbol":"BTCUSDT_PERP.A","history":[{"t":1683849600,"o":108759.582,"h":113410.896,"l":106832.225,"c":106886.179}]}]`))
	})

	var out []struct {
		Symbol  string `json:"symbol"`
		History []struct {
			T int64   `json:"t"`
			C float64 `json:"c"`
		} `json:"history"`
	}
	if err := client.get(context.Background(), "/open-interest-history", nil, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 || out[0].Symbol != "BTCUSDT_PERP.A" || len(out[0].History) != 1 {
		body, _ := json.Marshal(out)
		t.Fatalf("the measured shape was not decoded: %s", body)
	}
	if out[0].History[0].T != 1683849600 {
		t.Fatalf("the timestamp came out wrong: %d", out[0].History[0].T)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// THE THIRD SPELLING OF `Retry-After`.
//
// The four values below are not invented: they were MEASURED against the real API in probes
// spaced ~2.2 s apart, and the regression between them shows what they are — the countdown
// until the quota window reopens. `Atoi` fails on all four, and that is why every production
// log said exactly "waiting 1m0s".
// ─────────────────────────────────────────────────────────────────────────────

// THE FRACTIONAL WAIT IS INTERPRETED AS A DURATION.
func TestRetryAfterReadsTheFractionalSecondsTheProviderSends(t *testing.T) {
	casos := []struct {
		raw  string
		want time.Duration
	}{
		// The four values measured in production, in the order they arrived.
		{"31.076", 31076 * time.Millisecond},
		{"28.883", 28883 * time.Millisecond},
		{"26.706", 26706 * time.Millisecond},
		{"24.478", 24478 * time.Millisecond},
	}

	for _, caso := range casos {
		header := http.Header{}
		header.Set("Retry-After", caso.raw)

		got := retryAfterOf(header)
		if got != caso.want {
			t.Fatalf("Retry-After %q became %v, expected %v", caso.raw, got, caso.want)
		}
		if got == 0 {
			t.Fatalf("Retry-After %q was discarded, and the caller would fall back to the %v default",
				caso.raw, DefaultCoinalyzeRetryAfter)
		}
	}
}

// AND THE CONSEQUENCE IS THE ONE THE REQUIREMENT DESCRIBES: the applied wait becomes the
// provider's, NOT the default interval. This is the discriminating assertion — reading the
// right header and still waiting a minute fixes nothing.
func TestAFractional429DoesNotFallBackToTheDefaultInterval(t *testing.T) {
	client, _ := coinalyzeFake(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "26.706")
		w.WriteHeader(http.StatusTooManyRequests)
	})

	var out []any
	err := client.get(context.Background(), "/exchanges", nil, &out)

	var coinErr *CoinalyzeError
	if !errors.As(err, &coinErr) {
		t.Fatalf("expected CoinalyzeError, got %v", err)
	}
	if coinErr.RetryAfter != 26706*time.Millisecond {
		t.Fatalf("the error carried %v, expected 26.706s", coinErr.RetryAfter)
	}
	if pause := coinalyzeRetryPause(err); pause != 26706*time.Millisecond {
		t.Fatalf("the applied wait was %v, expected the provider's (26.706s); the %v default is still winning",
			pause, DefaultCoinalyzeRetryAfter)
	}
}

// WHAT CANNOT BE PARSED GOES TO THE LOG, RAW, TOGETHER WITH THE DEFAULT APPLIED. Discarding
// it silently is how the fractional form survived for months: the applied number was always
// the same, and nothing said it had not come from the provider.
func TestAnUnreadableRetryAfterIsLoggedRawWithTheDefaultApplied(t *testing.T) {
	ilegiveis := []string{
		"logo",  // prose
		"-5",    // negative: would become "resend now"
		"NaN",   // not a number
		"1e300", // does not fit in a time.Duration
	}

	for _, raw := range ilegiveis {
		log, sink := logger.NewCapture(logger.LevelDebug)

		header := http.Header{}
		header.Set("Retry-After", raw)
		got := retryAfterOf(header, log)

		records := sink.Records()

		if got != 0 {
			t.Fatalf("Retry-After %q must not become a wait, it became %v", raw, got)
		}

		var linha string
		for _, record := range records {
			if strings.Contains(record.Text(), "Retry-After") {
				linha = record.Text()
				break
			}
		}
		if linha == "" {
			t.Fatalf("Retry-After %q was discarded silently: %v", raw, records)
		}
		if !strings.Contains(linha, raw) {
			t.Fatalf("the raw value %q does not appear in the record: %s", raw, linha)
		}
		if !strings.Contains(linha, DefaultCoinalyzeRetryAfter.String()) {
			t.Fatalf("the applied default (%v) does not appear in the record: %s", DefaultCoinalyzeRetryAfter, linha)
		}
	}
}

// AND ABSENCE REMAINS SILENT. "The provider did not say" has no raw value to record, and a
// warning for every headerless 429 would drown the case where there is something to read.
func TestAnAbsentRetryAfterIsNotReportedAsUnreadable(t *testing.T) {
	log, sink := logger.NewCapture(logger.LevelDebug)
	got := retryAfterOf(http.Header{}, log)
	records := sink.Records()

	if got != 0 {
		t.Fatalf("without a header the wait is zero, got %v", got)
	}
	for _, record := range records {
		if strings.Contains(record.Text(), "Retry-After") {
			t.Fatalf("absence of a header is not an unreadable value: %s", record.Text())
		}
	}
}
