package coinalyze

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// The Coinalyze HTTP client, the SECOND derivatives provenance.
//
// # WHY A SECOND SOURCE
//
// Binance retains thirty days of open interest and long/short. Measured at the depth the
// collector declares, that is `1d -> 30 points`: a daily chart is born with one month.
// Coinalyze serves the same daily series since 2023 — measured, 1,200 points in one call —
// and removing that ceiling is why it exists.
//
// # THE KEY NEVER APPEARS
//
// It travels in the `api_key` header and nowhere else: not in the URL (which enters proxy
// logs and shell history), not in an error message. `redactedURL` exists so the URL of a
// failure can be logged without carrying a secret with it — an error that hides the whole
// URL cannot be diagnosed, and one that shows it raw leaks.
const (
	coinalyzeBaseURL = "https://api.coinalyze.net/v1"
	coinalyzeTimeout = 30 * time.Second
)

// CoinalyzeError carries the HTTP status so the caller can tell a permanent refusal from a
// transient one WITHOUT parsing prose. Classifying by message is how a provider's wording
// change silently turns a 401 into a retry loop.
type CoinalyzeError struct {
	Status  int
	Message string
	// RetryAfter is what the provider asked us to wait, when it asked. Zero means it did
	// not say, which is different from "wait zero".
	RetryAfter time.Duration
}

func (e *CoinalyzeError) Error() string {
	return fmt.Sprintf("coinalyze: http %d: %s", e.Status, e.Message)
}

// Retryable reports whether trying again could succeed.
//
// 400 and 401 are the caller's fault and repeating them only burns budget: a malformed
// window is malformed on the second attempt too, and a rejected key is rejected until a
// human changes it. 429 and 5xx are the provider's state, and state passes.
func (e *CoinalyzeError) Retryable() bool {
	if e.Status == http.StatusTooManyRequests {
		return true
	}
	return e.Status >= 500 && e.Status <= 599
}

// CoinalyzeClient talks to one Coinalyze deployment.
type CoinalyzeClient struct {
	baseURL string
	apiKey  string
	http    *http.Client
	log     *slog.Logger
}

// NewCoinalyzeClient builds a client. An empty key is NOT rejected here — the refusal
// belongs to configuration, where the operator has a file to fix; refusing at construction
// would make the collector unbuildable in a test that never calls the network.
func NewCoinalyzeClient(apiKey string, opts ...CoinalyzeOption) *CoinalyzeClient {
	c := &CoinalyzeClient{
		baseURL: coinalyzeBaseURL,
		apiKey:  apiKey,
		http:    &http.Client{Timeout: coinalyzeTimeout},
		log:     logger.OrDiscard(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// CoinalyzeOption configures a CoinalyzeClient.
type CoinalyzeOption func(*CoinalyzeClient)

// WithCoinalyzeBaseURL points the client at a fake in tests.
func WithCoinalyzeBaseURL(base string) CoinalyzeOption {
	return func(c *CoinalyzeClient) { c.baseURL = strings.TrimSuffix(base, "/") }
}

// WithCoinalyzeLogger injects the logger used for provider diagnostics. The option is applied
// by the constructor, exactly like the base-URL seam; a nil logger is a silent sink.
func WithCoinalyzeLogger(log *slog.Logger) CoinalyzeOption {
	return func(c *CoinalyzeClient) { c.log = logger.OrDiscard(log) }
}

// There is NO option to inject a transport. Tests point the client at an `httptest.Server`
// through `WithCoinalyzeBaseURL` — that is the seam they need. A public option nobody
// exercises is API that ages without proof.

// redactedURL renders a URL fit for a log: path and query kept, secrets removed.
//
// The key does not travel in the query today, and this is what keeps that true if someone
// ever adds it: a redaction that only handles the header would leave the URL as the leak.
func redactedURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "<invalid url>"
	}
	query := parsed.Query()
	for _, secret := range []string{"api_key", "apikey", "key", "token"} {
		if query.Has(secret) {
			query.Set(secret, "REDACTED")
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// get performs one request and decodes the body into `out`.
//
// A BODY THAT IS NOT THE EXPECTED SHAPE IS AN UPSTREAM ERROR, not an absence of data. The
// distinction is the whole point: `[]` means "the provider has nothing for this window",
// and a decode failure means "the provider answered something we do not understand".
// Collapsing the two writes an empty series and calls the coverage complete.
func (c *CoinalyzeClient) get(ctx context.Context, path string, params url.Values, out any) error {
	endpoint := c.baseURL + path
	if len(params) > 0 {
		endpoint += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("coinalyze: build request for %s: %w", redactedURL(endpoint), err)
	}
	req.Header.Set("Accept", "application/json")
	// The ONLY place the key travels.
	req.Header.Set("api_key", c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("coinalyze: get %s: %w", redactedURL(endpoint), err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if resp.StatusCode != http.StatusOK {
		return &CoinalyzeError{
			Status:     resp.StatusCode,
			Message:    messageOf(body),
			RetryAfter: retryAfterOf(resp.Header, c.log),
		}
	}
	if readErr != nil {
		return fmt.Errorf("coinalyze: read %s: %w", redactedURL(endpoint), readErr)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("coinalyze: %s answered a body we do not understand: %w", redactedURL(endpoint), err)
	}
	return nil
}

// GetHistory performs ONE history request and decodes the array response into `out`.
//
// It is the client's public seam and is generic in the body on purpose: the three history
// endpoints share the envelope (`[{symbol, history:[…]}]`) and differ only in the point —
// `{o,h,l,c}` for open interest and funding, `{r,l,s}` for long/short. Which of the two to
// request is the collection cycle's business; what this method guarantees is what holds for
// all three: the key travels in the header, the status classifies the error, and a body that
// is not the expected shape is an UPSTREAM ERROR and not an absence of data.
func (c *CoinalyzeClient) GetHistory(ctx context.Context, path string, params url.Values, out any) error {
	return c.get(ctx, path, params, out)
}

// messageOf pulls the provider's own wording out of an error body, falling back to the raw
// text. It is for the LOG, never for classification — see `CoinalyzeError.Retryable`.
func messageOf(body []byte) string {
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Message != "" {
		return envelope.Message
	}
	text := strings.TrimSpace(string(body))
	if len(text) > 200 {
		text = text[:200] + "…"
	}
	if text == "" {
		return "(empty body)"
	}
	return text
}

// retryAfterOf reads the provider's own wait, in the THREE spellings this API emits.
//
// # THE THIRD SPELLING, AND WHAT DISCARDING IT COST
//
// The RFC allows two: delay in whole seconds and an HTTP date. Coinalyze sends a third —
// FRACTIONAL seconds — measured in production in probes spaced ~2.2 s apart: `31.076`,
// `28.883`, `26.706`, `24.478`. It is a countdown to the window reopening, the most precise
// signal this provider emits.
//
// `strconv.Atoi("31.076")` fails. `http.ParseTime("31.076")` fails. Returning `0` for it
// would mean "the provider did not say" — the caller then falls back to
// `DefaultCoinalyzeRetryAfter`, which is why production logs said exactly "waiting 1m0s" and
// never 26.7s: the provider was not being vague, the reader was discarding its answer.
//
// `ParseFloat` covers the first two numeric spellings at once — `"42"` still means 42s —
// so it ADDS the fractional form without removing anything.
func retryAfterOf(header http.Header, logs ...*slog.Logger) time.Duration {
	log := logger.OrDiscard(logs...)
	raw := strings.TrimSpace(header.Get("Retry-After"))
	if raw == "" {
		// ABSENCE IS NOT AN UNREADABLE VALUE: the provider said nothing, and there is no raw
		// value to record. The budget decides the fallback.
		return 0
	}
	if wait, ok := coinalyzeSecondsWait(raw); ok {
		return wait
	}
	if when, err := http.ParseTime(raw); err == nil {
		if wait := time.Until(when); wait > 0 {
			return wait
		}
		// A date in the past: it has already expired, there is no wait to make. It is also
		// not unreadable.
		return 0
	}

	// THE RAW VALUE GOES TO THE LOG TOGETHER WITH THE DEFAULT THAT WILL BE APPLIED.
	// Discarding it silently is exactly how the fractional form survived for months: the
	// applied number was always the same, and nothing said it had not come from the
	// provider.
	log.Warn("coinalyze Retry-After unreadable; applying the default",
		"retry_after", raw, "default", DefaultCoinalyzeRetryAfter)
	return 0
}

// coinalyzeSecondsWait parses the delay in seconds, whole or fractional.
//
// The second return is "could parse", and it is FALSE for NaN, infinity, negative and
// anything that would not fit in a `time.Duration`: converting an out-of-range float to an
// integer has undefined behavior in Go, and a `Retry-After` that became a NEGATIVE duration
// would mean "resend now" — the opposite of what the provider demanded.
func coinalyzeSecondsWait(raw string) (time.Duration, bool) {
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 {
		return 0, false
	}
	if nanos := seconds * float64(time.Second); nanos > math.MaxInt64 {
		return 0, false
	}
	return time.Duration(seconds * float64(time.Second)), true
}

// CoinalyzeIsRetryable reports whether an error from this client is worth repeating.
func CoinalyzeIsRetryable(err error) bool {
	var coinErr *CoinalyzeError
	if errors.As(err, &coinErr) {
		return coinErr.Retryable()
	}
	return false
}
