package venue

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/marxms/go-whale/internal/adapters/outbound/exchange"
	"github.com/marxms/go-whale/internal/market"
)

const (
	maxErrorBody = 512
	maxAttempts  = 3
)

// getJSON performs one GET and returns the decoded payload. For a BingX
// envelope it returns the `data` field; for Binance it returns the bare body.
// acquire runs before every attempt, so a retry pays the same weight as the
// first call.
func (c *Client) getJSON(
	ctx context.Context,
	path string,
	query url.Values,
	acquire func(context.Context) error,
) (any, error) {
	endpoint := c.baseURL() + path
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}

	var payload any
	cfg := exchange.DefaultRetryConfig()
	cfg.MaxAttempts = maxAttempts

	err := exchange.WithRetry(ctx, c.log, cfg, "venue "+c.venue.String(), func() error {
		if acquire != nil {
			if err := acquire(ctx); err != nil {
				return err
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return fmt.Errorf("build request for %s: %w", path, err)
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "gowhale/1.0")

		resp, err := c.http.Do(req)
		if err != nil {
			return exchange.Translate(err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			if c.venue == market.Binance &&
				(resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusTeapot) {
				c.budget.PenalizeForRefusal()
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
			return exchange.Translate(fmt.Errorf("HTTP %d on %s: %s", resp.StatusCode, path, truncate(body, maxErrorBody)))
		}

		// Decode from the body stream: the spot exchangeInfo is ~18 MB and a
		// fixed read limit truncated it into an unexpected EOF.
		decoder := json.NewDecoder(resp.Body)
		decoder.UseNumber()

		var raw any
		if err := decoder.Decode(&raw); err != nil {
			return exchange.Translate(fmt.Errorf("decode %s: %w", path, err))
		}
		decoded, err := decodePayload(raw)
		if err != nil {
			return exchange.Translate(fmt.Errorf("decode %s: %w", path, err))
		}
		payload = decoded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func decodePayload(raw any) (any, error) {
	envelope, ok := raw.(map[string]any)
	if !ok {
		return raw, nil
	}

	code, hasCode := envelope["code"]
	if !hasCode {
		return raw, nil
	}
	if n, ok := asInt64(code); ok && n != 0 {
		msg, _ := asString(envelope["msg"])
		return nil, fmt.Errorf("venue code %d: %s", n, msg)
	}
	if data, ok := envelope["data"]; ok {
		return data, nil
	}
	return raw, nil
}

func truncate(body []byte, limit int) string {
	text := strings.TrimSpace(string(body))
	if len(text) <= limit {
		return text
	}
	return text[:limit] + "..."
}

func asMap(value any) (map[string]any, bool) {
	m, ok := value.(map[string]any)
	return m, ok
}

func asSlice(value any) ([]any, bool) {
	s, ok := value.([]any)
	return s, ok
}

func asString(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case json.Number:
		return v.String(), true
	default:
		return "", false
	}
}

func asFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func asInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case json.Number:
		n, err := v.Int64()
		if err == nil {
			return n, true
		}
		f, ferr := v.Float64()
		return int64(f), ferr == nil
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err == nil {
			return n, true
		}
		f, ferr := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return int64(f), ferr == nil
	default:
		return 0, false
	}
}

func asBool(value any) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "1":
			return true, true
		case "false", "0":
			return false, true
		}
	case json.Number:
		n, err := v.Int64()
		if err == nil {
			return n != 0, true
		}
	}
	return false, false
}

func ptrString(value any) *string {
	if s, ok := asString(value); ok {
		return &s
	}
	return nil
}

func ptrFloat(value any) *float64 {
	if f, ok := asFloat(value); ok {
		return &f
	}
	return nil
}

func ptrInt64(value any) *int64 {
	if n, ok := asInt64(value); ok {
		return &n
	}
	return nil
}

func ptrBool(value any) *bool {
	if b, ok := asBool(value); ok {
		return &b
	}
	return nil
}
