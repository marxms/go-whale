package exchange

import (
	"errors"
	"strings"
)

// Canonical error classes for outbound venue calls.
//
// They exist so a concrete vendor error does not have to cross the adapter
// boundary: the adapter classifies the failure and returns a wrapper whose
// TEXT keeps the venue's own wording (the marker strings the rest of the
// process already classifies by) while its TYPE is one of these sentinels.
var (
	// ErrRateLimited is the venue refusing us for rate (429/418/-1003).
	ErrRateLimited = errors.New("exchange: rate limited")
	// ErrTransient is a condition that passes: timeout, reset, 5xx, EOF.
	ErrTransient = errors.New("exchange: transient failure")
	// ErrPermanent is a verdict about the request or the symbol.
	ErrPermanent = errors.New("exchange: permanent failure")
)

func rateLimitMarkers() []string {
	return []string{
		"429", "418", "-1003", "too many requests", "ratelimit", "rate limit", "ddosprotection",
	}
}

func transientMarkers() []string {
	return []string{
		"timeout", "connection refused", "connection reset", "temporary failure",
		"no such host", "i/o timeout", "network is unreachable", "broken pipe",
		"502", "503", "504", "service unavailable", "eof", "temporarily",
	}
}

// translatedError keeps the vendor's WORDING but not its value.
//
// This is deliberate and it is the boundary: Unwrap returns only the canonical
// class, so errors.Is works while errors.As cannot recover the vendor's
// concrete type from a consumer. Wrapping the cause with %w would leak exactly
// the type this adapter exists to contain.
type translatedError struct {
	class error
	cause string
}

func (e *translatedError) Error() string { return e.class.Error() + ": " + e.cause }

func (e *translatedError) Unwrap() error { return e.class }

// Translate classifies a vendor error and returns a canonical wrapper.
//
// The original message is kept for logs and for the existing string-based
// classification; the original value is reduced to text so no vendor type
// crosses this boundary.
func Translate(err error) error {
	if err == nil {
		return nil
	}

	message := strings.ToLower(err.Error())
	switch {
	case containsAny(message, rateLimitMarkers()):
		return &translatedError{class: ErrRateLimited, cause: err.Error()}
	case containsAny(message, transientMarkers()):
		return &translatedError{class: ErrTransient, cause: err.Error()}
	default:
		return &translatedError{class: ErrPermanent, cause: err.Error()}
	}
}

func containsAny(haystack string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(haystack, needle) {
			return true
		}
	}
	return false
}
