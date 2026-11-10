// Package exchange provides retry logic for transient exchange API failures.
package exchange

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	logger "github.com/marxms/go-whale/internal/platform/logging"
)

// RetryConfig configures retry behavior for exchange operations.
type RetryConfig struct {
	MaxAttempts int           // Maximum number of attempts (default: 3)
	BaseDelay   time.Duration // Initial delay between retries (default: 1s)
	MaxDelay    time.Duration // Maximum delay between retries (default: 30s)
	BackoffMult float64       // Backoff multiplier (default: 2.0)
}

// DefaultRetryConfig returns sensible defaults for exchange API retries.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 3,
		BaseDelay:   1 * time.Second,
		MaxDelay:    30 * time.Second,
		BackoffMult: 2.0,
	}
}

// isRetryableError checks if an error is transient and worth retrying.
// Covers: rate limits (429), network timeouts, connection refused, reset.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Rate limits
	if strings.Contains(errStr, "429") || strings.Contains(errStr, "rate limit") ||
		strings.Contains(errStr, "too many requests") || strings.Contains(errStr, "ratelimit") {
		return true
	}

	// Network transient errors. `eof` covers the truncated response
	// (`unexpected EOF`) that Translate already classifies as transient; without
	// it, the classification said transient while the retry helper refused it.
	if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset") || strings.Contains(errStr, "temporary failure") ||
		strings.Contains(errStr, "no such host") || strings.Contains(errStr, "i/o timeout") ||
		strings.Contains(errStr, "network is unreachable") || strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "eof") {
		return true
	}

	// Exchange maintenance (5xx errors)
	if strings.Contains(errStr, "503") || strings.Contains(errStr, "502") ||
		strings.Contains(errStr, "504") || strings.Contains(errStr, "service unavailable") {
		return true
	}

	return false
}

// WithRetry executes fn with exponential backoff retry on transient failures.
// Returns the last error if all attempts fail.
func WithRetry(ctx context.Context, log *slog.Logger, cfg RetryConfig, label string, fn func() error) error {
	log = logger.OrDiscard(log)
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 1 * time.Second
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 30 * time.Second
	}
	if cfg.BackoffMult <= 0 {
		cfg.BackoffMult = 2.0
	}

	var lastErr error

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		// Check context before each attempt
		if ctx.Err() != nil {
			return fmt.Errorf("%s: context cancelled: %w", label, ctx.Err()) //nolint:misspell // error string is caller-observable; do not rewrite it
		}

		// Execute the function
		err := fn()
		if err == nil {
			if attempt > 1 {
				log.Info("retry succeeded", "label", label, "attempt", attempt)
			}
			return nil
		}

		lastErr = err

		// Check if retryable
		if !isRetryableError(err) {
			return fmt.Errorf("%s: non-retryable error: %w", label, err)
		}

		// Last attempt - don't sleep
		if attempt >= cfg.MaxAttempts {
			break
		}

		// Calculate backoff delay with exponential increase
		delay := time.Duration(float64(cfg.BaseDelay) * math.Pow(cfg.BackoffMult, float64(attempt-1)))
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}

		log.Warn("retry attempt failed (retryable)",
			"label", label,
			"attempt", attempt,
			"max_attempts", cfg.MaxAttempts,
			"error", err,
			"retry_in", delay)

		// Sleep with context cancellation support
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: context cancelled during retry backoff: %w", label, ctx.Err()) //nolint:misspell // error string is caller-observable; do not rewrite it
		case <-time.After(delay):
		}
	}

	return fmt.Errorf("%s: all %d attempts failed: %w", label, cfg.MaxAttempts, lastErr)
}

// WithRetryResult executes fn with retry and returns the result on success.
func WithRetryResult[T any](ctx context.Context, log *slog.Logger, cfg RetryConfig, label string, fn func() (T, error)) (T, error) {
	var result T
	err := WithRetry(ctx, log, cfg, label, func() error {
		var fnErr error
		result, fnErr = fn()
		return fnErr
	})
	return result, err
}

// ErrAllAttemptsFailed is returned when all retry attempts are exhausted.
var ErrAllAttemptsFailed = errors.New("all retry attempts failed")
