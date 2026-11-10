// Package logger builds the process logger from configuration.
//
// There is NO package-level logger and no mutable level. Every component that logs receives a
// *slog.Logger through its constructor, so a test can observe exactly what one component emitted
// without swapping global state and without racing another test in the same binary.
package logger

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Level is the configured minimum severity.
//
// It is a string rather than slog.Level so an operator typo is a startup error instead of a
// silently accepted integer.
type Level string

// LevelDebug, LevelInfo, LevelWarn and LevelError are the accepted log levels.
const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

// Supported output formats.
const (
	FormatText = "text"
	FormatJSON = "json"
)

// String names the level in upper case, matching the prefix the old logger printed.
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return string(l)
	}
}

// SlogLevel maps the configured level onto slog's scale.
func (l Level) SlogLevel() slog.Level {
	switch l {
	case LevelDebug:
		return slog.LevelDebug
	case LevelWarn:
		return slog.LevelWarn
	case LevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// ParseLevel accepts the published vocabulary and normalizes case and padding. "warning" is
// kept as an alias because GOWHALE_LOG_LEVEL accepted it before this package moved to slog.
func ParseLevel(value string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return LevelInfo, nil
	case "debug":
		return LevelDebug, nil
	case "warn", "warning":
		return LevelWarn, nil
	case "error":
		return LevelError, nil
	default:
		return "", fmt.Errorf("invalid log level %q: want debug, info, warn or error", value)
	}
}

// Config is the complete input of New.
type Config struct {
	Level  Level
	Format string
	Writer io.Writer
}

// ConfigFromProcess reads the process-wide logging environment. Both keys default to the
// documented values (info/text); an unparseable level is an error, never a silent downgrade.
func ConfigFromProcess() (Config, error) {
	level, err := ParseLevel(os.Getenv("GOWHALE_LOG_LEVEL"))
	if err != nil {
		return Config{}, err
	}
	format := strings.ToLower(strings.TrimSpace(os.Getenv("GOWHALE_LOG_FORMAT")))
	if format == "" {
		format = FormatText
	}
	return Config{Level: level, Format: format, Writer: os.Stdout}, nil
}

// New builds one logger. The writer defaults to stdout and the level defaults to info; an
// unrecognized level or format is an error so the process fails at boot, not at first log line.
func New(cfg Config) (*slog.Logger, error) {
	level, err := ParseLevel(string(cfg.Level))
	if err != nil {
		return nil, err
	}

	format := strings.ToLower(strings.TrimSpace(cfg.Format))
	if format == "" {
		format = FormatText
	}

	writer := cfg.Writer
	if writer == nil {
		writer = os.Stdout
	}

	opts := &slog.HandlerOptions{Level: level.SlogLevel()}
	switch format {
	case FormatText:
		return slog.New(slog.NewTextHandler(writer, opts)), nil
	case FormatJSON:
		return slog.New(slog.NewJSONHandler(writer, opts)), nil
	default:
		return nil, fmt.Errorf("invalid log format %q: want text or json", cfg.Format)
	}
}

// OrDiscard returns the first non-nil logger, or a logger that drops everything. Constructors
// use it so a zero value keeps working after the global logger is gone; call sites never fall
// back to process-wide state.
func OrDiscard(logs ...*slog.Logger) *slog.Logger {
	for _, log := range logs {
		if log != nil {
			return log
		}
	}
	// A fresh discard logger is cheap and carries no process-wide state; OrDiscard is reached
	// with nil only by zero-value fixtures, never on a hot path.
	return slog.New(slog.DiscardHandler)
}
