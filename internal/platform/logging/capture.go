package logger

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
)

// Record is one emitted line, with the level that emitted it and its structured attributes.
type Record struct {
	Level   Level
	Message string
	Attrs   []slog.Attr
}

// Attr returns the value of the first attribute with the given key.
func (r Record) Attr(key string) (slog.Value, bool) {
	for _, attr := range r.Attrs {
		if attr.Key == key {
			return attr.Value, true
		}
	}
	return slog.Value{}, false
}

// Text renders the record as `message key=value ...`, the shape LevelOf searches. It exists so a
// substring assertion keeps working when a value moved from the message into an attribute.
func (r Record) Text() string {
	var b strings.Builder
	b.WriteString(r.Message)
	for _, attr := range r.Attrs {
		b.WriteByte(' ')
		b.WriteString(attr.Key)
		b.WriteByte('=')
		b.WriteString(attr.Value.String())
	}
	return b.String()
}

// String renders the record as `LEVEL: text`.
func (r Record) String() string { return fmt.Sprintf("%s: %s", r.Level, r.Text()) }

// Sink stores what was emitted during a capture.
type Sink struct {
	mu      sync.Mutex
	records []Record
}

func (s *Sink) append(level Level, message string, attrs []slog.Attr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]slog.Attr, len(attrs))
	copy(copied, attrs)
	s.records = append(s.records, Record{Level: level, Message: message, Attrs: copied})
}

// Records returns a COPY of what was emitted, in emission order.
func (s *Sink) Records() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, len(s.records))
	copy(out, s.records)
	for i := range out {
		copied := make([]slog.Attr, len(out[i].Attrs))
		copy(copied, out[i].Attrs)
		out[i].Attrs = copied
	}
	return out
}

// LevelOf returns the level of the FIRST line whose message or attributes contain substr, and
// false when none does.
//
// Returning the level rather than a boolean "emitted at X" is deliberate: a failing assertion can
// say WHICH level came out, which is the information missing when a severity silently drops.
func (s *Sink) LevelOf(substr string) (Level, bool) {
	for _, r := range s.Records() {
		if strings.Contains(r.Text(), substr) {
			return r.Level, true
		}
	}
	return LevelDebug, false
}

// captureHandler writes slog records into a Sink. It is per-logger state: two tests can capture
// concurrently because nothing here is a package variable.
type captureHandler struct {
	sink   *Sink
	min    slog.Level
	attrs  []slog.Attr
	groups []string
}

func (h *captureHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.min
}

func (h *captureHandler) Handle(_ context.Context, record slog.Record) error {
	attrs := make([]slog.Attr, 0, len(h.attrs)+record.NumAttrs())
	attrs = append(attrs, h.attrs...)
	record.Attrs(func(attr slog.Attr) bool {
		attrs = append(attrs, attr)
		return true
	})
	if len(h.groups) > 0 {
		prefix := strings.Join(h.groups, ".")
		for i := range attrs {
			attrs[i].Key = prefix + "." + attrs[i].Key
		}
	}
	h.sink.append(levelFromSlog(record.Level), record.Message, attrs)
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	clone.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &clone
}

func (h *captureHandler) WithGroup(name string) slog.Handler {
	clone := *h
	clone.groups = append(append([]string(nil), h.groups...), name)
	return &clone
}

func levelFromSlog(level slog.Level) Level {
	switch level {
	case slog.LevelDebug:
		return LevelDebug
	case slog.LevelInfo:
		return LevelInfo
	case slog.LevelWarn:
		return LevelWarn
	case slog.LevelError:
		return LevelError
	default:
		return Level(fmt.Sprintf("LEVEL(%d)", level))
	}
}

// NewCapture returns a logger that records into a fresh sink, plus that sink. It has no restore
// step because it never touched global state; every test owns its logger.
//
// The capture threshold is lowered to the requested level, so a test can observe Warn while the
// production process runs at Error.
func NewCapture(level Level) (*slog.Logger, *Sink) {
	sink := &Sink{}
	return slog.New(&captureHandler{sink: sink, min: level.SlogLevel()}), sink
}
