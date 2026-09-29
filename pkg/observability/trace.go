package observability

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Span is a lightweight, stdlib-only tracing primitive: enough to
// correlate a single request/message's path (e.g. bind -> submit -> route
// -> deliver) through structured logs, without pulling in a full
// distributed tracing dependency.
//
// A Span is created with StartSpan, attached to a context.Context, and
// finished with End, which logs the completed span via log/slog.
type Span struct {
	traceID  string
	spanID   string
	parentID string
	name     string
	start    time.Time

	mu       sync.Mutex
	attrs    []slog.Attr
	ended    atomic.Bool
	duration time.Duration
	logger   *slog.Logger
}

type spanContextKey struct{}

// SpanOption configures a Span at creation time.
type SpanOption func(*Span)

// WithLogger sets the *slog.Logger a span logs to on End. Defaults to
// slog.Default().
func WithLogger(l *slog.Logger) SpanOption {
	return func(s *Span) {
		if l != nil {
			s.logger = l
		}
	}
}

// StartSpan starts a new span named name, as a child of any span already
// present in ctx (inheriting its trace ID), or as the root of a new trace
// otherwise. The returned context carries the new span; retrieve it later
// with SpanFromContext.
func StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	s := &Span{
		spanID: newHexID(8),
		name:   name,
		start:  time.Now(),
		logger: slog.Default(),
	}
	if parent, ok := SpanFromContext(ctx); ok {
		s.traceID = parent.traceID
		s.parentID = parent.spanID
	} else {
		s.traceID = newHexID(16)
	}
	for _, opt := range opts {
		opt(s)
	}
	return context.WithValue(ctx, spanContextKey{}, s), s
}

// SpanFromContext returns the span stored in ctx, if any.
func SpanFromContext(ctx context.Context) (*Span, bool) {
	s, ok := ctx.Value(spanContextKey{}).(*Span)
	return s, ok
}

// TraceID returns the span's trace ID (shared by every span in the same
// trace).
func (s *Span) TraceID() string { return s.traceID }

// SpanID returns this span's own ID.
func (s *Span) SpanID() string { return s.spanID }

// ParentSpanID returns the parent span's ID, or "" if this is a root span.
func (s *Span) ParentSpanID() string { return s.parentID }

// Name returns the span's name.
func (s *Span) Name() string { return s.name }

// TraceParent renders the span in W3C traceparent header format:
// version-trace_id-parent_id-flags. See
// https://www.w3.org/TR/trace-context/#traceparent-header
func (s *Span) TraceParent() string {
	return "00-" + s.traceID + "-" + s.spanID + "-01"
}

// SetAttr records a structured attribute to be logged when the span ends.
// Safe for concurrent use.
func (s *Span) SetAttr(key string, value any) {
	s.mu.Lock()
	s.attrs = append(s.attrs, slog.Any(key, value))
	s.mu.Unlock()
}

// Duration returns the span's recorded duration. Zero until End has been
// called.
func (s *Span) Duration() time.Duration { return s.duration }

// End finishes the span, recording its duration and logging it via
// log/slog with structured fields (trace_id, span_id, parent_span_id,
// name, duration_ms), plus any attributes set with SetAttr. End is
// idempotent: only the first call has an effect.
func (s *Span) End() {
	s.EndWithError(nil)
}

// EndWithError finishes the span like End, additionally logging err (if
// non-nil) and using a warn level for failed spans.
func (s *Span) EndWithError(err error) {
	if !s.ended.CompareAndSwap(false, true) {
		return
	}
	s.duration = time.Since(s.start)

	s.mu.Lock()
	attrs := make([]slog.Attr, 0, len(s.attrs)+6)
	attrs = append(attrs,
		slog.String("trace_id", s.traceID),
		slog.String("span_id", s.spanID),
	)
	if s.parentID != "" {
		attrs = append(attrs, slog.String("parent_span_id", s.parentID))
	}
	attrs = append(attrs,
		slog.String("name", s.name),
		slog.Float64("duration_ms", float64(s.duration.Microseconds())/1000.0),
	)
	attrs = append(attrs, s.attrs...)
	s.mu.Unlock()

	logger := s.logger
	if logger == nil {
		logger = slog.Default()
	}
	level := slog.LevelInfo
	if err != nil {
		level = slog.LevelWarn
		attrs = append(attrs, slog.String("error", err.Error()))
	}
	logger.LogAttrs(context.Background(), level, "span."+s.name, attrs...)
}

// newHexID returns a random hex-encoded ID of n bytes (2n hex characters),
// generated with crypto/rand. It falls back to a math/rand-derived value
// in the extremely unlikely event crypto/rand fails to read entropy, so ID
// generation never panics or blocks indefinitely.
func newHexID(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		// Fallback so ID generation never panics or blocks indefinitely
		// in the extremely unlikely event crypto/rand fails to read
		// entropy: derive bytes from the current time plus a counter,
		// which is unique-enough for log correlation even if it is not
		// cryptographically random.
		seed := uint64(time.Now().UnixNano()) ^ idFallbackCounter.Add(1)
		for i := range buf {
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], seed)
			buf[i] = b[i%8]
			seed = seed*6364136223846793005 + 1
		}
	}
	return hex.EncodeToString(buf)
}

var idFallbackCounter atomic.Uint64
