package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestSpanStartEndDuration(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	ctx, span := StartSpan(context.Background(), "submit", WithLogger(logger))
	if span.TraceID() == "" || span.SpanID() == "" {
		t.Fatal("expected non-empty trace/span IDs")
	}
	if span.ParentSpanID() != "" {
		t.Fatal("root span should have no parent")
	}
	got, ok := SpanFromContext(ctx)
	if !ok || got != span {
		t.Fatal("expected span retrievable from context")
	}

	time.Sleep(2 * time.Millisecond)
	span.End()

	if span.Duration() <= 0 {
		t.Fatal("expected recorded duration > 0")
	}

	var logged map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logged); err != nil {
		t.Fatalf("expected valid JSON log line: %v\n%s", err, buf.String())
	}
	if logged["trace_id"] != span.TraceID() {
		t.Fatalf("expected trace_id %q, got %v", span.TraceID(), logged["trace_id"])
	}
	if logged["span_id"] != span.SpanID() {
		t.Fatalf("expected span_id %q, got %v", span.SpanID(), logged["span_id"])
	}
	if logged["name"] != "submit" {
		t.Fatalf("expected name submit, got %v", logged["name"])
	}
	if _, ok := logged["duration_ms"]; !ok {
		t.Fatal("expected duration_ms field in log output")
	}
}

func TestSpanEndIdempotent(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	_, span := StartSpan(context.Background(), "x", WithLogger(logger))
	span.End()
	first := buf.String()
	span.End()
	if buf.String() != first {
		t.Fatalf("End should be idempotent, log grew after second call:\n%s", buf.String())
	}
}

func TestSpanParentChildTraceID(t *testing.T) {
	ctx, root := StartSpan(context.Background(), "root")
	defer root.End()
	_, child := StartSpan(ctx, "child")
	defer child.End()

	if child.TraceID() != root.TraceID() {
		t.Fatalf("expected child to inherit trace ID: root=%s child=%s", root.TraceID(), child.TraceID())
	}
	if child.ParentSpanID() != root.SpanID() {
		t.Fatalf("expected child parent span id to equal root span id")
	}
	if child.SpanID() == root.SpanID() {
		t.Fatal("expected distinct span IDs")
	}
}

func TestTraceParentFormat(t *testing.T) {
	_, span := StartSpan(context.Background(), "x")
	defer span.End()
	tp := span.TraceParent()
	parts := strings.Split(tp, "-")
	if len(parts) != 4 {
		t.Fatalf("expected 4 dash-separated parts, got %v", parts)
	}
	if parts[0] != "00" {
		t.Fatalf("expected version 00, got %s", parts[0])
	}
	if len(parts[1]) != 32 {
		t.Fatalf("expected 32 hex char trace id, got %d chars: %s", len(parts[1]), parts[1])
	}
	if len(parts[2]) != 16 {
		t.Fatalf("expected 16 hex char span id, got %d chars: %s", len(parts[2]), parts[2])
	}
	if parts[3] != "01" {
		t.Fatalf("expected flags 01, got %s", parts[3])
	}
}

func TestSpanEndWithError(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	_, span := StartSpan(context.Background(), "failing", WithLogger(logger))
	span.EndWithError(context.DeadlineExceeded)

	var logged map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logged); err != nil {
		t.Fatalf("expected valid JSON: %v\n%s", err, buf.String())
	}
	if logged["level"] != "WARN" {
		t.Fatalf("expected WARN level for failed span, got %v", logged["level"])
	}
	if logged["error"] != context.DeadlineExceeded.Error() {
		t.Fatalf("expected error field, got %v", logged["error"])
	}
}

func TestSpanAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	_, span := StartSpan(context.Background(), "attrd", WithLogger(logger))
	span.SetAttr("provider_id", "p1")
	span.SetAttr("tenant_id", "t1")
	span.End()

	var logged map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logged); err != nil {
		t.Fatalf("expected valid JSON: %v\n%s", err, buf.String())
	}
	if logged["provider_id"] != "p1" || logged["tenant_id"] != "t1" {
		t.Fatalf("expected custom attrs in log line: %v", logged)
	}
}

func TestUniqueIDs(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		_, span := StartSpan(context.Background(), "x")
		if seen[span.SpanID()] {
			t.Fatalf("duplicate span id generated: %s", span.SpanID())
		}
		seen[span.SpanID()] = true
	}
}
