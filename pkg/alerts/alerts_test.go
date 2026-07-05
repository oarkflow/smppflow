package alerts

import (
	"context"
	"testing"
	"time"
)

func TestAlertForDuration(t *testing.T) {
	e := New()
	e.AddRule(Rule{ID: "dlq", Signal: "dlq_depth", Above: 10, For: time.Minute, Severity: Critical})
	t0 := time.Now()
	if xs := e.Evaluate(context.Background(), Signal{Name: "dlq_depth", Value: 11, At: t0}); len(xs) != 0 {
		t.Fatal("should wait")
	}
	xs := e.Evaluate(context.Background(), Signal{Name: "dlq_depth", Value: 11, At: t0.Add(time.Minute)})
	if len(xs) != 1 || xs[0].Status != "open" {
		t.Fatalf("want open %#v", xs)
	}
	e.Evaluate(context.Background(), Signal{Name: "dlq_depth", Value: 0, At: t0.Add(2 * time.Minute)})
	if len(e.Open()) != 0 {
		t.Fatal("should close")
	}
}
