package ratelimit

import (
	"context"
	"testing"
)

func TestLimiterBackpressureAndAdaptive(t *testing.T) {
	l := New()
	if err := l.SetPolicy(Policy{Key: "tenant:t1", RatePerSecond: 1, Burst: 1, QueueLimit: 1, Adaptive: true, MinRatePerSecond: 1, MaxRatePerSecond: 5}); err != nil {
		t.Fatal(err)
	}
	d, err := l.Allow(context.Background(), "tenant:t1", 1)
	if err != nil || !d.Allowed {
		t.Fatalf("want allowed %#v %v", d, err)
	}
	d, err = l.Allow(context.Background(), "tenant:t1", 1)
	if err != nil || !d.Queued {
		t.Fatalf("want queued %#v %v", d, err)
	}
	d, _ = l.Allow(context.Background(), "tenant:t1", 1)
	if d.Allowed || d.Queued {
		t.Fatalf("want limited %#v", d)
	}
	l.MarkThrottle("tenant:t1")
	if l.Snapshot()[0].RatePerSecond != 1 {
		t.Fatal("adaptive min failed")
	}
}
