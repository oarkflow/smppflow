package sla_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/sla"
)

type fakeSender struct {
	latency time.Duration
	err     error
}

func (f fakeSender) SendCanary(ctx context.Context, req message.SubmitRequest) (string, time.Duration, error) {
	return "canary-1", f.latency, f.err
}

func TestRunCanaryRequiresSender(t *testing.T) {
	a := sla.New()
	if _, err := a.RunCanary(context.Background(), "p", message.SubmitRequest{}); err == nil {
		t.Fatal("RunCanary without a sender must fail")
	}
}

func TestRunCanarySuccess(t *testing.T) {
	a := sla.New()
	a.SetSLO(sla.SLO{ProviderID: "p", MaxLatency: time.Second})
	a.Sender = fakeSender{latency: 50 * time.Millisecond}
	st, err := a.RunCanary(context.Background(), "p", message.SubmitRequest{})
	if err != nil {
		t.Fatalf("RunCanary: %v", err)
	}
	if st.Sent != 1 || st.Delivered != 1 || st.Failed != 0 {
		t.Fatalf("counters=%+v", st)
	}
	if st.LastLatency != 50*time.Millisecond || st.LastSuccessAt.IsZero() {
		t.Fatalf("state=%+v", st)
	}
	if st.IncidentOpen || !st.CoolingUntil.IsZero() {
		t.Fatalf("healthy canary must not open incidents or cooldowns: %+v", st)
	}
	if len(a.CooldownProviders()) != 0 {
		t.Fatalf("cooldowns=%v want none", a.CooldownProviders())
	}
}

func TestRunCanaryLatencyBreachOpensIncidentAndCooldown(t *testing.T) {
	a := sla.New()
	a.SetSLO(sla.SLO{ProviderID: "p", MaxLatency: 10 * time.Millisecond, Cooldown: time.Minute})
	a.Sender = fakeSender{latency: 500 * time.Millisecond}
	inc := &sla.MemoryIncidents{}
	a.Incidents = inc
	st, err := a.RunCanary(context.Background(), "p", message.SubmitRequest{})
	if err != nil {
		t.Fatalf("RunCanary: %v", err)
	}
	if !st.IncidentOpen {
		t.Fatalf("incident must open on SLO breach: %+v", st)
	}
	if st.CoolingUntil.Before(time.Now()) {
		t.Fatalf("cooldown must be in the future: %+v", st)
	}
	if len(inc.Items) != 1 {
		t.Fatalf("incidents=%d want 1", len(inc.Items))
	}
	if inc.Items[0].Reason != "latency SLO exceeded" {
		t.Fatalf("reason=%q", inc.Items[0].Reason)
	}
	cooling := a.CooldownProviders()
	if len(cooling) != 1 || cooling[0] != "p" {
		t.Fatalf("cooldowns=%v want [p]", cooling)
	}
}

func TestRunCanaryFailureOpensSingleIncident(t *testing.T) {
	a := sla.New()
	a.SetSLO(sla.SLO{ProviderID: "p"})
	a.Sender = fakeSender{err: errors.New("dial tcp: connection refused")}
	inc := &sla.MemoryIncidents{}
	a.Incidents = inc
	for i := 0; i < 3; i++ {
		st, err := a.RunCanary(context.Background(), "p", message.SubmitRequest{})
		if err == nil {
			t.Fatal("failed canary must return the sender error")
		}
		if st.Sent != int64(i+1) || st.Failed != int64(i+1) {
			t.Fatalf("counters=%+v", st)
		}
	}
	if len(inc.Items) != 1 {
		t.Fatalf("incidents=%d want 1 (deduplicated while open)", len(inc.Items))
	}

	a.Sender = fakeSender{latency: time.Millisecond}
	st, err := a.RunCanary(context.Background(), "p", message.SubmitRequest{})
	if err != nil {
		t.Fatalf("recovery canary: %v", err)
	}
	if st.IncidentOpen {
		t.Fatal("incident must close on recovery")
	}
	a.Sender = fakeSender{err: errors.New("again")}
	if _, err := a.RunCanary(context.Background(), "p", message.SubmitRequest{}); err == nil {
		t.Fatal("expected failure")
	}
	if len(inc.Items) != 2 {
		t.Fatalf("incidents=%d want 2 (new incident after recovery)", len(inc.Items))
	}
}

func TestRunCanaryBlackholeDetection(t *testing.T) {
	a := sla.New()
	a.SetSLO(sla.SLO{ProviderID: "p", BlackholeAfter: time.Hour})
	a.States["p"] = sla.State{ProviderID: "p", LastSuccessAt: time.Now().Add(-3 * time.Hour)}
	a.Sender = fakeSender{err: errors.New("timeout")}
	inc := &sla.MemoryIncidents{}
	a.Incidents = inc
	if _, err := a.RunCanary(context.Background(), "p", message.SubmitRequest{}); err == nil {
		t.Fatal("expected failure")
	}
	if len(inc.Items) != 1 || inc.Items[0].Reason != "blackhole detected" {
		t.Fatalf("incidents=%+v want blackhole", inc.Items)
	}
}

func TestMemoryIncidents(t *testing.T) {
	m := &sla.MemoryIncidents{}
	if err := m.CreateIncident(context.Background(), sla.Incident{ID: "i1", Severity: "major"}); err != nil {
		t.Fatal(err)
	}
	if err := m.CreateIncident(context.Background(), sla.Incident{ID: "i2"}); err != nil {
		t.Fatal(err)
	}
	if len(m.Items) != 2 || m.Items[0].ID != "i1" {
		t.Fatalf("items=%+v", m.Items)
	}
}
