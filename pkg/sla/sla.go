package sla

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/routing"
)

type ProbeSender interface {
	SendCanary(context.Context, message.SubmitRequest) (providerMessageID string, latency time.Duration, err error)
}
type IncidentSink interface {
	CreateIncident(context.Context, Incident) error
}
type Incident struct {
	ID         string
	ProviderID string
	Severity   string
	Reason     string
	StartedAt  time.Time
	Meta       map[string]string
}
type SLO struct {
	ProviderID      string
	MaxLatency      time.Duration
	MinDeliveryRate float64
	MaxErrorRate    float64
	BlackholeAfter  time.Duration
	Cooldown        time.Duration
}
type State struct {
	ProviderID    string
	LastCanaryAt  time.Time
	LastSuccessAt time.Time
	LastLatency   time.Duration
	Sent          int64
	Failed        int64
	Delivered     int64
	CoolingUntil  time.Time
	IncidentOpen  bool
}
type Automation struct {
	mu        sync.Mutex
	Router    *routing.Router
	Sender    ProbeSender
	Incidents IncidentSink
	SLOs      map[string]SLO
	States    map[string]State
}

func New() *Automation             { return &Automation{SLOs: map[string]SLO{}, States: map[string]State{}} }
func (a *Automation) SetSLO(s SLO) { a.mu.Lock(); a.SLOs[s.ProviderID] = s; a.mu.Unlock() }
func (a *Automation) RunCanary(ctx context.Context, providerID string, req message.SubmitRequest) (State, error) {
	a.mu.Lock()
	slo := a.SLOs[providerID]
	st := a.States[providerID]
	a.mu.Unlock()
	if a.Sender == nil {
		return st, errors.New("canary sender required")
	}
	_, lat, err := a.Sender.SendCanary(ctx, req)
	now := time.Now()
	st.ProviderID = providerID
	st.LastCanaryAt = now
	st.Sent++
	reason := ""
	if err != nil {
		st.Failed++
		reason = "canary failed: " + err.Error()
	} else {
		st.Delivered++
		st.LastSuccessAt = now
		st.LastLatency = lat
		if slo.MaxLatency > 0 && lat > slo.MaxLatency {
			reason = "latency SLO exceeded"
		}
	}
	if slo.BlackholeAfter > 0 && !st.LastSuccessAt.IsZero() && now.Sub(st.LastSuccessAt) > slo.BlackholeAfter {
		reason = "blackhole detected"
	}
	if reason != "" {
		if slo.Cooldown > 0 {
			st.CoolingUntil = now.Add(slo.Cooldown)
		}
		if a.Router != nil {
			ps := a.Router.Providers()
			for _, p := range ps {
				if p.ID == providerID {
					old, _ := a.Router.ProviderStats(providerID)
					old.QualityScore = old.QualityScore * .5
					old.Failures++
					a.Router.UpdateProviderStats(old)
				}
			}
		}
		if a.Incidents != nil && !st.IncidentOpen {
			st.IncidentOpen = true
			_ = a.Incidents.CreateIncident(ctx, Incident{ID: providerID + "-" + now.Format("20060102150405"), ProviderID: providerID, Severity: "major", Reason: reason, StartedAt: now})
		}
	} else {
		st.IncidentOpen = false
	}
	a.mu.Lock()
	a.States[providerID] = st
	a.mu.Unlock()
	return st, err
}
func (a *Automation) CooldownProviders() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	out := []string{}
	for id, st := range a.States {
		if st.CoolingUntil.After(now) {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

type MemoryIncidents struct {
	mu    sync.Mutex
	Items []Incident
}

func (m *MemoryIncidents) CreateIncident(ctx context.Context, i Incident) error {
	_ = ctx
	m.mu.Lock()
	m.Items = append(m.Items, i)
	m.mu.Unlock()
	return nil
}
