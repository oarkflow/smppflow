package alerts

import (
	"context"
	"sort"
	"sync"
	"time"
)

type Severity string

const (
	Info     Severity = "info"
	Warning  Severity = "warning"
	Critical Severity = "critical"
)

type Signal struct {
	Name   string            `json:"name"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels,omitempty"`
	At     time.Time         `json:"at"`
}
type Rule struct {
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	Signal   string            `json:"signal"`
	Above    float64           `json:"above,omitempty"`
	Below    float64           `json:"below,omitempty"`
	For      time.Duration     `json:"for,omitempty"`
	Severity Severity          `json:"severity"`
	Labels   map[string]string `json:"labels,omitempty"`
}
type Incident struct {
	ID       string    `json:"id"`
	RuleID   string    `json:"rule_id"`
	RuleName string    `json:"rule_name"`
	Severity Severity  `json:"severity"`
	Signal   Signal    `json:"signal"`
	OpenedAt time.Time `json:"opened_at"`
	ClosedAt time.Time `json:"closed_at,omitempty"`
	Status   string    `json:"status"`
}
type Engine struct {
	mu        sync.Mutex
	rules     []Rule
	open      map[string]Incident
	firstSeen map[string]time.Time
}

func New() *Engine { return &Engine{open: map[string]Incident{}, firstSeen: map[string]time.Time{}} }
func (e *Engine) AddRule(r Rule) {
	if r.Severity == "" {
		r.Severity = Warning
	}
	e.mu.Lock()
	e.rules = append(e.rules, r)
	e.mu.Unlock()
}
func (e *Engine) Evaluate(ctx context.Context, s Signal) []Incident {
	_ = ctx
	if s.At.IsZero() {
		s.At = time.Now()
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	fired := []Incident{}
	for _, r := range e.rules {
		if r.Signal != s.Name {
			continue
		}
		key := r.ID + ":" + labelKey(s.Labels)
		match := matches(r, s)
		if match {
			if e.firstSeen[key].IsZero() {
				e.firstSeen[key] = s.At
			}
			if r.For == 0 || s.At.Sub(e.firstSeen[key]) >= r.For {
				inc := e.open[key]
				if inc.ID == "" {
					inc = Incident{ID: "inc-" + s.At.Format("20060102150405.000000000"), RuleID: r.ID, RuleName: r.Name, Severity: r.Severity, OpenedAt: s.At, Status: "open"}
				}
				inc.Signal = s
				e.open[key] = inc
				fired = append(fired, inc)
			}
		} else {
			delete(e.firstSeen, key)
			if inc := e.open[key]; inc.ID != "" {
				inc.ClosedAt = s.At
				inc.Status = "closed"
				delete(e.open, key)
			}
		}
	}
	sort.Slice(fired, func(i, j int) bool { return fired[i].Severity > fired[j].Severity })
	return fired
}
func (e *Engine) Open() []Incident {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := []Incident{}
	for _, i := range e.open {
		out = append(out, i)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OpenedAt.Before(out[j].OpenedAt) })
	return out
}
func matches(r Rule, s Signal) bool {
	ok := false
	if r.Above != 0 && s.Value > r.Above {
		ok = true
	}
	if r.Below != 0 && s.Value < r.Below {
		ok = true
	}
	if r.Above == 0 && r.Below == 0 {
		ok = s.Value > 0
	}
	for k, v := range r.Labels {
		if s.Labels[k] != v {
			return false
		}
	}
	return ok
}
func labelKey(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := ""
	for _, k := range keys {
		out += k + "=" + m[k] + ";"
	}
	return out
}
