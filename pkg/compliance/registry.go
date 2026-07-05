package compliance

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
)

type RuleDecision struct {
	Allowed bool     `json:"allowed"`
	Reasons []string `json:"reasons,omitempty"`
	Actions []string `json:"actions,omitempty"`
}

type QuietHours struct {
	StartHour int    `json:"start_hour"`
	EndHour   int    `json:"end_hour"`
	Timezone  string `json:"timezone"`
}
type CountryRule struct {
	Country                string     `json:"country"`
	RequiresSenderApproval bool       `json:"requires_sender_approval"`
	RequiresTemplate       bool       `json:"requires_template"`
	DLTRequired            bool       `json:"dlt_required"`
	TenDLCRequired         bool       `json:"ten_dlc_required"`
	GDPR                   bool       `json:"gdpr"`
	TCPA                   bool       `json:"tcpa"`
	QuietHours             QuietHours `json:"quiet_hours,omitempty"`
	BlockedPrefixes        []string   `json:"blocked_prefixes,omitempty"`
	BlockedTerms           []string   `json:"blocked_terms,omitempty"`
}

type SenderApprovalLookup interface {
	IsSenderApproved(ctx context.Context, tenant, sender, country string) bool
}
type TemplateApprovalLookup interface {
	IsTemplateApproved(ctx context.Context, tenant, templateID, country string) bool
}

type Registry struct {
	mu        sync.RWMutex
	rules     map[string]CountryRule
	dnd       map[string]bool
	optout    map[string]bool
	senders   SenderApprovalLookup
	templates TemplateApprovalLookup
	now       func() time.Time
}

func NewRegistry() *Registry {
	return &Registry{rules: map[string]CountryRule{}, dnd: map[string]bool{}, optout: map[string]bool{}, now: time.Now}
}
func (r *Registry) SetNow(fn func() time.Time) {
	r.mu.Lock()
	if fn != nil {
		r.now = fn
	}
	r.mu.Unlock()
}
func (r *Registry) SetSenderLookup(s SenderApprovalLookup) { r.mu.Lock(); r.senders = s; r.mu.Unlock() }
func (r *Registry) SetTemplateLookup(t TemplateApprovalLookup) {
	r.mu.Lock()
	r.templates = t
	r.mu.Unlock()
}
func (r *Registry) UpsertRule(rule CountryRule) error {
	if rule.Country == "" {
		return errors.New("country required")
	}
	rule.Country = strings.ToUpper(rule.Country)
	r.mu.Lock()
	r.rules[rule.Country] = rule
	r.mu.Unlock()
	return nil
}
func (r *Registry) ListRules() []CountryRule {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]CountryRule, 0, len(r.rules))
	for _, x := range r.rules {
		out = append(out, x)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Country < out[j].Country })
	return out
}
func (r *Registry) AddDND(number string) { r.mu.Lock(); r.dnd[number] = true; r.mu.Unlock() }
func (r *Registry) AddOptOut(tenant, number string) {
	r.mu.Lock()
	r.optout[tenant+"\x00"+number] = true
	r.mu.Unlock()
}
func (r *Registry) RemoveOptOut(tenant, number string) {
	r.mu.Lock()
	delete(r.optout, tenant+"\x00"+number)
	r.mu.Unlock()
}

func (r *Registry) Evaluate(ctx context.Context, req message.SubmitRequest, country string) RuleDecision {
	r.mu.RLock()
	rule := r.rules[strings.ToUpper(country)]
	dnd := r.dnd[req.To]
	opt := r.optout[req.TenantID+"\x00"+req.To]
	senders := r.senders
	templates := r.templates
	now := r.now
	r.mu.RUnlock()
	dec := RuleDecision{Allowed: true}
	if dnd {
		dec.Allowed = false
		dec.Reasons = append(dec.Reasons, "destination is in DND list")
	}
	if opt {
		dec.Allowed = false
		dec.Reasons = append(dec.Reasons, "destination opted out")
	}
	if rule.Country == "" {
		return dec
	}
	for _, p := range rule.BlockedPrefixes {
		if strings.HasPrefix(req.To, p) {
			dec.Allowed = false
			dec.Reasons = append(dec.Reasons, "blocked destination prefix")
		}
	}
	lt := strings.ToLower(req.Text)
	for _, term := range rule.BlockedTerms {
		if strings.Contains(lt, strings.ToLower(term)) {
			dec.Allowed = false
			dec.Reasons = append(dec.Reasons, "blocked content term")
		}
	}
	if inQuietHours(now(), rule.QuietHours) {
		dec.Allowed = false
		dec.Reasons = append(dec.Reasons, "quiet hours active")
		dec.Actions = append(dec.Actions, "schedule_after_quiet_hours")
	}
	if rule.RequiresSenderApproval && (senders == nil || !senders.IsSenderApproved(ctx, req.TenantID, req.From, rule.Country)) {
		dec.Allowed = false
		dec.Reasons = append(dec.Reasons, "sender id not approved")
	}
	if rule.RequiresTemplate && (templates == nil || !templates.IsTemplateApproved(ctx, req.TenantID, req.TemplateID, rule.Country)) {
		dec.Allowed = false
		dec.Reasons = append(dec.Reasons, "template not approved")
	}
	return dec
}

func inQuietHours(t time.Time, q QuietHours) bool {
	if q.StartHour == q.EndHour {
		return false
	}
	loc := time.Local
	if q.Timezone != "" {
		if l, err := time.LoadLocation(q.Timezone); err == nil {
			loc = l
		}
	}
	h := t.In(loc).Hour()
	if q.StartHour < q.EndHour {
		return h >= q.StartHour && h < q.EndHour
	}
	return h >= q.StartHour || h < q.EndHour
}
