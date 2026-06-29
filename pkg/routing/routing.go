package routing

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/provider"
)

type Rule struct {
	ID         string
	TenantID   string
	UserID     string
	Prefix     string
	Sender     string
	ProviderID string
	Priority   int
	Weight     int
	MinQuality float64
	MaxCost    float64
	Enabled    bool
	Meta       map[string]string
}

type ProviderStats struct {
	ProviderID     string
	QualityScore   float64 // 0..100, higher is better
	DeliveryRate   float64 // 0..1
	AvgLatency     time.Duration
	CurrentTPS     int
	MaxTPS         int
	CostPerSegment float64
	Failures       int64
	Successes      int64
	UpdatedAt      time.Time
}

type Decision struct {
	Provider   provider.Profile
	Rule       Rule
	Stats      ProviderStats
	Score      float64
	Reason     string
	Candidates int
}

type Router struct {
	mu             sync.RWMutex
	providers      map[string]provider.Profile
	stats          map[string]ProviderStats
	rules          []Rule
	byTenant       map[string][]int
	byTenantSender map[string][]int
	byTenantUser   map[string][]int
	prefixes       []string
}

func New() *Router {
	return &Router{providers: map[string]provider.Profile{}, stats: map[string]ProviderStats{}, byTenant: map[string][]int{}, byTenantSender: map[string][]int{}, byTenantUser: map[string][]int{}}
}

func (r *Router) AddProvider(p provider.Profile) { _ = r.RegisterProvider(p) }
func (r *Router) RegisterProvider(p provider.Profile) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p.ID == "" {
		return errors.New("provider id is required")
	}
	if p.TPS <= 0 {
		p.TPS = 100
	}
	if p.WindowSize <= 0 {
		p.WindowSize = 16
	}
	r.providers[p.ID] = p
	if _, ok := r.stats[p.ID]; !ok {
		r.stats[p.ID] = ProviderStats{ProviderID: p.ID, QualityScore: 80, DeliveryRate: 0.98, MaxTPS: p.TPS, UpdatedAt: time.Now()}
	}
	return nil
}
func (r *Router) RemoveProvider(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.providers, id)
	delete(r.stats, id)
}
func (r *Router) UpdateProviderStats(st ProviderStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st.ProviderID == "" {
		return
	}
	if st.QualityScore == 0 {
		old := r.stats[st.ProviderID]
		if old.QualityScore != 0 {
			st.QualityScore = old.QualityScore
		}
	}
	if st.MaxTPS == 0 {
		if p, ok := r.providers[st.ProviderID]; ok {
			st.MaxTPS = p.TPS
		}
	}
	st.UpdatedAt = time.Now()
	r.stats[st.ProviderID] = st
}
func (r *Router) ProviderStats(id string) (ProviderStats, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, ok := r.stats[id]
	return st, ok
}

func (r *Router) AddRule(rule Rule) { _ = r.RegisterRule(rule) }
func (r *Router) RegisterRule(rule Rule) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rule.ID == "" {
		rule.ID = "rule-" + time.Now().Format("20060102150405.000000000")
	}
	if rule.ProviderID == "" {
		return errors.New("rule provider id is required")
	}
	if !rule.Enabled {
		rule.Enabled = true
	}
	rule.Sender = protocol.AutoDetectAddress(protocol.Address{Addr: rule.Sender}).Addr
	rule.Prefix = protocol.AutoDetectAddress(protocol.Address{Addr: rule.Prefix}).Addr
	r.rules = append(r.rules, rule)
	r.rebuildLocked()
	return nil
}
func (r *Router) rebuildLocked() {
	sort.SliceStable(r.rules, func(i, j int) bool {
		if r.rules[i].Priority == r.rules[j].Priority {
			return r.rules[i].ID < r.rules[j].ID
		}
		return r.rules[i].Priority < r.rules[j].Priority
	})
	r.byTenant = map[string][]int{}
	r.byTenantSender = map[string][]int{}
	r.byTenantUser = map[string][]int{}
	pfx := map[string]struct{}{}
	for i, rule := range r.rules {
		if rule.TenantID != "" {
			r.byTenant[rule.TenantID] = append(r.byTenant[rule.TenantID], i)
		}
		if rule.TenantID != "" && rule.Sender != "" {
			r.byTenantSender[rule.TenantID+"\x00"+rule.Sender] = append(r.byTenantSender[rule.TenantID+"\x00"+rule.Sender], i)
		}
		if rule.TenantID != "" && rule.UserID != "" {
			r.byTenantUser[rule.TenantID+"\x00"+rule.UserID] = append(r.byTenantUser[rule.TenantID+"\x00"+rule.UserID], i)
		}
		if rule.Prefix != "" {
			pfx[rule.Prefix] = struct{}{}
		}
	}
	r.prefixes = r.prefixes[:0]
	for k := range pfx {
		r.prefixes = append(r.prefixes, k)
	}
	sort.Slice(r.prefixes, func(i, j int) bool { return len(r.prefixes[i]) > len(r.prefixes[j]) })
}
func (r *Router) Providers() []provider.Profile {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]provider.Profile, 0, len(r.providers))
	for _, p := range r.providers {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (r *Router) Rules() []Rule {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Rule, len(r.rules))
	copy(out, r.rules)
	return out
}

func (r *Router) Route(ctx context.Context, req message.SubmitRequest) (Decision, error) {
	req = normalizeSubmitRequest(req)
	r.mu.RLock()
	defer r.mu.RUnlock()
	idxs := r.candidateIndexesLocked(req)
	best := Decision{Score: -1}
	seen := map[string]bool{}
	for _, idx := range idxs {
		if idx < 0 || idx >= len(r.rules) {
			continue
		}
		rule := r.rules[idx]
		if !rule.Enabled || !ruleMatches(rule, req) {
			continue
		}
		if seen[rule.ID] {
			continue
		}
		seen[rule.ID] = true
		p, ok := r.providers[rule.ProviderID]
		if !ok {
			continue
		}
		st := r.stats[p.ID]
		if rule.MinQuality > 0 && st.QualityScore < rule.MinQuality {
			continue
		}
		if rule.MaxCost > 0 && st.CostPerSegment > rule.MaxCost {
			continue
		}
		sc := score(rule, p, st, req)
		if sc > best.Score {
			best = Decision{Provider: p, Rule: rule, Stats: st, Score: sc, Reason: "best indexed route", Candidates: len(idxs)}
		}
	}
	if best.Score >= 0 {
		return best, nil
	}
	for _, p := range r.providers {
		st := r.stats[p.ID]
		sc := score(Rule{Priority: 9999, Weight: 1}, p, st, req)
		if sc > best.Score {
			best = Decision{Provider: p, Stats: st, Score: sc, Reason: "quality fallback", Candidates: len(r.providers)}
		}
	}
	if best.Score >= 0 {
		return best, nil
	}
	return Decision{}, errors.New("no provider available")
}

func (r *Router) candidateIndexesLocked(req message.SubmitRequest) []int {
	out := []int{}
	add := func(xs []int) { out = append(out, xs...) }
	userID := req.Meta["user_id"]
	if req.TenantID != "" && userID != "" {
		add(r.byTenantUser[req.TenantID+"\x00"+userID])
	}
	if req.TenantID != "" && req.From != "" {
		add(r.byTenantSender[req.TenantID+"\x00"+req.From])
	}
	if req.TenantID != "" {
		add(r.byTenant[req.TenantID])
	}
	for _, p := range r.prefixes {
		if strings.HasPrefix(req.To, p) {
			for i, rule := range r.rules {
				if rule.Prefix == p {
					out = append(out, i)
				}
			}
			break
		}
	}
	if len(out) == 0 {
		for i := range r.rules {
			out = append(out, i)
		}
	}
	return out
}
func ruleMatches(rule Rule, req message.SubmitRequest) bool {
	if rule.TenantID != "" && rule.TenantID != req.TenantID {
		return false
	}
	if rule.UserID != "" && req.Meta["user_id"] != rule.UserID {
		return false
	}
	if rule.Sender != "" && rule.Sender != req.From {
		return false
	}
	if rule.Prefix != "" && !strings.HasPrefix(req.To, rule.Prefix) {
		return false
	}
	return true
}
func score(rule Rule, p provider.Profile, st ProviderStats, req message.SubmitRequest) float64 {
	q := st.QualityScore
	if q == 0 {
		q = 75
	}
	delivery := st.DeliveryRate * 100
	if delivery == 0 {
		delivery = 80
	}
	latencyPenalty := float64(st.AvgLatency.Milliseconds()) / 100.0
	tpsPenalty := 0.0
	if st.MaxTPS > 0 {
		tpsPenalty = float64(st.CurrentTPS) / float64(st.MaxTPS) * 10
	}
	costPenalty := st.CostPerSegment * 100
	priorityBoost := float64(10000-rule.Priority) / 100
	weightBoost := float64(rule.Weight)
	return q*1.7 + delivery*1.1 + priorityBoost + weightBoost - latencyPenalty - tpsPenalty - costPenalty - float64(st.Failures)*0.01
}

func normalizeSubmitRequest(req message.SubmitRequest) message.SubmitRequest {
	req.From = protocol.AutoDetectAddress(protocol.Address{Addr: req.From}).Addr
	req.To = protocol.AutoDetectAddress(protocol.Address{Addr: req.To}).Addr
	return req
}
