package routing

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/provider"
	"github.com/oarkflow/smppflow/pkg/ratelimit"
)

type Strategy string

const (
	StrategyBestQuality Strategy = "best_quality"
	StrategyLeastCost   Strategy = "least_cost"
	StrategyFastest     Strategy = "fastest"
	StrategyWeighted    Strategy = "weighted"
	StrategyBalanced    Strategy = "balanced"
)

type RoutingObjective string

const (
	ObjectiveHighestDelivery RoutingObjective = "highest_delivery"
	ObjectiveLowestCost      RoutingObjective = "lowest_cost"
	ObjectiveLowestLatency   RoutingObjective = "lowest_latency"
	ObjectiveBalanced        RoutingObjective = "balanced"
	ObjectiveFailoverOnly    RoutingObjective = "failover_only"
)

type ProviderState string

const (
	ProviderActive      ProviderState = "active"
	ProviderMaintenance ProviderState = "maintenance"
	ProviderDown        ProviderState = "down"
	ProviderDraining    ProviderState = "draining"
	ProviderSuspended   ProviderState = "suspended"
)

type RejectionReason string

const (
	RejectRuleDisabled     RejectionReason = "rule_disabled"
	RejectRuleInactive     RejectionReason = "rule_inactive"
	RejectRuleMismatch     RejectionReason = "rule_mismatch"
	RejectTenantNotAllowed RejectionReason = "tenant_not_allowed"
	RejectProviderMissing  RejectionReason = "provider_missing"
	RejectProviderDown     RejectionReason = "provider_down"
	RejectMaintenance      RejectionReason = "provider_maintenance"
	RejectSuspended        RejectionReason = "provider_suspended"
	RejectNoBind           RejectionReason = "no_active_bind"
	RejectDraining         RejectionReason = "provider_draining"
	RejectTPSExceeded      RejectionReason = "tps_exceeded"
	RejectWindowFull       RejectionReason = "window_full"
	RejectCapability       RejectionReason = "capability_not_supported"
	RejectQuality          RejectionReason = "quality_below_minimum"
	RejectCost             RejectionReason = "cost_above_maximum"
	RejectCooldown         RejectionReason = "provider_cooldown"
	RejectRateLimit        RejectionReason = "rate_limited"
)

type Rule struct {
	ID              string
	Version         int64
	Name            string
	TenantID        string
	UserID          string
	CampaignID      string
	Prefix          string
	Prefixes        []string
	Sender          string
	Country         string
	Operator        string
	MCC             string
	MNC             string
	MessageTypes    []string
	ProviderID      string
	Priority        int
	Weight          int
	Objective       RoutingObjective
	MinQuality      float64
	MaxCost         float64
	MinDeliveryRate float64
	MaxLatency      time.Duration
	ActiveFrom      time.Time
	ActiveTo        time.Time
	// Enabled is retained for source compatibility. New code should use Disabled for explicit off state.
	Enabled  bool
	Disabled bool
	Meta     map[string]string
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

type ScoreBreakdown struct {
	Quality  float64 `json:"quality"`
	Delivery float64 `json:"delivery"`
	Latency  float64 `json:"latency"`
	Cost     float64 `json:"cost"`
	Capacity float64 `json:"capacity"`
	Priority float64 `json:"priority"`
	Weight   float64 `json:"weight"`
	Penalty  float64 `json:"penalty"`
	Total    float64 `json:"total"`
}

type CapacitySnapshot struct {
	ProviderID string `json:"provider_id"`
	CurrentTPS int    `json:"current_tps"`
	MaxTPS     int    `json:"max_tps"`
	WindowUsed int    `json:"window_used"`
	WindowSize int    `json:"window_size"`
	Healthy    bool   `json:"healthy"`
	Draining   bool   `json:"draining"`
	State      string `json:"state"`
}

type CandidateExplanation struct {
	ProviderID string            `json:"provider_id,omitempty"`
	RuleID     string            `json:"rule_id,omitempty"`
	Eligible   bool              `json:"eligible"`
	Priority   int               `json:"priority,omitempty"`
	RejectedBy []RejectionReason `json:"rejected_by,omitempty"`
	Score      ScoreBreakdown    `json:"score,omitempty"`
	Capacity   CapacitySnapshot  `json:"capacity,omitempty"`
}

type RouteDecisionSnapshot struct {
	ID             string                 `json:"id"`
	MessageID      string                 `json:"message_id,omitempty"`
	RouteVersion   int64                  `json:"route_version"`
	RuleID         string                 `json:"rule_id,omitempty"`
	ProviderID     string                 `json:"provider_id"`
	TenantID       string                 `json:"tenant_id,omitempty"`
	Objective      RoutingObjective       `json:"objective"`
	Strategy       Strategy               `json:"strategy"`
	Score          float64                `json:"score"`
	ScoreBreakdown ScoreBreakdown         `json:"score_breakdown"`
	Rejected       []CandidateExplanation `json:"rejected,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
}

type Decision struct {
	Provider       provider.Profile
	Rule           Rule
	Stats          ProviderStats
	Score          float64
	Reason         string
	Candidates     int
	ScoreBreakdown ScoreBreakdown
	Rejected       []CandidateExplanation
	Snapshot       RouteDecisionSnapshot
}

type RouterOptions struct {
	Strategy             Strategy
	StickyBy             string
	Cooldown             time.Duration
	StrictTenantFallback bool
}

type SessionSnapshot struct {
	ProviderID string
	TenantID   string
	SystemID   string
	BindMode   string
	Healthy    bool
	Draining   bool
	WindowUsed int
	WindowSize int
	State      string
}

type Router struct {
	mu             sync.RWMutex
	providers      map[string]provider.Profile
	states         map[string]ProviderState
	stats          map[string]ProviderStats
	sessions       map[string]SessionSnapshot
	rules          []Rule
	byTenant       map[string][]int
	byTenantSender map[string][]int
	byTenantUser   map[string][]int
	byPrefix       map[string][]int
	prefixes       []string
	opts           RouterOptions
	cooldown       map[string]time.Time
	sticky         map[string]string
	limiter        *ratelimit.Limiter
	routeVersion   int64
	snapshots      []RouteDecisionSnapshot
}

func New() *Router {
	return &Router{
		providers: map[string]provider.Profile{}, states: map[string]ProviderState{}, stats: map[string]ProviderStats{}, sessions: map[string]SessionSnapshot{},
		byTenant: map[string][]int{}, byTenantSender: map[string][]int{}, byTenantUser: map[string][]int{}, byPrefix: map[string][]int{},
		opts: RouterOptions{Strategy: StrategyBalanced, StrictTenantFallback: true}, cooldown: map[string]time.Time{}, sticky: map[string]string{},
	}
}

func (r *Router) SetOptions(opts RouterOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if opts.Strategy == "" {
		opts.Strategy = StrategyBalanced
	}
	// Preserve caller choice. StrictTenantFallback defaults to true in New(); use
	// AllowGlobalRulesForTenants for explicit shared-route behavior.
	r.opts = opts
}
func (r *Router) SetStrategy(s Strategy) {
	r.mu.Lock()
	if s == "" {
		s = StrategyBalanced
	}
	r.opts.Strategy = s
	r.mu.Unlock()
}
func (r *Router) SetStickyBy(stickyBy string)     { r.mu.Lock(); r.opts.StickyBy = stickyBy; r.mu.Unlock() }
func (r *Router) SetLimiter(l *ratelimit.Limiter) { r.mu.Lock(); r.limiter = l; r.mu.Unlock() }
func (r *Router) CooldownProvider(id string, d ...time.Duration) {
	r.mu.Lock()
	dur := r.opts.Cooldown
	if len(d) > 0 {
		dur = d[0]
	}
	if dur <= 0 {
		dur = time.Minute
	}
	r.cooldown[id] = time.Now().Add(dur)
	r.mu.Unlock()
}
func (r *Router) SetProviderState(id string, st ProviderState) {
	r.mu.Lock()
	if st == "" {
		st = ProviderActive
	}
	r.states[id] = st
	r.mu.Unlock()
}
func (r *Router) UpdateSessionSnapshot(s SessionSnapshot) {
	if s.ProviderID == "" {
		return
	}
	r.mu.Lock()
	r.sessions[s.ProviderID] = s
	if key := sessionSnapshotKey(s.ProviderID, s.TenantID, s.SystemID, s.BindMode); key != s.ProviderID {
		r.sessions[key] = s
	}
	r.mu.Unlock()
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
	if _, ok := r.states[p.ID]; !ok {
		r.states[p.ID] = stateFromProfile(p)
	}
	if _, ok := r.stats[p.ID]; !ok {
		r.stats[p.ID] = ProviderStats{ProviderID: p.ID, QualityScore: 80, DeliveryRate: 0.98, MaxTPS: p.TPS, UpdatedAt: time.Now()}
	}
	r.routeVersion++
	return nil
}
func (r *Router) RemoveProvider(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.providers, id)
	delete(r.stats, id)
	delete(r.states, id)
	delete(r.sessions, id)
	r.routeVersion++
}
func (r *Router) UpdateProviderStats(st ProviderStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updateProviderStatsLocked(st)
}
func (r *Router) updateProviderStatsLocked(st ProviderStats) {
	if st.ProviderID == "" {
		return
	}
	old := r.stats[st.ProviderID]
	if st.QualityScore == 0 && old.QualityScore != 0 {
		st.QualityScore = old.QualityScore
	}
	if st.DeliveryRate == 0 && old.DeliveryRate != 0 {
		st.DeliveryRate = old.DeliveryRate
	}
	if st.MaxTPS == 0 {
		if p, ok := r.providers[st.ProviderID]; ok {
			st.MaxTPS = p.TPS
		} else {
			st.MaxTPS = old.MaxTPS
		}
	}
	if st.CostPerSegment == 0 && old.CostPerSegment != 0 {
		st.CostPerSegment = old.CostPerSegment
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
	if _, ok := r.providers[rule.ProviderID]; !ok {
		return errors.New("rule references missing provider: " + rule.ProviderID)
	}
	if rule.Priority == 0 {
		rule.Priority = 1000
	}
	if rule.Weight <= 0 {
		rule.Weight = 1
	}
	if rule.Objective == "" {
		rule.Objective = ObjectiveBalanced
	}
	rule.Enabled = !rule.Disabled
	rule.Sender = protocol.AutoDetectAddress(protocol.Address{Addr: rule.Sender}).Addr
	rule.Prefix = protocol.AutoDetectAddress(protocol.Address{Addr: rule.Prefix}).Addr
	for i := range rule.Prefixes {
		rule.Prefixes[i] = protocol.AutoDetectAddress(protocol.Address{Addr: rule.Prefixes[i]}).Addr
	}
	r.rules = append(r.rules, rule)
	r.routeVersion++
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
	r.byPrefix = map[string][]int{}
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
		allp := rulePrefixes(rule)
		for _, p := range allp {
			if p != "" {
				r.byPrefix[p] = append(r.byPrefix[p], i)
				pfx[p] = struct{}{}
			}
		}
	}
	r.prefixes = r.prefixes[:0]
	for k := range pfx {
		r.prefixes = append(r.prefixes, k)
	}
	sort.Slice(r.prefixes, func(i, j int) bool {
		if len(r.prefixes[i]) == len(r.prefixes[j]) {
			return r.prefixes[i] < r.prefixes[j]
		}
		return len(r.prefixes[i]) > len(r.prefixes[j])
	})
	trie := NewPrefixTrie()
	for p, ids := range r.byPrefix {
		for _, id := range ids {
			trie.Insert(p, id)
		}
	}
	extra(r).trie = trie
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
func (r *Router) Snapshots() []RouteDecisionSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RouteDecisionSnapshot, len(r.snapshots))
	copy(out, r.snapshots)
	return out
}
func (r *Router) AttachSnapshotMessageID(snapshotID string, msgID message.ID) {
	if snapshotID == "" || msgID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.snapshots {
		if r.snapshots[i].ID == snapshotID {
			r.snapshots[i].MessageID = string(msgID)
			return
		}
	}
}

func (r *Router) Route(ctx context.Context, req message.SubmitRequest) (Decision, error) {
	return r.routeProduction(ctx, req, PlanOptions{Mode: RouteInitial})
}

type candidatePlan struct{ Decision }

func (r *Router) planLocked(req message.SubmitRequest) ([]Decision, []CandidateExplanation) {
	idxs := r.candidateIndexesLocked(req)
	eligible := []Decision{}
	rejected := []CandidateExplanation{}
	seen := map[string]bool{}
	for _, idx := range idxs {
		if idx < 0 || idx >= len(r.rules) {
			continue
		}
		rule := r.rules[idx]
		key := rule.ID + "\x00" + rule.ProviderID
		if seen[key] {
			continue
		}
		seen[key] = true
		exp := CandidateExplanation{RuleID: rule.ID, ProviderID: rule.ProviderID, Priority: rule.Priority}
		if reasons := r.rejectionReasonsLocked(rule, req); len(reasons) > 0 {
			exp.RejectedBy = reasons
			rejected = append(rejected, exp)
			continue
		}
		p, ok := r.providers[rule.ProviderID]
		if !ok {
			exp.RejectedBy = []RejectionReason{RejectProviderMissing}
			rejected = append(rejected, exp)
			continue
		}
		st := r.stats[p.ID]
		if st.ProviderID == "" {
			st = ProviderStats{ProviderID: p.ID, QualityScore: 80, DeliveryRate: 0.98, MaxTPS: p.TPS}
		}
		reasons := r.providerRejectionsLocked(rule, p, st, req)
		if len(reasons) > 0 {
			exp.RejectedBy = reasons
			exp.Capacity = capacityFrom(st, r.sessionForProviderLocked(req, p.ID))
			rejected = append(rejected, exp)
			continue
		}
		sb := scoreBreakdown(rule, p, st, req, objectiveFor(rule, r.opts), r.opts.Strategy)
		exp.Eligible = true
		exp.Score = sb
		exp.Capacity = capacityFrom(st, r.sessionForProviderLocked(req, p.ID))
		eligible = append(eligible, Decision{Provider: p, Rule: rule, Stats: st, Score: sb.Total, ScoreBreakdown: sb, Reason: "eligible"})
	}
	if len(eligible) == 0 && req.TenantID == "" && len(idxs) == 0 {
		// Single-tenant/development fallback only when no tenant isolation is requested.
		for _, id := range sortedProviderIDs(r.providers) {
			p := r.providers[id]
			st := r.stats[id]
			rule := Rule{ID: "fallback:" + id, ProviderID: id, Priority: 9999, Weight: 1, Objective: ObjectiveBalanced, Enabled: true}
			if reasons := r.providerRejectionsLocked(rule, p, st, req); len(reasons) > 0 {
				rejected = append(rejected, CandidateExplanation{ProviderID: id, RuleID: rule.ID, RejectedBy: reasons})
				continue
			}
			sb := scoreBreakdown(rule, p, st, req, ObjectiveBalanced, r.opts.Strategy)
			eligible = append(eligible, Decision{Provider: p, Rule: rule, Stats: st, Score: sb.Total, ScoreBreakdown: sb, Reason: "development fallback"})
		}
	}
	return eligible, rejected
}

func (r *Router) selectLocked(req message.SubmitRequest, candidates []Decision) Decision {
	minPriority := candidates[0].Rule.Priority
	for _, c := range candidates {
		if c.Rule.Priority < minPriority {
			minPriority = c.Rule.Priority
		}
	}
	tier := make([]Decision, 0, len(candidates))
	for _, c := range candidates {
		if c.Rule.Priority == minPriority {
			tier = append(tier, c)
		}
	}
	if key := stickyKey(req, r.opts.StickyBy); key != "" {
		if pid := r.sticky[key]; pid != "" {
			for _, c := range tier {
				if c.Provider.ID == pid {
					c.Reason = "sticky route"
					return c
				}
			}
		}
	}
	strat := r.opts.Strategy
	if strat == "" {
		strat = StrategyBalanced
	}
	if strat == StrategyWeighted {
		return weightedPick(req, tier)
	}
	sort.SliceStable(tier, func(i, j int) bool { return decisionLess(tier[i], tier[j], strat) })
	return tier[0]
}

func (r *Router) reserveLocked(ctx context.Context, req message.SubmitRequest, d Decision) []RejectionReason {
	keys := []string{}
	if req.TenantID != "" {
		keys = append(keys, "tenant:"+req.TenantID)
	}
	if d.Provider.ID != "" {
		keys = append(keys, "provider:"+d.Provider.ID)
	}
	if d.Rule.ID != "" {
		keys = append(keys, "route:"+d.Rule.ID)
	}
	for _, key := range keys {
		rd, err := r.limiter.Allow(ctx, key, 1)
		if err != nil {
			continue
		}
		if !rd.Allowed {
			return []RejectionReason{RejectRateLimit}
		}
	}
	return nil
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
	if tr := extra(r).trie; tr != nil {
		add(tr.MatchAll(req.To))
	} else {
		for _, p := range r.prefixes {
			if strings.HasPrefix(req.To, p) {
				add(r.byPrefix[p])
			}
		}
	}
	if len(out) == 0 && req.TenantID == "" {
		for i := range r.rules {
			out = append(out, i)
		}
	}
	return uniqueInts(out)
}

func (r *Router) rejectionReasonsLocked(rule Rule, req message.SubmitRequest) []RejectionReason {
	now := time.Now()
	rs := []RejectionReason{}
	if rule.Disabled {
		rs = append(rs, RejectRuleDisabled)
	}
	if !rule.ActiveFrom.IsZero() && now.Before(rule.ActiveFrom) {
		rs = append(rs, RejectRuleInactive)
	}
	if !rule.ActiveTo.IsZero() && now.After(rule.ActiveTo) {
		rs = append(rs, RejectRuleInactive)
	}
	if !ruleMatches(rule, req) {
		rs = append(rs, RejectRuleMismatch)
	}
	return rs
}

func (r *Router) providerRejectionsLocked(rule Rule, p provider.Profile, st ProviderStats, req message.SubmitRequest) []RejectionReason {
	rs := []RejectionReason{}
	state := r.states[p.ID]
	if state == "" {
		state = stateFromProfile(p)
	}
	switch state {
	case ProviderDown:
		rs = append(rs, RejectProviderDown)
	case ProviderMaintenance:
		rs = append(rs, RejectMaintenance)
	case ProviderDraining:
		rs = append(rs, RejectDraining)
	case ProviderSuspended:
		rs = append(rs, RejectSuspended)
	}
	if until, ok := r.cooldown[p.ID]; ok && time.Now().Before(until) {
		rs = append(rs, RejectCooldown)
	}
	if rule.MinQuality > 0 && st.QualityScore < rule.MinQuality {
		rs = append(rs, RejectQuality)
	}
	if rule.MinDeliveryRate > 0 && st.DeliveryRate < rule.MinDeliveryRate {
		rs = append(rs, RejectQuality)
	}
	if rule.MaxLatency > 0 && st.AvgLatency > rule.MaxLatency {
		rs = append(rs, RejectQuality)
	}
	if rule.MaxCost > 0 && st.CostPerSegment > rule.MaxCost {
		rs = append(rs, RejectCost)
	}
	maxTPS := st.MaxTPS
	if maxTPS <= 0 {
		maxTPS = p.TPS
	}
	if maxTPS > 0 && st.CurrentTPS >= maxTPS {
		rs = append(rs, RejectTPSExceeded)
	}
	if req.Meta != nil {
		if truthy(req.Meta["require_query_sm"]) && !p.SupportsQuery {
			rs = append(rs, RejectCapability)
		}
		if truthy(req.Meta["require_data_sm"]) && !p.SupportsDataSM {
			rs = append(rs, RejectCapability)
		}
		if truthy(req.Meta["require_submit_multi"]) && !p.SupportsSubmitMulti {
			rs = append(rs, RejectCapability)
		}
	}
	if s, ok := r.sessionForProviderOKLocked(req, p.ID); ok {
		if s.Draining {
			rs = append(rs, RejectDraining)
		}
		if !s.Healthy && s.State != "" {
			rs = append(rs, RejectNoBind)
		}
		if s.WindowSize > 0 && s.WindowUsed >= s.WindowSize {
			rs = append(rs, RejectWindowFull)
		}
	}
	return uniqueReasons(rs)
}

func sessionSnapshotKey(providerID, tenantID, systemID, bindMode string) string {
	if systemID == "" && tenantID == "" && bindMode == "" {
		return providerID
	}
	return providerID + "|" + tenantID + "|" + systemID + "|" + bindMode
}
func (r *Router) sessionForProviderLocked(req message.SubmitRequest, providerID string) SessionSnapshot {
	s, _ := r.sessionForProviderOKLocked(req, providerID)
	return s
}
func (r *Router) sessionForDecisionLocked(req message.SubmitRequest, d Decision) SessionSnapshot {
	return r.sessionForProviderLocked(req, d.Provider.ID)
}
func (r *Router) sessionForProviderOKLocked(req message.SubmitRequest, providerID string) (SessionSnapshot, bool) {
	if req.Meta != nil {
		if k := req.Meta["bind_key"]; k != "" {
			if s, ok := r.sessions[k]; ok {
				return s, true
			}
		}
		if sid := req.Meta["system_id"]; sid != "" {
			k := sessionSnapshotKey(providerID, req.TenantID, sid, req.Meta["bind_mode"])
			if s, ok := r.sessions[k]; ok {
				return s, true
			}
		}
	}
	s, ok := r.sessions[providerID]
	return s, ok
}

func ruleMatches(rule Rule, req message.SubmitRequest) bool {
	if rule.TenantID != "" && rule.TenantID != req.TenantID {
		return false
	}
	if rule.UserID != "" && req.Meta["user_id"] != rule.UserID {
		return false
	}
	if rule.CampaignID != "" && rule.CampaignID != req.CampaignID {
		return false
	}
	if rule.Sender != "" && rule.Sender != req.From {
		return false
	}
	if len(rule.MessageTypes) > 0 && !contains(rule.MessageTypes, req.Meta["message_type"]) {
		return false
	}
	if rule.Country != "" && !eqFold(rule.Country, req.Meta["country"]) {
		return false
	}
	if rule.Operator != "" && !eqFold(rule.Operator, req.Meta["operator"]) {
		return false
	}
	if rule.MCC != "" && !eqFold(rule.MCC, req.Meta["mcc"]) {
		return false
	}
	if rule.MNC != "" && !eqFold(rule.MNC, req.Meta["mnc"]) {
		return false
	}
	pfxs := rulePrefixes(rule)
	if len(pfxs) > 0 {
		ok := false
		for _, p := range pfxs {
			if p == "" || strings.HasPrefix(req.To, p) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func score(rule Rule, p provider.Profile, st ProviderStats, req message.SubmitRequest) float64 {
	return scoreBreakdown(rule, p, st, req, objectiveFor(rule, RouterOptions{Strategy: StrategyBalanced}), StrategyBalanced).Total
}
func scoreBreakdown(rule Rule, p provider.Profile, st ProviderStats, req message.SubmitRequest, obj RoutingObjective, strat Strategy) ScoreBreakdown {
	q := st.QualityScore
	if q == 0 {
		q = 75
	}
	delivery := st.DeliveryRate * 100
	if delivery == 0 {
		delivery = 80
	}
	latencyMs := float64(st.AvgLatency.Milliseconds())
	latencyScore := 100.0
	if latencyMs > 0 {
		latencyScore = 10000.0 / (100.0 + latencyMs)
	}
	costScore := 100.0 - st.CostPerSegment*10000
	if costScore < 0 {
		costScore = 0
	}
	maxTPS := st.MaxTPS
	if maxTPS <= 0 {
		maxTPS = p.TPS
	}
	capacity := 100.0
	if maxTPS > 0 {
		capacity = (1 - float64(st.CurrentTPS)/float64(maxTPS)) * 100
		if capacity < 0 {
			capacity = 0
		}
	}
	weight := float64(rule.Weight)
	if weight <= 0 {
		weight = 1
	}
	priority := 10000.0 / float64(max(1, rule.Priority))
	failurePenalty := float64(st.Failures) * 0.01
	var total float64
	switch strat {
	case StrategyLeastCost:
		total = costScore*3 + q*.6 + delivery*.8 + capacity*.5 + weight - failurePenalty
	case StrategyFastest:
		total = latencyScore*3 + q*.7 + delivery*.8 + capacity*.5 + weight - failurePenalty
	case StrategyBestQuality:
		total = q*2.5 + delivery*2 + latencyScore*.5 + capacity*.5 + weight - failurePenalty
	default:
		switch obj {
		case ObjectiveLowestCost:
			total = costScore*3 + q*.6 + delivery*.8 + capacity*.5 + weight - failurePenalty
		case ObjectiveLowestLatency:
			total = latencyScore*3 + q*.7 + delivery*.8 + capacity*.5 + weight - failurePenalty
		case ObjectiveHighestDelivery:
			total = q*2 + delivery*2.5 + latencyScore*.5 + capacity*.5 + weight - failurePenalty
		default:
			total = q*1.4 + delivery*1.2 + latencyScore*.8 + costScore*.9 + capacity*.8 + weight + priority*.01 - failurePenalty
		}
	}
	return ScoreBreakdown{Quality: q, Delivery: delivery, Latency: latencyScore, Cost: costScore, Capacity: capacity, Priority: priority, Weight: weight, Penalty: failurePenalty, Total: total}
}

func normalizeSubmitRequest(req message.SubmitRequest) message.SubmitRequest {
	if req.Meta == nil {
		req.Meta = map[string]string{}
	}
	req.From = protocol.AutoDetectAddress(protocol.Address{Addr: req.From}).Addr
	req.To = protocol.AutoDetectAddress(protocol.Address{Addr: req.To}).Addr
	return req
}
func rulePrefixes(rule Rule) []string {
	out := []string{}
	if rule.Prefix != "" {
		out = append(out, rule.Prefix)
	}
	out = append(out, rule.Prefixes...)
	return out
}
func objectiveFor(rule Rule, opts RouterOptions) RoutingObjective {
	if rule.Objective != "" {
		return rule.Objective
	}
	switch opts.Strategy {
	case StrategyLeastCost:
		return ObjectiveLowestCost
	case StrategyFastest:
		return ObjectiveLowestLatency
	case StrategyBestQuality:
		return ObjectiveHighestDelivery
	default:
		return ObjectiveBalanced
	}
}
func decisionLess(a, b Decision, s Strategy) bool {
	if a.Rule.Priority != b.Rule.Priority {
		return a.Rule.Priority < b.Rule.Priority
	}
	switch s {
	case StrategyLeastCost:
		if a.Stats.CostPerSegment != b.Stats.CostPerSegment {
			return a.Stats.CostPerSegment < b.Stats.CostPerSegment
		}
	case StrategyFastest:
		if a.Stats.AvgLatency != b.Stats.AvgLatency {
			return a.Stats.AvgLatency < b.Stats.AvgLatency
		}
	case StrategyBestQuality:
		if a.Stats.QualityScore != b.Stats.QualityScore {
			return a.Stats.QualityScore > b.Stats.QualityScore
		}
	}
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	if a.Rule.Weight != b.Rule.Weight {
		return a.Rule.Weight > b.Rule.Weight
	}
	return a.Provider.ID < b.Provider.ID
}
func weightedPick(req message.SubmitRequest, xs []Decision) Decision {
	sort.SliceStable(xs, func(i, j int) bool { return xs[i].Provider.ID < xs[j].Provider.ID })
	total := 0
	for _, x := range xs {
		total += max(1, x.Rule.Weight)
	}
	if total <= 0 {
		return xs[0]
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(req.TenantID + "|" + req.CampaignID + "|" + req.To + "|" + req.From))
	n := int(h.Sum32() % uint32(total))
	cur := 0
	for _, x := range xs {
		cur += max(1, x.Rule.Weight)
		if n < cur {
			return x
		}
	}
	return xs[0]
}
func stickyKey(req message.SubmitRequest, by string) string {
	switch strings.ToLower(by) {
	case "recipient", "msisdn", "to":
		return req.TenantID + "|to|" + req.To
	case "campaign":
		if req.CampaignID != "" {
			return req.TenantID + "|campaign|" + req.CampaignID
		}
	case "tenant":
		return "tenant|" + req.TenantID
	case "sender":
		return req.TenantID + "|sender|" + req.From
	case "message":
		if req.IdempotencyKey != "" {
			return req.TenantID + "|msg|" + req.IdempotencyKey
		}
	}
	return ""
}
func stateFromProfile(p provider.Profile) ProviderState {
	if p.Meta != nil {
		switch strings.ToLower(p.Meta["state"]) {
		case "down":
			return ProviderDown
		case "maintenance":
			return ProviderMaintenance
		case "draining":
			return ProviderDraining
		case "suspended", "disabled":
			return ProviderSuspended
		}
	}
	return ProviderActive
}
func capacityFrom(st ProviderStats, ss SessionSnapshot) CapacitySnapshot {
	return CapacitySnapshot{ProviderID: st.ProviderID, CurrentTPS: st.CurrentTPS, MaxTPS: st.MaxTPS, WindowUsed: ss.WindowUsed, WindowSize: ss.WindowSize, Healthy: ss.Healthy, Draining: ss.Draining, State: ss.State}
}
func (r *Router) expireCooldownLocked() {
	now := time.Now()
	for k, until := range r.cooldown {
		if now.After(until) {
			delete(r.cooldown, k)
		}
	}
}
func removeDecision(xs []Decision, pid, rid string) []Decision {
	out := xs[:0]
	for _, x := range xs {
		if x.Provider.ID != pid || x.Rule.ID != rid {
			out = append(out, x)
		}
	}
	return out
}
func sortedProviderIDs(m map[string]provider.Profile) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
func uniqueInts(xs []int) []int {
	seen := map[int]bool{}
	out := xs[:0]
	for _, x := range xs {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}
func uniqueReasons(xs []RejectionReason) []RejectionReason {
	seen := map[RejectionReason]bool{}
	out := xs[:0]
	for _, x := range xs {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}
func contains(xs []string, v string) bool {
	for _, x := range xs {
		if eqFold(x, v) {
			return true
		}
	}
	return false
}
func eqFold(a, b string) bool { return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b)) }
func truthy(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "y", "required":
		return true
	}
	return false
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
