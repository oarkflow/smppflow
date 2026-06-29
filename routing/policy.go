package routing

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/message"
)

type Strategy string

const (
	StrategyBestQuality Strategy = "best_quality"
	StrategyLeastCost   Strategy = "least_cost"
	StrategyFastest     Strategy = "fastest"
	StrategyWeighted    Strategy = "weighted"
)

type Policy struct {
	Strategy Strategy
	StickyBy string
	Cooldown time.Duration
}
type AdvancedRouter struct {
	Base     *Router
	Policy   Policy
	mu       sync.Mutex
	cooldown map[string]time.Time
	sticky   map[string]string
}

func NewAdvanced(base *Router, pol Policy) *AdvancedRouter {
	if base == nil {
		base = New()
	}
	return &AdvancedRouter{Base: base, Policy: pol, cooldown: map[string]time.Time{}, sticky: map[string]string{}}
}
func (a *AdvancedRouter) CooldownProvider(id string) {
	a.mu.Lock()
	a.cooldown[id] = time.Now().Add(a.Policy.Cooldown)
	a.mu.Unlock()
}
func (a *AdvancedRouter) Route(ctx context.Context, req message.SubmitRequest) (Decision, error) {
	a.mu.Lock()
	for k, until := range a.cooldown {
		if time.Now().After(until) {
			delete(a.cooldown, k)
		}
	}
	cd := map[string]bool{}
	for k := range a.cooldown {
		cd[k] = true
	}
	a.mu.Unlock()
	providers := a.Base.Providers()
	rules := a.Base.Rules()
	candidates := []Decision{}
	for _, p := range providers {
		if cd[p.ID] {
			continue
		}
		st, _ := a.Base.ProviderStats(p.ID)
		rule := Rule{ProviderID: p.ID, Priority: 9999, Weight: 1}
		for _, r := range rules {
			if r.ProviderID == p.ID && ruleMatches(r, normalizeSubmitRequest(req)) {
				rule = r
				break
			}
		}
		candidates = append(candidates, Decision{Provider: p, Rule: rule, Stats: st, Score: score(rule, p, st, req), Reason: string(a.Policy.Strategy), Candidates: len(providers)})
	}
	if len(candidates) == 0 {
		return a.Base.Route(ctx, req)
	}
	switch a.Policy.Strategy {
	case StrategyLeastCost:
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].Stats.CostPerSegment < candidates[j].Stats.CostPerSegment })
		return candidates[0], nil
	case StrategyFastest:
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].Stats.AvgLatency < candidates[j].Stats.AvgLatency })
		return candidates[0], nil
	case StrategyWeighted:
		total := 0
		for _, d := range candidates {
			w := d.Rule.Weight
			if w <= 0 {
				w = 1
			}
			total += w
		}
		n := rand.Intn(total)
		cur := 0
		for _, d := range candidates {
			w := d.Rule.Weight
			if w <= 0 {
				w = 1
			}
			cur += w
			if n < cur {
				return d, nil
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Score > candidates[j].Score })
	return candidates[0], nil
}
