package intelligence

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

type Reachability string

const (
	ReachabilityUnknown     Reachability = "unknown"
	ReachabilityReachable   Reachability = "reachable"
	ReachabilityUnreachable Reachability = "unreachable"
	ReachabilityAbsent      Reachability = "absent"
)

type NumberProfile struct {
	MSISDN          string            `json:"msisdn"`
	Country         string            `json:"country,omitempty"`
	MCC             string            `json:"mcc,omitempty"`
	MNC             string            `json:"mnc,omitempty"`
	OriginalMCC     string            `json:"original_mcc,omitempty"`
	OriginalMNC     string            `json:"original_mnc,omitempty"`
	Carrier         string            `json:"carrier,omitempty"`
	OriginalCarrier string            `json:"original_carrier,omitempty"`
	Ported          bool              `json:"ported"`
	Roaming         bool              `json:"roaming"`
	RoamingMCC      string            `json:"roaming_mcc,omitempty"`
	RoamingMNC      string            `json:"roaming_mnc,omitempty"`
	Reachability    Reachability      `json:"reachability"`
	FraudScore      float64           `json:"fraud_score,omitempty"`
	Sources         []string          `json:"sources,omitempty"`
	Meta            map[string]string `json:"meta,omitempty"`
	CheckedAt       time.Time         `json:"checked_at"`
	ExpiresAt       time.Time         `json:"expires_at,omitempty"`
}

type LookupAdapter interface {
	Name() string
	LookupNumber(context.Context, string) (NumberProfile, error)
}

type LookupPolicy struct {
	TTL              time.Duration
	RequireReachable bool
	BlockRoaming     bool
	MaxFraudScore    float64
	PreferFresh      bool
}

type LookupDecision struct {
	Allowed bool          `json:"allowed"`
	Profile NumberProfile `json:"profile"`
	Reasons []string      `json:"reasons,omitempty"`
	Cached  bool          `json:"cached"`
}

type LookupEngine struct {
	mu       sync.RWMutex
	adapters []LookupAdapter
	cache    map[string]NumberProfile
	policy   LookupPolicy
}

func NewLookupEngine(policy LookupPolicy, adapters ...LookupAdapter) *LookupEngine {
	if policy.TTL == 0 {
		policy.TTL = 10 * time.Minute
	}
	return &LookupEngine{adapters: adapters, cache: map[string]NumberProfile{}, policy: policy}
}

func (e *LookupEngine) Register(a LookupAdapter) {
	if a == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.adapters = append(e.adapters, a)
}

func (e *LookupEngine) Lookup(ctx context.Context, msisdn string) (LookupDecision, error) {
	if msisdn == "" {
		return LookupDecision{}, errors.New("msisdn required")
	}
	now := time.Now()
	e.mu.RLock()
	cached, ok := e.cache[msisdn]
	adapters := append([]LookupAdapter(nil), e.adapters...)
	e.mu.RUnlock()
	if ok && (cached.ExpiresAt.IsZero() || cached.ExpiresAt.After(now)) && !e.policy.PreferFresh {
		dec := e.evaluate(cached)
		dec.Cached = true
		return dec, nil
	}
	var errs []string
	var best NumberProfile
	for _, a := range adapters {
		p, err := a.LookupNumber(ctx, msisdn)
		if err != nil {
			errs = append(errs, a.Name()+": "+err.Error())
			continue
		}
		if p.MSISDN == "" {
			p.MSISDN = msisdn
		}
		if p.CheckedAt.IsZero() {
			p.CheckedAt = now
		}
		if p.ExpiresAt.IsZero() {
			p.ExpiresAt = now.Add(e.policy.TTL)
		}
		p.Sources = append(p.Sources, a.Name())
		best = mergeProfile(best, p)
	}
	if best.MSISDN == "" {
		if ok {
			dec := e.evaluate(cached)
			dec.Cached = true
			dec.Reasons = append(dec.Reasons, errs...)
			return dec, nil
		}
		return LookupDecision{Allowed: false, Reasons: errs}, errors.New("number lookup failed")
	}
	e.mu.Lock()
	e.cache[msisdn] = best
	e.mu.Unlock()
	dec := e.evaluate(best)
	dec.Reasons = append(dec.Reasons, errs...)
	return dec, nil
}

func (e *LookupEngine) evaluate(p NumberProfile) LookupDecision {
	dec := LookupDecision{Allowed: true, Profile: p}
	if e.policy.RequireReachable && p.Reachability != ReachabilityReachable {
		dec.Allowed = false
		dec.Reasons = append(dec.Reasons, "number is not reachable")
	}
	if e.policy.BlockRoaming && p.Roaming {
		dec.Allowed = false
		dec.Reasons = append(dec.Reasons, "roaming is blocked")
	}
	if e.policy.MaxFraudScore > 0 && p.FraudScore > e.policy.MaxFraudScore {
		dec.Allowed = false
		dec.Reasons = append(dec.Reasons, "fraud score exceeds policy")
	}
	return dec
}

func mergeProfile(a, b NumberProfile) NumberProfile {
	if a.MSISDN == "" {
		return b
	}
	if b.Country != "" {
		a.Country = b.Country
	}
	if b.MCC != "" {
		a.MCC = b.MCC
	}
	if b.MNC != "" {
		a.MNC = b.MNC
	}
	if b.Carrier != "" {
		a.Carrier = b.Carrier
	}
	if b.OriginalMCC != "" {
		a.OriginalMCC = b.OriginalMCC
	}
	if b.OriginalMNC != "" {
		a.OriginalMNC = b.OriginalMNC
	}
	if b.OriginalCarrier != "" {
		a.OriginalCarrier = b.OriginalCarrier
	}
	a.Ported = a.Ported || b.Ported
	a.Roaming = a.Roaming || b.Roaming
	if b.RoamingMCC != "" {
		a.RoamingMCC = b.RoamingMCC
	}
	if b.RoamingMNC != "" {
		a.RoamingMNC = b.RoamingMNC
	}
	if b.Reachability != "" && b.Reachability != ReachabilityUnknown {
		a.Reachability = b.Reachability
	}
	if b.FraudScore > a.FraudScore {
		a.FraudScore = b.FraudScore
	}
	a.Sources = append(a.Sources, b.Sources...)
	sort.Strings(a.Sources)
	if a.Meta == nil {
		a.Meta = map[string]string{}
	}
	for k, v := range b.Meta {
		a.Meta[k] = v
	}
	if b.CheckedAt.After(a.CheckedAt) {
		a.CheckedAt = b.CheckedAt
	}
	if a.ExpiresAt.IsZero() || (!b.ExpiresAt.IsZero() && b.ExpiresAt.Before(a.ExpiresAt)) {
		a.ExpiresAt = b.ExpiresAt
	}
	return a
}

type StaticAdapter struct {
	AdapterName string
	Profiles    map[string]NumberProfile
	Err         error
}

func (s StaticAdapter) Name() string {
	if s.AdapterName != "" {
		return s.AdapterName
	}
	return "static"
}
func (s StaticAdapter) LookupNumber(ctx context.Context, msisdn string) (NumberProfile, error) {
	_ = ctx
	if s.Err != nil {
		return NumberProfile{}, s.Err
	}
	p, ok := s.Profiles[msisdn]
	if !ok {
		return NumberProfile{}, errors.New("number not found")
	}
	return p, nil
}
