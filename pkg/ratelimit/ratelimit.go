package ratelimit

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

type Scope string

const (
	ScopeGlobal   Scope = "global"
	ScopeTenant   Scope = "tenant"
	ScopeProvider Scope = "provider"
	ScopeRoute    Scope = "route"
)

type Policy struct {
	Key              string `json:"key"`
	Scope            Scope  `json:"scope"`
	RatePerSecond    int    `json:"rate_per_second"`
	Burst            int    `json:"burst"`
	QueueLimit       int    `json:"queue_limit"`
	Adaptive         bool   `json:"adaptive"`
	MinRatePerSecond int    `json:"min_rate_per_second"`
	MaxRatePerSecond int    `json:"max_rate_per_second"`
}

type Decision struct {
	Allowed       bool          `json:"allowed"`
	Queued        bool          `json:"queued"`
	Delay         time.Duration `json:"delay"`
	Key           string        `json:"key"`
	Reason        string        `json:"reason,omitempty"`
	RatePerSecond int           `json:"rate_per_second"`
	Tokens        float64       `json:"tokens"`
}

type bucket struct {
	policy    Policy
	tokens    float64
	last      time.Time
	queued    int
	throttles int
	successes int
}

type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	now     func() time.Time
}

func New() *Limiter { return &Limiter{buckets: map[string]*bucket{}, now: time.Now} }
func (l *Limiter) SetPolicy(p Policy) error {
	if p.Key == "" {
		return errors.New("rate policy key required")
	}
	if p.RatePerSecond <= 0 {
		return errors.New("rate_per_second must be positive")
	}
	if p.Burst <= 0 {
		p.Burst = p.RatePerSecond
	}
	if p.MinRatePerSecond <= 0 {
		p.MinRatePerSecond = 1
	}
	if p.MaxRatePerSecond <= 0 {
		p.MaxRatePerSecond = p.RatePerSecond
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.buckets[p.Key]
	if b == nil {
		b = &bucket{last: l.now(), tokens: float64(p.Burst)}
		l.buckets[p.Key] = b
	}
	b.policy = p
	if b.tokens > float64(p.Burst) {
		b.tokens = float64(p.Burst)
	}
	return nil
}
func (l *Limiter) Allow(ctx context.Context, key string, cost int) (Decision, error) {
	_ = ctx
	if cost <= 0 {
		cost = 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.buckets[key]
	if b == nil {
		return Decision{}, errors.New("rate policy not found")
	}
	l.refill(b)
	if b.tokens >= float64(cost) {
		b.tokens -= float64(cost)
		b.successes++
		return Decision{Allowed: true, Key: key, RatePerSecond: b.policy.RatePerSecond, Tokens: b.tokens}, nil
	}
	need := float64(cost) - b.tokens
	delay := time.Duration(need / float64(b.policy.RatePerSecond) * float64(time.Second))
	if b.policy.QueueLimit > 0 && b.queued < b.policy.QueueLimit {
		b.queued++
		return Decision{Allowed: false, Queued: true, Delay: delay, Key: key, Reason: "backpressure", RatePerSecond: b.policy.RatePerSecond, Tokens: b.tokens}, nil
	}
	return Decision{Allowed: false, Delay: delay, Key: key, Reason: "rate_limited", RatePerSecond: b.policy.RatePerSecond, Tokens: b.tokens}, nil
}
func (l *Limiter) DoneQueued(key string) {
	l.mu.Lock()
	if b := l.buckets[key]; b != nil && b.queued > 0 {
		b.queued--
	}
	l.mu.Unlock()
}
func (l *Limiter) MarkThrottle(key string) {
	l.mu.Lock()
	if b := l.buckets[key]; b != nil {
		b.throttles++
		if b.policy.Adaptive && b.policy.RatePerSecond > b.policy.MinRatePerSecond {
			b.policy.RatePerSecond = maxInt(b.policy.MinRatePerSecond, b.policy.RatePerSecond/2)
		}
	}
	l.mu.Unlock()
}
func (l *Limiter) MarkSuccess(key string) {
	l.mu.Lock()
	if b := l.buckets[key]; b != nil {
		b.successes++
		if b.policy.Adaptive && b.throttles == 0 && b.policy.RatePerSecond < b.policy.MaxRatePerSecond {
			b.policy.RatePerSecond++
		}
		if b.successes%100 == 0 {
			b.throttles = 0
		}
	}
	l.mu.Unlock()
}
func (l *Limiter) Snapshot() []Policy {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Policy, 0, len(l.buckets))
	for _, b := range l.buckets {
		out = append(out, b.policy)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
func (l *Limiter) refill(b *bucket) {
	now := l.now()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens += elapsed * float64(b.policy.RatePerSecond)
	if b.tokens > float64(b.policy.Burst) {
		b.tokens = float64(b.policy.Burst)
	}
	b.last = now
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
