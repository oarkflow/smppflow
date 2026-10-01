package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

func HashSecret(secret string) string {
	s := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(s[:])
}
func ConstantTimeEqualSecret(secret, hash string) bool {
	hs := HashSecret(secret)
	return hmac.Equal([]byte(hs), []byte(hash))
}
func EqualString(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

type IPPolicy struct {
	AllowCIDR []string
	DenyCIDR  []string
	allow     []*net.IPNet
	deny      []*net.IPNet
}

// Compile parses AllowCIDR and DenyCIDR into compiled networks.
//
// Every entry must be a valid IP or CIDR; invalid entries are rejected rather
// than dropped. Silently discarding a malformed allowlist entry would leave
// p.allow empty, and an empty allowlist permits every address, so a typo would
// fail open instead of closed.
func (p *IPPolicy) Compile() error {
	parse := func(name string, xs []string) ([]*net.IPNet, error) {
		out := []*net.IPNet{}
		for _, x := range xs {
			if strings.TrimSpace(x) == "" {
				continue
			}
			if !strings.Contains(x, "/") {
				ip := net.ParseIP(x)
				if ip == nil {
					return nil, fmt.Errorf("security: invalid %s entry %q: not an IP address or CIDR", name, x)
				}
				if ip.To4() != nil {
					x += "/32"
				} else {
					x += "/128"
				}
			}
			_, n, err := net.ParseCIDR(x)
			if err != nil {
				return nil, fmt.Errorf("security: invalid %s entry %q: %w", name, x, err)
			}
			out = append(out, n)
		}
		return out, nil
	}
	var err error
	p.allow, err = parse("AllowCIDR", p.AllowCIDR)
	if err != nil {
		return err
	}
	p.deny, err = parse("DenyCIDR", p.DenyCIDR)
	return err
}

// Allowed reports whether remote (either "ip" or "ip:port") is permitted.
//
// Deny entries always win. When AllowCIDR is empty every address not denied is
// allowed; populate AllowCIDR to run in allowlist mode. Compile must be called
// before use.
func (p *IPPolicy) Allowed(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, n := range p.deny {
		if n.Contains(ip) {
			return false
		}
	}
	if len(p.allow) == 0 {
		return true
	}
	for _, n := range p.allow {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

type Limiter struct {
	mu     sync.Mutex
	rate   int
	burst  int
	tokens float64
	last   time.Time
}

func NewLimiter(rate, burst int) *Limiter {
	if rate <= 0 {
		rate = 1
	}
	if burst <= 0 {
		burst = rate
	}
	return &Limiter{rate: rate, burst: burst, tokens: float64(burst), last: time.Now()}
}
func (l *Limiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.tokens += now.Sub(l.last).Seconds() * float64(l.rate)
	l.last = now
	if l.tokens > float64(l.burst) {
		l.tokens = float64(l.burst)
	}
	if l.tokens >= 1 {
		l.tokens--
		return true
	}
	return false
}

type Redactor struct {
	Secrets []string
	Mask    string
}

func (r Redactor) Redact(s string) string {
	if r.Mask == "" {
		r.Mask = "***"
	}
	for _, sec := range r.Secrets {
		if sec != "" {
			s = strings.ReplaceAll(s, sec, r.Mask)
		}
	}
	return s
}
