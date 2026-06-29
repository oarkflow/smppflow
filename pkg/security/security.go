package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
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

func (p *IPPolicy) Compile() error {
	parse := func(xs []string) ([]*net.IPNet, error) {
		out := []*net.IPNet{}
		for _, x := range xs {
			if !strings.Contains(x, "/") {
				ip := net.ParseIP(x)
				if ip == nil {
					continue
				}
				if ip.To4() != nil {
					x += "/32"
				} else {
					x += "/128"
				}
			}
			_, n, err := net.ParseCIDR(x)
			if err != nil {
				return nil, err
			}
			out = append(out, n)
		}
		return out, nil
	}
	var err error
	p.allow, err = parse(p.AllowCIDR)
	if err != nil {
		return err
	}
	p.deny, err = parse(p.DenyCIDR)
	return err
}
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
