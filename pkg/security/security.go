package security

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
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

const (
	passwordHashPrefix = "pbkdf2-sha256"
	passwordIterations = 210000
	passwordSaltLen    = 16
	passwordKeyLen     = 32
)

// HashPassword derives a salted PBKDF2-HMAC-SHA256 hash for stored passwords
// (e.g. SMPP bind passwords) and encodes it as
// "pbkdf2-sha256$<iterations>$<salt>$<derived-key>" with raw stdlib base64
// segments. Use HashSecret for high-entropy API keys instead.
func HashPassword(password string) (string, error) {
	return hashPasswordWithIterations(password, passwordIterations)
}

func hashPasswordWithIterations(password string, iterations int) (string, error) {
	if iterations <= 0 {
		iterations = passwordIterations
	}
	salt := make([]byte, passwordSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	return encodePasswordHash(password, salt, iterations)
}

func encodePasswordHash(password string, salt []byte, iterations int) (string, error) {
	dk, err := pbkdf2.Key(sha256.New, password, salt, iterations, passwordKeyLen)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s$%d$%s$%s",
		passwordHashPrefix,
		iterations,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(dk),
	), nil
}

// VerifyPassword reports whether password matches the stored hash produced by
// HashPassword. Legacy unsalted SHA-256 hex digests (HashSecret output) are
// still accepted so existing Account.PasswordHash values keep working.
func VerifyPassword(password, encoded string) bool {
	if strings.HasPrefix(encoded, passwordHashPrefix+"$") {
		return verifyPasswordHash(password, encoded)
	}
	return ConstantTimeEqualSecret(password, encoded)
}

func verifyPasswordHash(password, encoded string) bool {
	salt, dk, iterations, ok := parsePasswordHash(encoded)
	if !ok {
		return false
	}
	got, err := pbkdf2.Key(sha256.New, password, salt, iterations, len(dk))
	if err != nil {
		return false
	}
	return hmac.Equal(got, dk)
}

func parsePasswordHash(encoded string) (salt, dk []byte, iterations int, ok bool) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 || parts[0] != passwordHashPrefix {
		return nil, nil, 0, false
	}
	n, err := strconv.Atoi(parts[1])
	if err != nil || n <= 0 {
		return nil, nil, 0, false
	}
	salt, err = base64.RawStdEncoding.DecodeString(parts[2])
	if err != nil || len(salt) == 0 {
		return nil, nil, 0, false
	}
	dk, err = base64.RawStdEncoding.DecodeString(parts[3])
	if err != nil || len(dk) == 0 {
		return nil, nil, 0, false
	}
	return salt, dk, n, true
}

// NeedsRehash reports whether encoded should be upgraded to the current
// HashPassword parameters: legacy SHA-256 hex digests and hashes with fewer
// iterations than the current default all qualify.
func NeedsRehash(encoded string) bool {
	if !strings.HasPrefix(encoded, passwordHashPrefix+"$") {
		return true
	}
	_, _, iterations, ok := parsePasswordHash(encoded)
	return !ok || iterations < passwordIterations
}

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
