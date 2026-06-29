package compliance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/message"
)

type Reputation struct {
	Domain    string
	Score     float64
	Category  string
	UpdatedAt time.Time
}
type SenderPolicy struct {
	TenantID         string
	Sender           string
	Verified         bool
	AllowedCountries []string
}
type ModerationDecision struct {
	Allowed        bool
	Score          float64
	Reasons        []string
	NormalizedText string
	URLs           []string
}
type Engine struct {
	mu        sync.RWMutex
	badWords  []*regexp.Regexp
	domainRep map[string]Reputation
	senders   map[string]SenderPolicy
	optOut    map[string]bool
}

func New() *Engine {
	return &Engine{domainRep: map[string]Reputation{}, senders: map[string]SenderPolicy{}, optOut: map[string]bool{}}
}
func (e *Engine) AddBlockedPattern(rx string) error {
	r, err := regexp.Compile(rx)
	if err != nil {
		return err
	}
	e.mu.Lock()
	e.badWords = append(e.badWords, r)
	e.mu.Unlock()
	return nil
}
func (e *Engine) SetDomainReputation(r Reputation) {
	e.mu.Lock()
	e.domainRep[strings.ToLower(r.Domain)] = r
	e.mu.Unlock()
}
func (e *Engine) VerifySender(p SenderPolicy) {
	e.mu.Lock()
	e.senders[p.TenantID+"\x00"+p.Sender] = p
	e.mu.Unlock()
}
func (e *Engine) OptOut(number string) { e.mu.Lock(); e.optOut[number] = true; e.mu.Unlock() }
func (e *Engine) HandleInbound(ctx context.Context, from, text string) (string, bool) {
	_ = ctx
	t := strings.TrimSpace(strings.ToUpper(text))
	switch t {
	case "STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT":
		e.OptOut(from)
		return "You are unsubscribed. Reply HELP for help.", true
	case "HELP":
		return "Help: contact support. Reply STOP to unsubscribe.", true
	default:
		return "", false
	}
}
func (e *Engine) Check(ctx context.Context, req message.SubmitRequest, country string) ModerationDecision {
	_ = ctx
	dec := ModerationDecision{Allowed: true, NormalizedText: normalize(req.Text)}
	e.mu.RLock()
	bad := append([]*regexp.Regexp(nil), e.badWords...)
	rep := map[string]Reputation{}
	for k, v := range e.domainRep {
		rep[k] = v
	}
	sender := e.senders[req.TenantID+"\x00"+req.From]
	opted := e.optOut[req.To]
	e.mu.RUnlock()
	add := func(score float64, reason string) {
		dec.Score += score
		dec.Reasons = append(dec.Reasons, reason)
		if dec.Score >= 60 {
			dec.Allowed = false
		}
	}
	if opted {
		add(100, "recipient opted out")
	}
	if !sender.Verified {
		add(30, "sender is not verified")
	}
	if len(sender.AllowedCountries) > 0 && !contains(sender.AllowedCountries, country) {
		add(40, "sender not allowed for country")
	}
	for _, r := range bad {
		if r.MatchString(dec.NormalizedText) {
			add(60, "blocked content pattern: "+r.String())
		}
	}
	for _, u := range extractURLs(req.Text) {
		dec.URLs = append(dec.URLs, u)
		host := hostOf(u)
		if rr, ok := rep[host]; ok {
			add(rr.Score, "domain reputation "+host+" category="+rr.Category)
		}
	}
	return dec
}
func Fingerprint(text string) string {
	h := sha256.Sum256([]byte(normalize(text)))
	return hex.EncodeToString(h[:])
}
func normalize(s string) string { return strings.Join(strings.Fields(strings.ToLower(s)), " ") }
func extractURLs(s string) []string {
	re := regexp.MustCompile(`https?://[^\s]+`)
	return re.FindAllString(s, -1)
}
func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return strings.ToLower(raw)
	}
	return strings.ToLower(u.Hostname())
}
func contains(xs []string, x string) bool {
	for _, v := range xs {
		if strings.EqualFold(v, x) {
			return true
		}
	}
	return false
}
