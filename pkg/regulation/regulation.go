package regulation

import (
	"context"
	"encoding/json"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
)

type QuietHours struct {
	Start    string `json:"start"`
	End      string `json:"end"`
	Timezone string `json:"timezone"`
}
type CountryRule struct {
	Country                    string            `json:"country"`
	SenderRegistrationRequired bool              `json:"sender_registration_required"`
	TemplateApprovalRequired   bool              `json:"template_approval_required"`
	TenDLCRequired             bool              `json:"ten_dlc_required"`
	DLTRequired                bool              `json:"dlt_required"`
	TCPARequired               bool              `json:"tcpa_required"`
	GDPRRequired               bool              `json:"gdpr_required"`
	LocalOptOutRequired        bool              `json:"local_opt_out_required"`
	QuietHours                 []QuietHours      `json:"quiet_hours"`
	BlockedPatterns            []string          `json:"blocked_patterns"`
	RequiredFooter             []string          `json:"required_footer"`
	AllowedSenders             []string          `json:"allowed_senders"`
	RegisteredTemplates        map[string]string `json:"registered_templates"`
	Meta                       map[string]string `json:"meta"`
}
type Violation struct {
	Code     string
	Message  string
	Severity string
}
type Decision struct {
	Allowed    bool
	Country    string
	Violations []Violation
	Rule       CountryRule
}
type Database struct {
	mu                sync.RWMutex
	rules             map[string]CountryRule
	dnd               map[string]bool
	optedOut          map[string]bool
	registeredSenders map[string]map[string]bool
}

func New() *Database {
	return &Database{rules: map[string]CountryRule{}, dnd: map[string]bool{}, optedOut: map[string]bool{}, registeredSenders: map[string]map[string]bool{}}
}
func (d *Database) SetRule(r CountryRule) {
	d.mu.Lock()
	d.rules[strings.ToUpper(r.Country)] = r
	d.mu.Unlock()
}
func (d *Database) Rule(country string) (CountryRule, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	r, ok := d.rules[strings.ToUpper(country)]
	return r, ok
}
func (d *Database) AddDND(e164 string) { d.mu.Lock(); d.dnd[e164] = true; d.mu.Unlock() }
func (d *Database) OptOut(e164 string) { d.mu.Lock(); d.optedOut[e164] = true; d.mu.Unlock() }
func (d *Database) RegisterSender(country, sender string) {
	d.mu.Lock()
	c := strings.ToUpper(country)
	if d.registeredSenders[c] == nil {
		d.registeredSenders[c] = map[string]bool{}
	}
	d.registeredSenders[c][sender] = true
	d.mu.Unlock()
}
func (d *Database) Check(ctx context.Context, req message.SubmitRequest, country string, now time.Time) Decision {
	_ = ctx
	country = strings.ToUpper(country)
	d.mu.RLock()
	rule := d.rules[country]
	dnd := d.dnd[req.To]
	out := d.optedOut[req.To]
	senders := d.registeredSenders[country]
	d.mu.RUnlock()
	dec := Decision{Allowed: true, Country: country, Rule: rule}
	add := func(code, msg, severity string) {
		dec.Allowed = false
		dec.Violations = append(dec.Violations, Violation{code, msg, severity})
	}
	if dnd {
		add("DND", "destination is on do-not-disturb list", "error")
	}
	if out {
		add("OPTOUT", "destination opted out", "error")
	}
	if rule.SenderRegistrationRequired && !senders[req.From] {
		add("SENDER_REGISTRATION", "sender is not registered for country", "error")
	}
	if len(rule.AllowedSenders) > 0 && !contains(rule.AllowedSenders, req.From) {
		add("SENDER_NOT_ALLOWED", "sender is not allowed by country rule", "error")
	}
	for _, p := range rule.BlockedPatterns {
		if match(p, req.Text) {
			add("CONTENT_RESTRICTED", "content matched restricted pattern", "error")
		}
	}
	for _, f := range rule.RequiredFooter {
		if !strings.Contains(strings.ToLower(req.Text), strings.ToLower(f)) {
			add("FOOTER_REQUIRED", "required footer missing: "+f, "error")
		}
	}
	if inQuiet(rule.QuietHours, now) {
		add("QUIET_HOURS", "message is inside configured quiet hours", "error")
	}
	if rule.TemplateApprovalRequired && len(rule.RegisteredTemplates) > 0 {
		ok := false
		for _, rx := range rule.RegisteredTemplates {
			if match("regex:"+rx, req.Text) {
				ok = true
				break
			}
		}
		if !ok {
			add("TEMPLATE_REQUIRED", "message does not match approved template", "error")
		}
	}
	return dec
}
func LoadJSON(r io.Reader) ([]CountryRule, error) {
	var out []CountryRule
	err := json.NewDecoder(r).Decode(&out)
	return out, err
}
func SaveJSON(w io.Writer, rules []CountryRule) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(rules)
}
func contains(xs []string, x string) bool {
	for _, v := range xs {
		if strings.EqualFold(v, x) {
			return true
		}
	}
	return false
}
func match(p, s string) bool {
	if strings.HasPrefix(p, "regex:") {
		ok, _ := regexp.MatchString(strings.TrimPrefix(p, "regex:"), s)
		return ok
	}
	return strings.Contains(strings.ToLower(s), strings.ToLower(p))
}
func inQuiet(qs []QuietHours, now time.Time) bool {
	t := now
	for _, q := range qs {
		loc := time.Local
		if q.Timezone != "" {
			if l, err := time.LoadLocation(q.Timezone); err == nil {
				loc = l
			}
		}
		t = t.In(loc)
		st, ok1 := parseHM(q.Start, t)
		en, ok2 := parseHM(q.End, t)
		if !ok1 || !ok2 {
			continue
		}
		if en.Before(st) {
			if t.After(st) || t.Before(en) {
				return true
			}
		} else if (t.Equal(st) || t.After(st)) && t.Before(en) {
			return true
		}
	}
	return false
}
func parseHM(s string, base time.Time) (time.Time, bool) {
	x, err := time.Parse("15:04", s)
	if err != nil {
		return time.Time{}, false
	}
	return time.Date(base.Year(), base.Month(), base.Day(), x.Hour(), x.Minute(), 0, 0, base.Location()), true
}
