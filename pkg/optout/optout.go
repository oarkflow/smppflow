package optout

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

type Action string

const (
	ActionOptOut Action = "opt_out"
	ActionOptIn  Action = "opt_in"
	ActionHelp   Action = "help"
	ActionNone   Action = "none"
)

type Record struct {
	TenantID  string    `json:"tenant_id"`
	MSISDN    string    `json:"msisdn"`
	Source    string    `json:"source"`
	Reason    string    `json:"reason"`
	CreatedAt time.Time `json:"created_at"`
}
type Reply struct {
	Action Action  `json:"action"`
	Text   string  `json:"text"`
	Record *Record `json:"record,omitempty"`
}

type Store struct {
	mu   sync.RWMutex
	rows map[string]Record
}

func NewStore() *Store          { return &Store{rows: map[string]Record{}} }
func key(t, n string) string    { return strings.ToLower(t) + ":" + normalize(n) }
func normalize(n string) string { n = strings.TrimSpace(n); n = strings.TrimPrefix(n, "+"); return n }
func (s *Store) Add(ctx context.Context, r Record) error {
	_ = ctx
	if r.TenantID == "" || r.MSISDN == "" {
		return errors.New("tenant and msisdn required")
	}
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}
	r.MSISDN = normalize(r.MSISDN)
	s.mu.Lock()
	s.rows[key(r.TenantID, r.MSISDN)] = r
	s.mu.Unlock()
	return nil
}
func (s *Store) Remove(ctx context.Context, tenant, msisdn string) error {
	_ = ctx
	s.mu.Lock()
	delete(s.rows, key(tenant, msisdn))
	s.mu.Unlock()
	return nil
}
func (s *Store) Has(ctx context.Context, tenant, msisdn string) bool {
	_ = ctx
	s.mu.RLock()
	_, ok := s.rows[key(tenant, msisdn)]
	s.mu.RUnlock()
	return ok
}
func (s *Store) List(ctx context.Context, tenant string) []Record {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []Record{}
	for _, r := range s.rows {
		if tenant == "" || r.TenantID == tenant {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MSISDN < out[j].MSISDN })
	return out
}

type Processor struct {
	Store                            *Store
	StopWords, StartWords, HelpWords []string
	StopReply, StartReply, HelpReply string
}

func NewProcessor(st *Store) *Processor {
	if st == nil {
		st = NewStore()
	}
	return &Processor{Store: st, StopWords: []string{"STOP", "UNSUBSCRIBE", "CANCEL", "END", "QUIT"}, StartWords: []string{"START", "YES", "UNSTOP"}, HelpWords: []string{"HELP", "INFO"}, StopReply: "You have been unsubscribed.", StartReply: "You have been resubscribed.", HelpReply: "Reply STOP to unsubscribe or START to subscribe."}
}
func (p *Processor) ProcessInbound(ctx context.Context, tenant, from, text, source string) (Reply, error) {
	word := strings.ToUpper(strings.TrimSpace(text))
	switch {
	case contains(p.StopWords, word):
		r := Record{TenantID: tenant, MSISDN: from, Source: source, Reason: "keyword:" + word}
		if err := p.Store.Add(ctx, r); err != nil {
			return Reply{}, err
		}
		return Reply{Action: ActionOptOut, Text: p.StopReply, Record: &r}, nil
	case contains(p.StartWords, word):
		_ = p.Store.Remove(ctx, tenant, from)
		return Reply{Action: ActionOptIn, Text: p.StartReply}, nil
	case contains(p.HelpWords, word):
		return Reply{Action: ActionHelp, Text: p.HelpReply}, nil
	default:
		return Reply{Action: ActionNone}, nil
	}
}
func contains(xs []string, v string) bool {
	for _, x := range xs {
		if strings.EqualFold(strings.TrimSpace(x), v) {
			return true
		}
	}
	return false
}
