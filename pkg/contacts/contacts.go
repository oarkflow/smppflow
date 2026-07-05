package contacts

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

type Contact struct {
	ID        string            `json:"id"`
	TenantID  string            `json:"tenant_id"`
	MSISDN    string            `json:"msisdn"`
	FirstName string            `json:"first_name,omitempty"`
	LastName  string            `json:"last_name,omitempty"`
	Country   string            `json:"country,omitempty"`
	Operator  string            `json:"operator,omitempty"`
	Tags      []string          `json:"tags,omitempty"`
	Meta      map[string]string `json:"meta,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}
type Segment struct {
	TenantID    string   `json:"tenant_id"`
	IncludeTags []string `json:"include_tags,omitempty"`
	ExcludeTags []string `json:"exclude_tags,omitempty"`
	Country     string   `json:"country,omitempty"`
	Operator    string   `json:"operator,omitempty"`
}
type Store struct {
	mu   sync.RWMutex
	rows map[string]Contact
}

func NewStore() *Store { return &Store{rows: map[string]Contact{}} }
func (s *Store) Upsert(ctx context.Context, c Contact) error {
	_ = ctx
	if c.TenantID == "" || c.MSISDN == "" {
		return errors.New("tenant and msisdn required")
	}
	if c.ID == "" {
		c.ID = c.TenantID + ":" + norm(c.MSISDN)
	}
	now := time.Now()
	if c.CreatedAt.IsZero() {
		c.CreatedAt = now
	}
	c.UpdatedAt = now
	c.MSISDN = norm(c.MSISDN)
	s.mu.Lock()
	s.rows[c.ID] = clone(c)
	s.mu.Unlock()
	return nil
}
func (s *Store) Delete(ctx context.Context, id string) error {
	_ = ctx
	s.mu.Lock()
	delete(s.rows, id)
	s.mu.Unlock()
	return nil
}
func (s *Store) List(ctx context.Context, tenant string) []Contact {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []Contact{}
	for _, c := range s.rows {
		if tenant == "" || c.TenantID == tenant {
			out = append(out, clone(c))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MSISDN < out[j].MSISDN })
	return out
}
func (s *Store) Select(ctx context.Context, seg Segment) []Contact {
	xs := s.List(ctx, seg.TenantID)
	out := []Contact{}
	for _, c := range xs {
		if seg.Country != "" && !strings.EqualFold(c.Country, seg.Country) {
			continue
		}
		if seg.Operator != "" && !strings.EqualFold(c.Operator, seg.Operator) {
			continue
		}
		if !hasAll(c.Tags, seg.IncludeTags) {
			continue
		}
		if hasAny(c.Tags, seg.ExcludeTags) {
			continue
		}
		out = append(out, c)
	}
	return out
}
func ImportCSV(ctx context.Context, st *Store, tenant string, r io.Reader) (int, error) {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true
	rows, err := cr.ReadAll()
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	header := map[string]int{}
	for i, h := range rows[0] {
		header[strings.ToLower(strings.TrimSpace(h))] = i
	}
	count := 0
	for _, row := range rows[1:] {
		get := func(k string) string {
			if i, ok := header[k]; ok && i < len(row) {
				return strings.TrimSpace(row[i])
			}
			return ""
		}
		c := Contact{TenantID: tenant, MSISDN: get("msisdn"), FirstName: get("first_name"), LastName: get("last_name"), Country: get("country"), Operator: get("operator")}
		if tags := get("tags"); tags != "" {
			c.Tags = split(tags)
		}
		if c.MSISDN == "" {
			continue
		}
		if err := st.Upsert(ctx, c); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}
func norm(s string) string { return strings.TrimPrefix(strings.TrimSpace(s), "+") }
func split(s string) []string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ';' || r == '|' })
	out := []string{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
func hasAll(tags, want []string) bool {
	m := map[string]bool{}
	for _, t := range tags {
		m[strings.ToLower(t)] = true
	}
	for _, w := range want {
		if !m[strings.ToLower(w)] {
			return false
		}
	}
	return true
}
func hasAny(tags, want []string) bool {
	m := map[string]bool{}
	for _, t := range tags {
		m[strings.ToLower(t)] = true
	}
	for _, w := range want {
		if m[strings.ToLower(w)] {
			return true
		}
	}
	return false
}
func clone(c Contact) Contact {
	c.Tags = append([]string(nil), c.Tags...)
	if c.Meta != nil {
		m := map[string]string{}
		for k, v := range c.Meta {
			m[k] = v
		}
		c.Meta = m
	}
	return c
}
