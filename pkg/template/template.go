package template

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	texttemplate "text/template"
	"time"
)

type Status string

const (
	Pending  Status = "pending"
	Approved Status = "approved"
	Rejected Status = "rejected"
	Expired  Status = "expired"
)

type Template struct {
	ID         string            `json:"id"`
	TenantID   string            `json:"tenant_id"`
	Name       string            `json:"name"`
	Body       string            `json:"body"`
	Country    string            `json:"country,omitempty"`
	Operator   string            `json:"operator,omitempty"`
	ExternalID string            `json:"external_id,omitempty"`
	Status     Status            `json:"status"`
	Variables  []string          `json:"variables,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
	Meta       map[string]string `json:"meta,omitempty"`
}
type RenderResult struct {
	TemplateID string            `json:"template_id"`
	Text       string            `json:"text"`
	Variables  map[string]string `json:"variables"`
	RenderedAt time.Time         `json:"rendered_at"`
}
type Store struct {
	mu        sync.RWMutex
	templates map[string]Template
}

func NewStore() *Store { return &Store{templates: map[string]Template{}} }
func (s *Store) Save(ctx context.Context, t Template) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.ID == "" {
		return errors.New("template id required")
	}
	if t.Status == "" {
		t.Status = Pending
	}
	now := time.Now()
	if t.CreatedAt.IsZero() {
		t.CreatedAt = now
	}
	t.UpdatedAt = now
	s.templates[t.ID] = t
	return nil
}
func (s *Store) Get(ctx context.Context, id string) (Template, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.templates[id]
	if !ok {
		return Template{}, errors.New("template not found")
	}
	return t, nil
}
func (s *Store) List(ctx context.Context, tenant string) ([]Template, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []Template{}
	for _, t := range s.templates {
		if tenant == "" || t.TenantID == tenant {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func Render(t Template, vars map[string]string) (RenderResult, error) {
	if t.Status != Approved {
		return RenderResult{}, errors.New("template is not approved")
	}
	data := map[string]string{}
	for k, v := range vars {
		data[k] = v
	}
	tpl, err := texttemplate.New(t.ID).Option("missingkey=error").Parse(t.Body)
	if err != nil {
		return RenderResult{}, err
	}
	var b bytes.Buffer
	if err := tpl.Execute(&b, data); err != nil {
		return RenderResult{}, err
	}
	return RenderResult{TemplateID: t.ID, Text: b.String(), Variables: data, RenderedAt: time.Now()}, nil
}
func ExtractVariables(body string) []string {
	vars := map[string]bool{}
	for _, part := range strings.Split(body, "{{")[1:] {
		if i := strings.Index(part, "}}"); i >= 0 {
			vars[strings.TrimSpace(part[:i])] = true
		}
	}
	out := []string{}
	for k := range vars {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
