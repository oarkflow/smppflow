package campaign

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/message"
)

type State string

const (
	Draft     State = "draft"
	Running   State = "running"
	Paused    State = "paused"
	Cancelled State = "cancelled"
	Completed State = "completed"
)

type Campaign struct {
	ID         string            `json:"id"`
	TenantID   string            `json:"tenant_id"`
	Name       string            `json:"name"`
	State      State             `json:"state"`
	Sender     string            `json:"sender"`
	TemplateID string            `json:"template_id,omitempty"`
	TPS        int               `json:"tps"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
	Meta       map[string]string `json:"meta,omitempty"`
}
type Recipient struct {
	ID         string               `json:"id"`
	CampaignID string               `json:"campaign_id"`
	To         string               `json:"to"`
	Variables  map[string]string    `json:"variables,omitempty"`
	MessageID  message.ID           `json:"message_id,omitempty"`
	State      message.SubmitResult `json:"state,omitempty"`
	CreatedAt  time.Time            `json:"created_at"`
	UpdatedAt  time.Time            `json:"updated_at"`
}
type Store struct {
	mu         sync.RWMutex
	campaigns  map[string]Campaign
	recipients map[string]Recipient
}

func NewStore() *Store {
	return &Store{campaigns: map[string]Campaign{}, recipients: map[string]Recipient{}}
}
func (s *Store) SaveCampaign(ctx context.Context, c Campaign) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.ID == "" {
		return errors.New("campaign id required")
	}
	if c.State == "" {
		c.State = Draft
	}
	now := time.Now()
	if c.CreatedAt.IsZero() {
		c.CreatedAt = now
	}
	c.UpdatedAt = now
	s.campaigns[c.ID] = c
	return nil
}
func (s *Store) SetState(ctx context.Context, id string, state State) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.campaigns[id]
	if !ok {
		return errors.New("campaign not found")
	}
	c.State = state
	c.UpdatedAt = time.Now()
	s.campaigns[id] = c
	return nil
}
func (s *Store) AddRecipient(ctx context.Context, r Recipient) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.ID == "" {
		r.ID = r.CampaignID + ":" + r.To
	}
	now := time.Now()
	if r.CreatedAt.IsZero() {
		r.CreatedAt = now
	}
	r.UpdatedAt = now
	s.recipients[r.ID] = r
	return nil
}
func (s *Store) ListCampaigns(ctx context.Context, tenant string) ([]Campaign, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []Campaign{}
	for _, c := range s.campaigns {
		if tenant == "" || c.TenantID == tenant {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (s *Store) ListRecipients(ctx context.Context, campaign string) ([]Recipient, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []Recipient{}
	for _, r := range s.recipients {
		if campaign == "" || r.CampaignID == campaign {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (s *Store) DedupeRecipients(ctx context.Context, campaign string) int {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]string{}
	removed := 0
	for id, r := range s.recipients {
		if r.CampaignID != campaign {
			continue
		}
		if old := seen[r.To]; old != "" {
			delete(s.recipients, id)
			removed++
		} else {
			seen[r.To] = id
		}
	}
	return removed
}
