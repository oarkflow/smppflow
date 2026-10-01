package storage

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
)

type Store interface{ message.Store }

type Memory struct {
	mu          sync.RWMutex
	msgs        map[message.ID]*message.Message
	dlrs        []*message.DeliveryReceipt
	idem        map[string]message.IdempotencyRecord
	transitions []message.Transition
	attempts    []message.SubmitAttempt
	inbound     []*message.InboundMessage
	webhooks    []message.WebhookEvent
}

func NewMemory() *Memory {
	return &Memory{msgs: map[message.ID]*message.Message{}, idem: map[string]message.IdempotencyRecord{}}
}

func cloneMessage(msg *message.Message) *message.Message {
	if msg == nil {
		return nil
	}
	cp := *msg
	cp.Parts = append([]message.SegmentRecord(nil), msg.Parts...)
	cp.Transitions = append([]message.Transition(nil), msg.Transitions...)
	cp.Attempts = append([]message.SubmitAttempt(nil), msg.Attempts...)
	if msg.Meta != nil {
		cp.Meta = copyMap(msg.Meta)
	}
	return &cp
}

func (m *Memory) SaveMessage(ctx context.Context, msg *message.Message) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
	msg.UpdatedAt = now
	m.msgs[msg.ID] = cloneMessage(msg)
	return nil
}
func (m *Memory) GetMessage(ctx context.Context, id message.ID) (*message.Message, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	x := m.msgs[id]
	if x == nil {
		return nil, ErrNotFound
	}
	return cloneMessage(x), nil
}
func (m *Memory) ListMessages(ctx context.Context) ([]*message.Message, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*message.Message, 0, len(m.msgs))
	for _, x := range m.msgs {
		out = append(out, cloneMessage(x))
	}
	// Newest first, with the ID as a tiebreak so listings stay stable when
	// several messages share a CreatedAt timestamp.
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}
func (m *Memory) UpdateMessageState(ctx context.Context, id message.ID, st lifecycle.MessageState) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	x := m.msgs[id]
	if x == nil {
		return ErrNotFound
	}
	if err := message.ApplyTransition(x, st, "state update", "store", x.ProviderID, ""); err != nil {
		x.State = st
		x.UpdatedAt = time.Now()
	}
	return nil
}
func (m *Memory) SaveDLR(ctx context.Context, d *message.DeliveryReceipt) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *d
	if cp.ReceivedAt.IsZero() {
		cp.ReceivedAt = time.Now()
	}
	if cp.DoneAt.IsZero() {
		cp.DoneAt = cp.ReceivedAt
	}
	m.dlrs = append(m.dlrs, &cp)
	for _, msg := range m.msgs {
		if correlate(msg, &cp) {
			applyDLRLocked(msg, &cp)
		}
	}
	return nil
}
func (m *Memory) ListDLR(ctx context.Context) ([]*message.DeliveryReceipt, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*message.DeliveryReceipt, 0, len(m.dlrs))
	for _, d := range m.dlrs {
		cp := *d
		out = append(out, &cp)
	}
	return out, nil
}

func correlate(msg *message.Message, d *message.DeliveryReceipt) bool {
	if msg.ProviderMessageID != "" && msg.ProviderMessageID == d.ProviderMessageID {
		return true
	}
	if string(msg.ID) == d.MessageID {
		return true
	}
	for _, p := range msg.Parts {
		if p.ID == d.SegmentID || (p.ProviderMessageID != "" && p.ProviderMessageID == d.ProviderMessageID) {
			return true
		}
	}
	return false
}
func applyDLRLocked(msg *message.Message, d *message.DeliveryReceipt) {
	matched := false
	for i := range msg.Parts {
		if msg.Parts[i].ID == d.SegmentID || (msg.Parts[i].ProviderMessageID != "" && msg.Parts[i].ProviderMessageID == d.ProviderMessageID) {
			msg.Parts[i].State = d.State
			msg.Parts[i].UpdatedAt = time.Now()
			msg.Parts[i].ErrorCode = d.ErrorCode
			matched = true
		}
	}
	if matched {
		msg.State = message.AggregateSegmentState(msg.Parts)
	} else {
		_ = message.ApplyTransition(msg, d.State, "dlr received", "provider", d.ProviderID, d.ErrorCode)
	}
	msg.UpdatedAt = time.Now()
}

func (m *Memory) SaveIdempotency(ctx context.Context, r message.IdempotencyRecord) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}
	m.idem[r.TenantID+"\x00"+r.Key] = r
	return nil
}
func (m *Memory) GetIdempotency(ctx context.Context, tenant, key string) (message.IdempotencyRecord, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.idem[tenant+"\x00"+key]
	if !ok || (!r.ExpiresAt.IsZero() && time.Now().After(r.ExpiresAt)) {
		return message.IdempotencyRecord{}, ErrNotFound
	}
	return r, nil
}
func (m *Memory) SaveTransition(ctx context.Context, t message.Transition) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	if t.At.IsZero() {
		t.At = time.Now()
	}
	m.transitions = append(m.transitions, t)
	return nil
}
func (m *Memory) ListTransitions(ctx context.Context, f message.EventFilter) ([]message.Transition, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []message.Transition{}
	for _, t := range m.transitions {
		if f.MessageID != "" && t.MessageID != f.MessageID {
			continue
		}
		if f.ProviderID != "" && t.ProviderID != f.ProviderID {
			continue
		}
		if f.State != "" && t.To != f.State {
			continue
		}
		if !f.From.IsZero() && t.At.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && t.At.After(f.To) {
			continue
		}
		out = append(out, t)
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}
	return out, nil
}
func (m *Memory) SaveAttempt(ctx context.Context, a message.SubmitAttempt) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	if a.StartedAt.IsZero() {
		a.StartedAt = time.Now()
	}
	m.attempts = append(m.attempts, a)
	if msg := m.msgs[a.MessageID]; msg != nil {
		msg.Attempts = append(msg.Attempts, a)
	}
	return nil
}
func (m *Memory) ListAttempts(ctx context.Context, f message.EventFilter) ([]message.SubmitAttempt, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []message.SubmitAttempt{}
	for _, a := range m.attempts {
		if f.MessageID != "" && a.MessageID != f.MessageID {
			continue
		}
		if f.ProviderID != "" && a.ProviderID != f.ProviderID {
			continue
		}
		if f.State != "" && a.State != f.State {
			continue
		}
		if !f.From.IsZero() && a.StartedAt.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && a.StartedAt.After(f.To) {
			continue
		}
		out = append(out, a)
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}
	return out, nil
}
func (m *Memory) SaveInbound(ctx context.Context, in *message.InboundMessage) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *in
	if cp.ID == "" {
		cp.ID = "in-" + time.Now().Format("20060102150405.000000000")
	}
	if cp.ReceivedAt.IsZero() {
		cp.ReceivedAt = time.Now()
	}
	m.inbound = append(m.inbound, &cp)
	return nil
}
func (m *Memory) ListInbound(ctx context.Context, f message.EventFilter) ([]*message.InboundMessage, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []*message.InboundMessage{}
	for _, in := range m.inbound {
		if f.ProviderID != "" && in.ProviderID != f.ProviderID {
			continue
		}
		if !f.From.IsZero() && in.ReceivedAt.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && in.ReceivedAt.After(f.To) {
			continue
		}
		cp := *in
		out = append(out, &cp)
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}
	return out, nil
}
func (m *Memory) SaveWebhookAttempt(ctx context.Context, e message.WebhookEvent) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	if e.ID == "" {
		e.ID = "wh-" + time.Now().Format("20060102150405.000000000")
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now()
	}
	m.webhooks = append(m.webhooks, e)
	return nil
}
func (m *Memory) ListWebhookAttempts(ctx context.Context, f message.EventFilter) ([]message.WebhookEvent, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []message.WebhookEvent{}
	for _, e := range m.webhooks {
		if f.TenantID != "" && e.TenantID != f.TenantID {
			continue
		}
		if f.MessageID != "" && e.MessageID != f.MessageID {
			continue
		}
		if !f.From.IsZero() && e.CreatedAt.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && e.CreatedAt.After(f.To) {
			continue
		}
		out = append(out, e)
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}
	return out, nil
}

var ErrNotFound = errString("not found")

type errString string

func (e errString) Error() string { return string(e) }
func copyMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
