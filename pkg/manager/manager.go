package manager

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/account"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/provider"
	"github.com/oarkflow/smppflow/pkg/queue"
	"github.com/oarkflow/smppflow/pkg/routing"
	"github.com/oarkflow/smppflow/pkg/session"
)

type TenantStatus string

const (
	TenantActive    TenantStatus = "active"
	TenantSuspended TenantStatus = "suspended"
	TenantDisabled  TenantStatus = "disabled"
)

type Tenant struct {
	ID                 string            `json:"id"`
	Name               string            `json:"name"`
	Status             TenantStatus      `json:"status"`
	TPS                int               `json:"tps"`
	BalanceLimitMicros int64             `json:"balance_limit_micros"`
	AllowedCountries   []string          `json:"allowed_countries,omitempty"`
	AllowedSenderIDs   []string          `json:"allowed_sender_ids,omitempty"`
	WebhookURL         string            `json:"webhook_url,omitempty"`
	CreatedAt          time.Time         `json:"created_at"`
	UpdatedAt          time.Time         `json:"updated_at"`
	Meta               map[string]string `json:"meta,omitempty"`
}
type BindAccount struct {
	ID           string            `json:"id"`
	TenantID     string            `json:"tenant_id"`
	SystemID     string            `json:"system_id"`
	PasswordHash string            `json:"password_hash,omitempty"`
	BindMode     string            `json:"bind_mode"`
	MaxBinds     int               `json:"max_binds"`
	TPS          int               `json:"tps"`
	IPAllowlist  []string          `json:"ip_allowlist,omitempty"`
	SourcePolicy []string          `json:"source_policy,omitempty"`
	Enabled      bool              `json:"enabled"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
	Meta         map[string]string `json:"meta,omitempty"`
}
type SenderApprovalState string

const (
	SenderPending  SenderApprovalState = "pending"
	SenderApproved SenderApprovalState = "approved"
	SenderRejected SenderApprovalState = "rejected"
	SenderExpired  SenderApprovalState = "expired"
)

type SenderApproval struct {
	ID          string              `json:"id"`
	TenantID    string              `json:"tenant_id"`
	Sender      string              `json:"sender"`
	Country     string              `json:"country"`
	Operator    string              `json:"operator,omitempty"`
	State       SenderApprovalState `json:"state"`
	DocumentRef string              `json:"document_ref,omitempty"`
	Reason      string              `json:"reason,omitempty"`
	CreatedAt   time.Time           `json:"created_at"`
	UpdatedAt   time.Time           `json:"updated_at"`
	ExpiresAt   time.Time           `json:"expires_at,omitempty"`
}
type AuditLog struct {
	ID         string            `json:"id"`
	TenantID   string            `json:"tenant_id,omitempty"`
	ActorID    string            `json:"actor_id,omitempty"`
	Action     string            `json:"action"`
	Resource   string            `json:"resource"`
	ResourceID string            `json:"resource_id"`
	At         time.Time         `json:"at"`
	Meta       map[string]string `json:"meta,omitempty"`
}
type Trace struct {
	Message     *message.Message           `json:"message,omitempty"`
	DLRs        []*message.DeliveryReceipt `json:"dlrs,omitempty"`
	Attempts    []message.SubmitAttempt    `json:"attempts,omitempty"`
	Transitions []message.Transition       `json:"transitions,omitempty"`
	Webhooks    []message.WebhookEvent     `json:"webhooks,omitempty"`
}
type RouteSimulation struct {
	Request           message.SubmitRequest `json:"request"`
	Decision          routing.Decision      `json:"decision"`
	ComplianceAllowed bool                  `json:"compliance_allowed"`
	CostMicros        int64                 `json:"cost_micros,omitempty"`
	RejectedProviders []string              `json:"rejected_providers,omitempty"`
	Reasons           []string              `json:"reasons,omitempty"`
}

type Store struct {
	mu        sync.RWMutex
	tenants   map[string]Tenant
	binds     map[string]BindAccount
	providers map[string]provider.Profile
	senders   map[string]SenderApproval
	audits    []AuditLog
}

func NewStore() *Store {
	return &Store{tenants: map[string]Tenant{}, binds: map[string]BindAccount{}, providers: map[string]provider.Profile{}, senders: map[string]SenderApproval{}}
}
func (s *Store) SaveTenant(ctx context.Context, t Tenant) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.ID == "" {
		return errors.New("tenant id required")
	}
	now := time.Now()
	if t.CreatedAt.IsZero() {
		t.CreatedAt = now
	}
	t.UpdatedAt = now
	if t.Status == "" {
		t.Status = TenantActive
	}
	s.tenants[t.ID] = t
	return nil
}
func (s *Store) GetTenant(ctx context.Context, id string) (Tenant, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tenants[id]
	if !ok {
		return Tenant{}, account.ErrNotFound
	}
	return cloneTenant(t), nil
}
func (s *Store) ListTenants(ctx context.Context) ([]Tenant, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Tenant, 0, len(s.tenants))
	for _, t := range s.tenants {
		out = append(out, cloneTenant(t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (s *Store) SaveBind(ctx context.Context, b BindAccount) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if b.ID == "" {
		b.ID = b.TenantID + ":" + b.SystemID
	}
	if b.MaxBinds == 0 {
		b.MaxBinds = 1
	}
	if !b.Enabled {
		if old, ok := s.binds[b.ID]; ok {
			b.Enabled = old.Enabled
		} else {
			b.Enabled = true
		}
	}
	now := time.Now()
	if b.CreatedAt.IsZero() {
		b.CreatedAt = now
	}
	b.UpdatedAt = now
	s.binds[b.ID] = b
	return nil
}
func (s *Store) ListBinds(ctx context.Context, tenant string) ([]BindAccount, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []BindAccount{}
	for _, b := range s.binds {
		if tenant == "" || b.TenantID == tenant {
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (s *Store) SaveProvider(ctx context.Context, p provider.Profile) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.ID == "" {
		return errors.New("provider id required")
	}
	s.providers[p.ID] = p
	return nil
}
func (s *Store) ListProviders(ctx context.Context) ([]provider.Profile, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]provider.Profile, 0, len(s.providers))
	for _, p := range s.providers {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (s *Store) SaveSenderApproval(ctx context.Context, a SenderApproval) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if a.ID == "" {
		a.ID = a.TenantID + ":" + a.Country + ":" + a.Sender
	}
	if a.State == "" {
		a.State = SenderPending
	}
	now := time.Now()
	if a.CreatedAt.IsZero() {
		a.CreatedAt = now
	}
	a.UpdatedAt = now
	s.senders[a.ID] = a
	return nil
}
func (s *Store) ListSenderApprovals(ctx context.Context, tenant string) ([]SenderApproval, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []SenderApproval{}
	for _, a := range s.senders {
		if tenant == "" || a.TenantID == tenant {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (s *Store) Audit(ctx context.Context, a AuditLog) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if a.ID == "" {
		a.ID = "audit-" + time.Now().Format("20060102150405.000000000")
	}
	if a.At.IsZero() {
		a.At = time.Now()
	}
	s.audits = append(s.audits, a)
	return nil
}
func (s *Store) ListAudit(ctx context.Context, tenant string, limit int) ([]AuditLog, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []AuditLog{}
	for i := len(s.audits) - 1; i >= 0; i-- {
		a := s.audits[i]
		if tenant == "" || a.TenantID == tenant {
			out = append(out, a)
		}
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

type Operations struct {
	Store        *Store
	MessageStore message.Store
	Router       *routing.Router
	Sessions     *session.Manager
	Queue        *queue.Memory
}

func NewOperations(st *Store, msgStore message.Store, r *routing.Router, q *queue.Memory, sessions *session.Manager) *Operations {
	if st == nil {
		st = NewStore()
	}
	return &Operations{Store: st, MessageStore: msgStore, Router: r, Queue: q, Sessions: sessions}
}
func (o *Operations) UpsertTenant(ctx context.Context, actor string, t Tenant) error {
	if err := o.Store.SaveTenant(ctx, t); err != nil {
		return err
	}
	return o.Store.Audit(ctx, AuditLog{TenantID: t.ID, ActorID: actor, Action: "tenant.upsert", Resource: "tenant", ResourceID: t.ID})
}
func (o *Operations) SuspendTenant(ctx context.Context, actor, tenant, reason string) error {
	t, err := o.Store.GetTenant(ctx, tenant)
	if err != nil {
		return err
	}
	t.Status = TenantSuspended
	if t.Meta == nil {
		t.Meta = map[string]string{}
	}
	t.Meta["suspend_reason"] = reason
	if err = o.Store.SaveTenant(ctx, t); err != nil {
		return err
	}
	return o.Store.Audit(ctx, AuditLog{TenantID: tenant, ActorID: actor, Action: "tenant.suspend", Resource: "tenant", ResourceID: tenant, Meta: map[string]string{"reason": reason}})
}
func (o *Operations) SaveProvider(ctx context.Context, actor string, p provider.Profile) error {
	if err := o.Store.SaveProvider(ctx, p); err != nil {
		return err
	}
	if o.Router != nil {
		_ = o.Router.RegisterProvider(p)
	}
	return o.Store.Audit(ctx, AuditLog{ActorID: actor, Action: "provider.upsert", Resource: "provider", ResourceID: p.ID})
}
func (o *Operations) SaveBind(ctx context.Context, actor string, b BindAccount) error {
	if err := o.Store.SaveBind(ctx, b); err != nil {
		return err
	}
	return o.Store.Audit(ctx, AuditLog{TenantID: b.TenantID, ActorID: actor, Action: "bind.upsert", Resource: "bind", ResourceID: b.ID})
}
func (o *Operations) SessionsSnapshot() []session.ManagedProvider {
	if o.Sessions == nil {
		return nil
	}
	return o.Sessions.Status()
}
func (o *Operations) QueueSnapshot() []queue.Job {
	if o.Queue == nil {
		return nil
	}
	return o.Queue.Snapshot()
}
func (o *Operations) DLQ() []queue.Job {
	if o.Queue == nil {
		return nil
	}
	return o.Queue.DLQ()
}
func (o *Operations) ReplayDLQ(ctx context.Context, ids ...string) error {
	if o.Queue == nil {
		return errors.New("nil queue")
	}
	return o.Queue.ReplayDLQ(ctx, ids...)
}
func (o *Operations) CancelQueued(ctx context.Context, id string) error {
	if o.Queue == nil {
		return errors.New("nil queue")
	}
	return o.Queue.Cancel(ctx, id)
}
func (o *Operations) SimulateRoute(ctx context.Context, req message.SubmitRequest) (RouteSimulation, error) {
	if o.Router == nil {
		return RouteSimulation{}, errors.New("nil router")
	}
	dec, err := o.Router.Route(ctx, req)
	sim := RouteSimulation{Request: req, Decision: dec, ComplianceAllowed: true}
	if err != nil {
		sim.Reasons = append(sim.Reasons, err.Error())
		return sim, err
	}
	return sim, nil
}
func (o *Operations) TraceMessage(ctx context.Context, id message.ID) (Trace, error) {
	if o.MessageStore == nil {
		return Trace{}, errors.New("nil message store")
	}
	msg, err := o.MessageStore.GetMessage(ctx, id)
	if err != nil {
		return Trace{}, err
	}
	tr := Trace{Message: msg}
	dlrs, _ := o.MessageStore.ListDLR(ctx)
	for _, d := range dlrs {
		if d.MessageID == string(id) || d.ProviderMessageID == msg.ProviderMessageID {
			tr.DLRs = append(tr.DLRs, d)
		}
	}
	if ex, ok := o.MessageStore.(message.ExtendedStore); ok {
		tr.Attempts, _ = ex.ListAttempts(ctx, message.EventFilter{MessageID: id})
		tr.Transitions, _ = ex.ListTransitions(ctx, message.EventFilter{MessageID: id})
		if wh, ok := o.MessageStore.(message.WebhookStore); ok {
			tr.Webhooks, _ = wh.ListWebhookAttempts(ctx, message.EventFilter{MessageID: id})
		}
	}
	return tr, nil
}
func HasRole(u account.User, roles ...string) bool {
	have := map[string]bool{}
	for _, r := range u.Roles {
		have[strings.ToLower(r)] = true
	}
	for _, r := range roles {
		if have[strings.ToLower(r)] {
			return true
		}
	}
	return false
}
func cloneTenant(t Tenant) Tenant {
	t.AllowedCountries = append([]string(nil), t.AllowedCountries...)
	t.AllowedSenderIDs = append([]string(nil), t.AllowedSenderIDs...)
	if t.Meta != nil {
		m := map[string]string{}
		for k, v := range t.Meta {
			m[k] = v
		}
		t.Meta = m
	}
	return t
}
