package cluster

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

type NodeState string

const (
	NodeStarting NodeState = "starting"
	NodeActive   NodeState = "active"
	NodeDraining NodeState = "draining"
	NodeDown     NodeState = "down"
)

type Node struct {
	ID       string
	Addr     string
	Zone     string
	State    NodeState
	Capacity int
	LastSeen time.Time
	Meta     map[string]string
}
type BindLease struct {
	ProviderID string
	TenantID   string
	BindID     string
	OwnerNode  string
	Version    int64
	ExpiresAt  time.Time
	Draining   bool
}
type Store interface {
	UpsertNode(context.Context, Node) error
	ListNodes(context.Context) ([]Node, error)
	TryAcquireBind(context.Context, BindLease, time.Duration) (BindLease, bool, error)
	RenewBind(context.Context, BindLease, time.Duration) (BindLease, error)
	ReleaseBind(context.Context, BindLease) error
	ListLeases(context.Context) ([]BindLease, error)
}

type MemoryStore struct {
	mu     sync.Mutex
	nodes  map[string]Node
	leases map[string]BindLease
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{nodes: map[string]Node{}, leases: map[string]BindLease{}}
}
func (s *MemoryStore) UpsertNode(ctx context.Context, n Node) error {
	_ = ctx
	if n.ID == "" {
		return errors.New("node id required")
	}
	if n.LastSeen.IsZero() {
		n.LastSeen = time.Now()
	}
	s.mu.Lock()
	s.nodes[n.ID] = n
	s.mu.Unlock()
	return nil
}
func (s *MemoryStore) ListNodes(ctx context.Context) ([]Node, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func leaseKey(l BindLease) string { return l.ProviderID + "\x00" + l.TenantID + "\x00" + l.BindID }
func (s *MemoryStore) TryAcquireBind(ctx context.Context, l BindLease, ttl time.Duration) (BindLease, bool, error) {
	_ = ctx
	if l.OwnerNode == "" {
		return l, false, errors.New("owner node required")
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := leaseKey(l)
	old, ok := s.leases[k]
	if ok && old.ExpiresAt.After(time.Now()) && old.OwnerNode != l.OwnerNode && !old.Draining {
		return old, false, nil
	}
	l.Version = old.Version + 1
	l.ExpiresAt = time.Now().Add(ttl)
	s.leases[k] = l
	return l, true, nil
}
func (s *MemoryStore) RenewBind(ctx context.Context, l BindLease, ttl time.Duration) (BindLease, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.leases[leaseKey(l)]
	if !ok || old.OwnerNode != l.OwnerNode {
		return l, errors.New("lease not owned")
	}
	old.Version++
	old.ExpiresAt = time.Now().Add(ttl)
	s.leases[leaseKey(l)] = old
	return old, nil
}
func (s *MemoryStore) ReleaseBind(ctx context.Context, l BindLease) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.leases[leaseKey(l)]
	if ok && old.OwnerNode != l.OwnerNode {
		return errors.New("lease not owned")
	}
	delete(s.leases, leaseKey(l))
	return nil
}
func (s *MemoryStore) ListLeases(ctx context.Context) ([]BindLease, error) {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]BindLease, 0, len(s.leases))
	for _, l := range s.leases {
		out = append(out, l)
	}
	return out, nil
}

type Coordinator struct {
	Store Store
	Node  Node
	TTL   time.Duration
}

func (c Coordinator) Heartbeat(ctx context.Context) error {
	n := c.Node
	if n.State == "" {
		n.State = NodeActive
	}
	n.LastSeen = time.Now()
	return c.Store.UpsertNode(ctx, n)
}
func (c Coordinator) AcquireBind(ctx context.Context, providerID, tenantID, bindID string) (BindLease, bool, error) {
	return c.Store.TryAcquireBind(ctx, BindLease{ProviderID: providerID, TenantID: tenantID, BindID: bindID, OwnerNode: c.Node.ID}, c.ttl())
}
func (c Coordinator) DrainBind(ctx context.Context, l BindLease) error {
	leases, err := c.Store.ListLeases(ctx)
	if err != nil {
		return err
	}
	for _, x := range leases {
		if leaseKey(x) == leaseKey(l) && x.OwnerNode == c.Node.ID {
			x.Draining = true
			_, _, err := c.Store.TryAcquireBind(ctx, x, c.ttl())
			return err
		}
	}
	return errors.New("lease not found")
}
func (c Coordinator) ttl() time.Duration {
	if c.TTL <= 0 {
		return 30 * time.Second
	}
	return c.TTL
}

type Partitioner struct{ Nodes []Node }

func (p Partitioner) Owner(key string) (Node, bool) {
	active := make([]Node, 0, len(p.Nodes))
	for _, n := range p.Nodes {
		if n.State == NodeActive && n.Capacity != 0 {
			active = append(active, n)
		}
	}
	if len(active) == 0 {
		return Node{}, false
	}
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return active[int(h)%len(active)], true
}
