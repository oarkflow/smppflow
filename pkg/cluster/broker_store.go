// BrokerStore implements Store on top of github.com/oarkflow/broker's
// cluster package, which is a generic (not message-broker-specific) Raft
// consensus engine: it exposes a Raft type driven by a pluggable Transport
// and a pluggable StateMachine (Apply/Snapshot/Restore), the same shape as
// etcd/raft or hashicorp/raft. That engine is reused here as a generic
// replicated log for node registration and bind-lease state, giving
// linearizable writes (every write is a Raft log entry that must reach a
// quorum before it is applied) instead of RedisStore's documented
// GET-then-SET race window on contested lease renewal/release.
//
// Design / how it works:
//
//   - Every mutation (UpsertNode, TryAcquireBind, RenewBind, ReleaseBind) is
//     encoded as a brokerOp and submitted to the underlying broker
//     cluster.Raft via Raft.Submit. Submit only succeeds on the current
//     Raft leader (cluster.ErrNotLeader otherwise) -- this mirrors real
//     Raft-backed coordination: writes are only safe once ordered through
//     the leader's log, so callers must route writes to whichever
//     BrokerStore replica IsLeader() reports true (or retry elsewhere on
//     ErrNotLeader), the same way a client of etcd/raft or hashicorp/raft
//     must talk to the leader.
//   - Every replica's brokerStateMachine.Apply is invoked, in log order,
//     once an entry commits (i.e. a quorum of replicas has it durably in
//     their log). Apply deterministically mutates the same
//     nodes/leases maps MemoryStore uses, using a timestamp embedded in the
//     command by the submitter (not time.Now() inside Apply) so every
//     replica computes an identical result from the identical entry --
//     required for state-machine replication to stay consistent.
//   - Reads (ListNodes/ListLeases) call Raft.ReadIndex first, which only
//     succeeds on the leader and only after the leader reconfirms its
//     leadership against a quorum; the read then blocks until this
//     replica's state machine has applied up to that confirmed index. That
//     is the standard Raft "ReadIndex" linearizable-read recipe, so reads
//     (like writes) are only served from the leader.
//
// Honesty about scope: this is a real Raft log and a real quorum-committed,
// deterministically-applied state machine (not a lock-free approximation),
// so the lease-acquisition race window documented in redis_store.go's
// header does not exist here -- two concurrent TryAcquireBind calls for the
// same lease key are strictly ordered by the Raft log, and only one can
// observe "the lease was free" for a given log position. What is *not*
// implemented is broker's own cluster membership/discovery, TLS, or
// production-grade snapshotting wiring: callers are responsible for
// constructing the Transport (broker's TCPTransport for real multi-process
// deployments, or MemoryTransport for embedded/single-process and test
// use) and for supplying the peer list. There is also no forwarding of
// writes from followers to the leader -- callers must route to the leader
// themselves, whereas RedisStore/MemoryStore accept calls from anywhere.
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	bcluster "github.com/oarkflow/broker/cluster"
)

// brokerOpKind identifies the kind of mutation carried by a brokerOp.
type brokerOpKind string

const (
	opUpsertNode  brokerOpKind = "upsert_node"
	opAcquireBind brokerOpKind = "acquire_bind"
	opRenewBind   brokerOpKind = "renew_bind"
	opReleaseBind brokerOpKind = "release_bind"
)

// brokerOp is the JSON-encoded command carried by a single Raft log entry.
// Now is set by the submitter (the current leader, at Submit time) and used
// by every replica's Apply instead of time.Now(), so all replicas compute
// byte-identical results from the same log entry.
type brokerOp struct {
	Kind  brokerOpKind `json:"kind"`
	Node  *Node        `json:"node,omitempty"`
	Lease *BindLease   `json:"lease,omitempty"`
	TTL   time.Duration `json:"ttl,omitempty"`
	Now   time.Time    `json:"now"`
}

// brokerOpOutcome is what applying a brokerOp produced, stashed by index so
// the submitter can retrieve it once the entry commits and applies.
type brokerOpOutcome struct {
	Lease    BindLease
	Acquired bool
	Err      string
}

// brokerStateMachine implements bcluster.StateMachine. It is applied
// identically, in log order, on every Raft replica.
type brokerStateMachine struct {
	mu          sync.Mutex
	nodes       map[string]Node
	leases      map[string]BindLease
	lastApplied uint64
	results     map[uint64]brokerOpOutcome
	waiters     map[uint64][]chan brokerOpOutcome
}

func newBrokerStateMachine() *brokerStateMachine {
	return &brokerStateMachine{
		nodes:   map[string]Node{},
		leases:  map[string]BindLease{},
		results: map[uint64]brokerOpOutcome{},
		waiters: map[uint64][]chan brokerOpOutcome{},
	}
}

// Apply implements bcluster.StateMachine. Called by the Raft engine, in
// order, once per committed log entry (on every replica).
func (sm *brokerStateMachine) Apply(entry *bcluster.LogEntry) error {
	sm.mu.Lock()

	var outcome brokerOpOutcome
	if entry.Type == bcluster.EntryNormal {
		var op brokerOp
		if err := json.Unmarshal(entry.Data, &op); err != nil {
			outcome = brokerOpOutcome{Err: "broker store: malformed op: " + err.Error()}
		} else {
			outcome = sm.applyOpLocked(op)
		}
	}

	sm.lastApplied = entry.Index
	sm.results[entry.Index] = outcome
	waiters := sm.waiters[entry.Index]
	delete(sm.waiters, entry.Index)
	sm.mu.Unlock()

	for _, ch := range waiters {
		ch <- outcome
	}
	return nil
}

// applyOpLocked mutates nodes/leases for a single op. Must be called with
// sm.mu held. Mirrors MemoryStore's semantics exactly, substituting op.Now
// for time.Now() so every replica agrees.
func (sm *brokerStateMachine) applyOpLocked(op brokerOp) brokerOpOutcome {
	switch op.Kind {
	case opUpsertNode:
		if op.Node == nil || op.Node.ID == "" {
			return brokerOpOutcome{Err: "node id required"}
		}
		n := *op.Node
		if n.LastSeen.IsZero() {
			n.LastSeen = op.Now
		}
		sm.nodes[n.ID] = n
		return brokerOpOutcome{}

	case opAcquireBind:
		if op.Lease == nil || op.Lease.OwnerNode == "" {
			return brokerOpOutcome{Err: "owner node required"}
		}
		l := *op.Lease
		ttl := op.TTL
		if ttl <= 0 {
			ttl = 30 * time.Second
		}
		k := leaseKey(l)
		old, ok := sm.leases[k]
		if ok && old.ExpiresAt.After(op.Now) && old.OwnerNode != l.OwnerNode && !old.Draining {
			return brokerOpOutcome{Lease: old, Acquired: false}
		}
		l.Version = old.Version + 1
		l.ExpiresAt = op.Now.Add(ttl)
		sm.leases[k] = l
		return brokerOpOutcome{Lease: l, Acquired: true}

	case opRenewBind:
		if op.Lease == nil {
			return brokerOpOutcome{Err: "lease required"}
		}
		l := *op.Lease
		ttl := op.TTL
		if ttl <= 0 {
			ttl = 30 * time.Second
		}
		old, ok := sm.leases[leaseKey(l)]
		if !ok || old.OwnerNode != l.OwnerNode {
			return brokerOpOutcome{Err: "lease not owned"}
		}
		old.Version++
		old.ExpiresAt = op.Now.Add(ttl)
		sm.leases[leaseKey(l)] = old
		return brokerOpOutcome{Lease: old, Acquired: true}

	case opReleaseBind:
		if op.Lease == nil {
			return brokerOpOutcome{Err: "lease required"}
		}
		l := *op.Lease
		old, ok := sm.leases[leaseKey(l)]
		if ok && old.OwnerNode != l.OwnerNode {
			return brokerOpOutcome{Err: "lease not owned"}
		}
		delete(sm.leases, leaseKey(l))
		return brokerOpOutcome{}

	default:
		return brokerOpOutcome{Err: "unknown op"}
	}
}

// Snapshot implements bcluster.StateMachine.
func (sm *brokerStateMachine) Snapshot() ([]byte, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	type snap struct {
		Nodes  map[string]Node      `json:"nodes"`
		Leases map[string]BindLease `json:"leases"`
	}
	return json.Marshal(snap{Nodes: sm.nodes, Leases: sm.leases})
}

// Restore implements bcluster.StateMachine.
func (sm *brokerStateMachine) Restore(data []byte) error {
	type snap struct {
		Nodes  map[string]Node      `json:"nodes"`
		Leases map[string]BindLease `json:"leases"`
	}
	var s snap
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if s.Nodes == nil {
		s.Nodes = map[string]Node{}
	}
	if s.Leases == nil {
		s.Leases = map[string]BindLease{}
	}
	sm.nodes = s.Nodes
	sm.leases = s.Leases
	return nil
}

// waitForOutcome blocks until entry idx has been applied on this replica
// and returns the outcome that Apply computed for it.
func (sm *brokerStateMachine) waitForOutcome(idx uint64, timeout time.Duration) (brokerOpOutcome, error) {
	sm.mu.Lock()
	if idx <= sm.lastApplied {
		out := sm.results[idx]
		sm.mu.Unlock()
		return out, nil
	}
	ch := make(chan brokerOpOutcome, 1)
	sm.waiters[idx] = append(sm.waiters[idx], ch)
	sm.mu.Unlock()
	select {
	case out := <-ch:
		return out, nil
	case <-time.After(timeout):
		return brokerOpOutcome{}, errors.New("broker store: timed out waiting for raft to apply command")
	}
}

// waitForApplied blocks until this replica's state machine has applied at
// least up to idx (used for linearizable ReadIndex-based reads).
func (sm *brokerStateMachine) waitForApplied(idx uint64, timeout time.Duration) error {
	if idx == 0 {
		return nil
	}
	_, err := sm.waitForOutcome(idx, timeout)
	return err
}

func (sm *brokerStateMachine) snapshotNodes() []Node {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	out := make([]Node, 0, len(sm.nodes))
	for _, n := range sm.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (sm *brokerStateMachine) snapshotLeases() []BindLease {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	out := make([]BindLease, 0, len(sm.leases))
	for _, l := range sm.leases {
		out = append(out, l)
	}
	return out
}

// BrokerStore implements Store on top of a broker cluster.Raft group. See
// the package-level doc comment above for the consistency model and its
// limitations.
type BrokerStore struct {
	id           string
	raft         *bcluster.Raft
	sm           *brokerStateMachine
	applyTimeout time.Duration
}

// BrokerStoreOption configures a BrokerStore at construction time.
type BrokerStoreOption func(*BrokerStore)

// WithApplyTimeout overrides how long a write/read waits for the
// underlying Raft group to commit/apply before giving up. Default 3s.
func WithApplyTimeout(d time.Duration) BrokerStoreOption {
	return func(s *BrokerStore) {
		if d > 0 {
			s.applyTimeout = d
		}
	}
}

// NewBrokerStore starts a broker cluster.Raft node with id, participating
// in a Raft group with the given peers (which must include id), and
// returns a Store backed by it. transport must already be able to reach
// every peer (use broker's cluster.NewMemoryTransport + Register for a
// single-process/embedded/test setup with more than one simulated node, or
// broker's cluster.NewTCPTransport for a real multi-process deployment).
//
// The returned BrokerStore only accepts writes and linearizable reads while
// its Raft node is the group's leader; see IsLeader/Leader. Callers running
// a multi-node smppflow cluster are expected to route Store calls to
// whichever node currently reports IsLeader() == true, retrying elsewhere
// when a call fails with an error wrapping bcluster.ErrNotLeader.
func NewBrokerStore(id string, peers []string, transport bcluster.Transport, opts ...BrokerStoreOption) (*BrokerStore, error) {
	if id == "" {
		return nil, errors.New("broker store: id required")
	}
	sm := newBrokerStateMachine()
	raft := bcluster.NewRaft(bcluster.RaftConfig{
		ID:               id,
		Peers:            peers,
		Transport:        transport,
		StateMachine:     sm,
		ElectionTimeout:  150 * time.Millisecond,
		HeartbeatTimeout: 50 * time.Millisecond,
	})
	if err := raft.Start(); err != nil {
		return nil, err
	}
	s := &BrokerStore{id: id, raft: raft, sm: sm, applyTimeout: 3 * time.Second}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// Close stops the underlying Raft node.
func (s *BrokerStore) Close() error {
	s.raft.Stop()
	return nil
}

// IsLeader reports whether this BrokerStore's Raft node currently believes
// it is the group leader (the only node that can accept writes/reads).
func (s *BrokerStore) IsLeader() bool { return s.raft.IsLeader() }

// Leader returns the node ID this replica believes is the current leader
// (may be stale/empty during an election).
func (s *BrokerStore) Leader() string { return s.raft.Leader() }

func (s *BrokerStore) submit(op brokerOp) (brokerOpOutcome, error) {
	data, err := json.Marshal(op)
	if err != nil {
		return brokerOpOutcome{}, err
	}
	idx, err := s.raft.Submit(data)
	if err != nil {
		return brokerOpOutcome{}, err
	}
	return s.sm.waitForOutcome(idx, s.applyTimeout)
}

func (s *BrokerStore) UpsertNode(ctx context.Context, n Node) error {
	_ = ctx
	if n.ID == "" {
		return errors.New("node id required")
	}
	out, err := s.submit(brokerOp{Kind: opUpsertNode, Node: &n, Now: time.Now()})
	if err != nil {
		return err
	}
	if out.Err != "" {
		return errors.New(out.Err)
	}
	return nil
}

func (s *BrokerStore) ListNodes(ctx context.Context) ([]Node, error) {
	_ = ctx
	idx, err := s.raft.ReadIndex()
	if err != nil {
		return nil, err
	}
	if err := s.sm.waitForApplied(idx, s.applyTimeout); err != nil {
		return nil, err
	}
	return s.sm.snapshotNodes(), nil
}

func (s *BrokerStore) TryAcquireBind(ctx context.Context, l BindLease, ttl time.Duration) (BindLease, bool, error) {
	_ = ctx
	if l.OwnerNode == "" {
		return l, false, errors.New("owner node required")
	}
	out, err := s.submit(brokerOp{Kind: opAcquireBind, Lease: &l, TTL: ttl, Now: time.Now()})
	if err != nil {
		return l, false, err
	}
	if out.Err != "" {
		return l, false, errors.New(out.Err)
	}
	return out.Lease, out.Acquired, nil
}

func (s *BrokerStore) RenewBind(ctx context.Context, l BindLease, ttl time.Duration) (BindLease, error) {
	_ = ctx
	out, err := s.submit(brokerOp{Kind: opRenewBind, Lease: &l, TTL: ttl, Now: time.Now()})
	if err != nil {
		return l, err
	}
	if out.Err != "" {
		return l, errors.New(out.Err)
	}
	return out.Lease, nil
}

func (s *BrokerStore) ReleaseBind(ctx context.Context, l BindLease) error {
	_ = ctx
	out, err := s.submit(brokerOp{Kind: opReleaseBind, Lease: &l, Now: time.Now()})
	if err != nil {
		return err
	}
	if out.Err != "" {
		return errors.New(out.Err)
	}
	return nil
}

func (s *BrokerStore) ListLeases(ctx context.Context) ([]BindLease, error) {
	_ = ctx
	idx, err := s.raft.ReadIndex()
	if err != nil {
		return nil, err
	}
	if err := s.sm.waitForApplied(idx, s.applyTimeout); err != nil {
		return nil, err
	}
	return s.sm.snapshotLeases(), nil
}

var _ Store = (*BrokerStore)(nil)
var _ bcluster.StateMachine = (*brokerStateMachine)(nil)
