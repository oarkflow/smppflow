package cluster

// RedisStore is a Redis-backed implementation of Store, providing real
// cross-process/cross-machine coordination (as opposed to MemoryStore,
// which only simulates a cluster within a single process).
//
// Design notes / known limitations:
//
//   - Node registration/heartbeat uses `SET node:<id> <json> PX <ttl>`.
//     Redis's own key expiry is the source of truth for node liveness: a
//     node that stops heartbeating simply has its key expire and vanish
//     from ListNodes. UpsertNode also starts a background per-node
//     goroutine that keeps refreshing the key every ttl/3 using the most
//     recently upserted node snapshot, so a single UpsertNode call is
//     enough to keep a node "alive" without the caller needing to run its
//     own heartbeat loop (Coordinator.Heartbeat can still be called
//     directly/periodically too -- both just call UpsertNode).
//
//   - Bind lease acquisition uses atomic `SET lease:<key> <json> NX PX
//     <ttl>` for the common case of acquiring a free lease: this is a true
//     compare-and-set with no race window, since NX only succeeds if the
//     key does not exist.
//
//   - Redis has no built-in Lua-free CAS for "take over an existing,
//     still-live-but-draining (or same-owner) lease", so that path is a
//     GET-then-SET: read the current lease, decide it's takeable (same
//     owner reacquiring, marked Draining, or its embedded ExpiresAt has
//     passed), then unconditionally SET the new value. Two nodes racing
//     this exact GET-then-SET window on the *same* lease key at the *same*
//     moment could both believe they won and one write would clobber the
//     other; the loser would not notice until its next RenewBind fails.
//     This is the documented, accepted race window per the task's
//     instructions (no Lua scripting). RenewBind and ReleaseBind have the
//     same GET-then-SET / GET-then-DEL race window, restricted to an
//     owner acting on its own believed-owned lease.
//
//   - Redis's own PX TTL is still the backstop for abandoned leases: even
//     if a node dies mid-lease without releasing, the key expires on its
//     own and a later TryAcquireBind's NX path acquires it cleanly.
//
//   - Listing nodes/leases is poll-based via SCAN (cursor iteration, no
//     KEYS) rather than pub/sub, per the task's guidance that polling is
//     acceptable and pub/sub is not required.
import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"
)

// RedisStore implements Store using Redis as the shared coordination
// backend. It is safe for concurrent use.
type RedisStore struct {
	client *redisClient

	// Prefix namespaces all keys this store touches, so multiple
	// independent clusters (or unrelated apps) can share one Redis.
	Prefix string

	// NodeTTL is the default TTL applied to node heartbeat keys and the
	// interval driving the background refresh goroutine (refreshed at
	// NodeTTL/3). Defaults to 30s.
	NodeTTL time.Duration

	mu         sync.Mutex
	latest     map[string]Node
	heartbeats map[string]context.CancelFunc
	closed     bool
	wg         sync.WaitGroup
}

// NewRedisStore constructs a RedisStore. addr defaults to 127.0.0.1:6379
// when empty; password may be empty for an unauthenticated Redis.
func NewRedisStore(addr, password string) *RedisStore {
	return &RedisStore{
		client:     &redisClient{Addr: addr, Password: password},
		Prefix:     "smppflow:cluster:",
		NodeTTL:    30 * time.Second,
		latest:     map[string]Node{},
		heartbeats: map[string]context.CancelFunc{},
	}
}

// SetDB selects a Redis logical database (default 0).
func (s *RedisStore) SetDB(db int) *RedisStore { s.client.DB = db; return s }

// SetDialTimeout overrides the per-command dial timeout (default 3s).
func (s *RedisStore) SetDialTimeout(d time.Duration) *RedisStore {
	s.client.DialTimeout = d
	return s
}

// Close stops all background heartbeat goroutines started by UpsertNode.
// It does not delete any keys from Redis; they will simply expire.
func (s *RedisStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	for id, cancel := range s.heartbeats {
		cancel()
		delete(s.heartbeats, id)
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *RedisStore) nodeTTL() time.Duration {
	if s.NodeTTL <= 0 {
		return 30 * time.Second
	}
	return s.NodeTTL
}

func (s *RedisStore) nodeKey(id string) string         { return s.Prefix + "node:" + id }
func (s *RedisStore) nodeScanPattern() string          { return s.Prefix + "node:*" }
func (s *RedisStore) leaseRedisKey(l BindLease) string { return s.Prefix + "lease:" + leaseKey(l) }
func (s *RedisStore) leaseScanPattern() string         { return s.Prefix + "lease:*" }

// ---- Node registry ----

func (s *RedisStore) UpsertNode(ctx context.Context, n Node) error {
	if n.ID == "" {
		return errors.New("node id required")
	}
	if n.LastSeen.IsZero() {
		n.LastSeen = time.Now()
	}
	ttl := s.nodeTTL()
	if err := s.putNode(ctx, n, ttl); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest[n.ID] = n
	if s.closed {
		return nil
	}
	if _, ok := s.heartbeats[n.ID]; !ok {
		hctx, cancel := context.WithCancel(context.Background())
		s.heartbeats[n.ID] = cancel
		s.wg.Add(1)
		go s.heartbeatLoop(hctx, n.ID, ttl)
	}
	return nil
}

func (s *RedisStore) putNode(ctx context.Context, n Node, ttl time.Duration) error {
	data, err := json.Marshal(n)
	if err != nil {
		return err
	}
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	_, err = s.client.do(ctx, "SET", s.nodeKey(n.ID), string(data), "PX", ttlMs)
	return err
}

// heartbeatLoop periodically re-SETs the node's key (with a fresh TTL and
// updated LastSeen) using the most recent snapshot passed to UpsertNode,
// so a node stays registered as long as the process calling UpsertNode is
// alive, even if the caller never invokes Coordinator.Heartbeat again.
func (s *RedisStore) heartbeatLoop(ctx context.Context, id string, ttl time.Duration) {
	defer s.wg.Done()
	interval := ttl / 3
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			n, ok := s.latest[id]
			s.mu.Unlock()
			if !ok {
				return
			}
			n.LastSeen = time.Now()
			cctx, cancel := context.WithTimeout(context.Background(), s.client.dialTimeout()+2*time.Second)
			_ = s.putNode(cctx, n, ttl) // best-effort; a missed refresh just shortens node liveness, not correctness
			cancel()
		}
	}
}

func (s *RedisStore) ListNodes(ctx context.Context) ([]Node, error) {
	keys, err := s.scanKeys(ctx, s.nodeScanPattern())
	if err != nil {
		return nil, err
	}
	out := make([]Node, 0, len(keys))
	for _, k := range keys {
		reply, err := s.client.do(ctx, "GET", k)
		if err != nil {
			return nil, err
		}
		if reply.isNil {
			continue // expired between SCAN and GET
		}
		var n Node
		if err := json.Unmarshal([]byte(reply.str), &n); err != nil {
			continue
		}
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// ---- Bind leases ----

func (s *RedisStore) getLease(ctx context.Context, key string) (BindLease, bool, error) {
	reply, err := s.client.do(ctx, "GET", key)
	if err != nil {
		return BindLease{}, false, err
	}
	if reply.isNil {
		return BindLease{}, false, nil
	}
	var l BindLease
	if err := json.Unmarshal([]byte(reply.str), &l); err != nil {
		return BindLease{}, false, err
	}
	return l, true, nil
}

func (s *RedisStore) TryAcquireBind(ctx context.Context, l BindLease, ttl time.Duration) (BindLease, bool, error) {
	if l.OwnerNode == "" {
		return l, false, errors.New("owner node required")
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	key := s.leaseRedisKey(l)
	ttlMs := strconv.FormatInt(ttl.Milliseconds(), 10)
	now := time.Now()

	candidate := l
	candidate.Version = 1
	candidate.ExpiresAt = now.Add(ttl)
	data, err := json.Marshal(candidate)
	if err != nil {
		return l, false, err
	}

	// Fast, fully-atomic path: nobody holds the lease.
	reply, err := s.client.do(ctx, "SET", key, string(data), "NX", "PX", ttlMs)
	if err != nil {
		return l, false, err
	}
	if !reply.isNil {
		return candidate, true, nil
	}

	// Contested: inspect the current holder.
	existing, ok, err := s.getLease(ctx, key)
	if err != nil {
		return l, false, err
	}
	if !ok {
		// Raced with an expiry/delete between the NX attempt and this GET.
		// Retry the atomic path once before falling back.
		reply2, err := s.client.do(ctx, "SET", key, string(data), "NX", "PX", ttlMs)
		if err != nil {
			return l, false, err
		}
		if !reply2.isNil {
			return candidate, true, nil
		}
		existing, ok, err = s.getLease(ctx, key)
		if err != nil {
			return l, false, err
		}
		if !ok {
			return l, false, errors.New("cluster: lease state unavailable after contention")
		}
	}

	if existing.OwnerNode != l.OwnerNode && existing.ExpiresAt.After(now) && !existing.Draining {
		return existing, false, nil
	}

	// Acquirable: same owner reacquiring, current holder is draining, or
	// its recorded ExpiresAt has already passed. This SET is an
	// unconditional overwrite (documented race window, see file header).
	newLease := l
	newLease.Version = existing.Version + 1
	newLease.ExpiresAt = now.Add(ttl)
	data2, err := json.Marshal(newLease)
	if err != nil {
		return l, false, err
	}
	if _, err := s.client.do(ctx, "SET", key, string(data2), "PX", ttlMs); err != nil {
		return l, false, err
	}
	return newLease, true, nil
}

func (s *RedisStore) RenewBind(ctx context.Context, l BindLease, ttl time.Duration) (BindLease, error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	key := s.leaseRedisKey(l)
	existing, ok, err := s.getLease(ctx, key)
	if err != nil {
		return l, err
	}
	if !ok || existing.OwnerNode != l.OwnerNode {
		return l, errors.New("lease not owned")
	}
	existing.Version++
	existing.ExpiresAt = time.Now().Add(ttl)
	data, err := json.Marshal(existing)
	if err != nil {
		return l, err
	}
	// Documented race window: see file header (GET-then-SET).
	if _, err := s.client.do(ctx, "SET", key, string(data), "PX", strconv.FormatInt(ttl.Milliseconds(), 10)); err != nil {
		return l, err
	}
	return existing, nil
}

func (s *RedisStore) ReleaseBind(ctx context.Context, l BindLease) error {
	key := s.leaseRedisKey(l)
	existing, ok, err := s.getLease(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if existing.OwnerNode != l.OwnerNode {
		return errors.New("lease not owned")
	}
	// Documented race window: see file header (GET-then-DEL).
	_, err = s.client.do(ctx, "DEL", key)
	return err
}

func (s *RedisStore) ListLeases(ctx context.Context) ([]BindLease, error) {
	keys, err := s.scanKeys(ctx, s.leaseScanPattern())
	if err != nil {
		return nil, err
	}
	out := make([]BindLease, 0, len(keys))
	for _, k := range keys {
		reply, err := s.client.do(ctx, "GET", k)
		if err != nil {
			return nil, err
		}
		if reply.isNil {
			continue
		}
		var l BindLease
		if err := json.Unmarshal([]byte(reply.str), &l); err != nil {
			continue
		}
		out = append(out, l)
	}
	return out, nil
}

// ---- helpers ----

func (s *RedisStore) scanKeys(ctx context.Context, pattern string) ([]string, error) {
	var keys []string
	cursor := "0"
	for {
		reply, err := s.client.do(ctx, "SCAN", cursor, "MATCH", pattern, "COUNT", "200")
		if err != nil {
			return nil, err
		}
		if len(reply.arr) != 2 {
			return nil, errors.New("cluster: unexpected SCAN reply shape")
		}
		cursor = reply.arr[0].str
		for _, item := range reply.arr[1].arr {
			keys = append(keys, item.str)
		}
		if cursor == "" || cursor == "0" {
			break
		}
	}
	return keys, nil
}

var _ Store = (*RedisStore)(nil)
