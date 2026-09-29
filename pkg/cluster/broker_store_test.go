package cluster

// Tests for BrokerStore run a real 3-node broker cluster.Raft group inside
// this process using broker's MemoryTransport (an in-process Transport
// intended for embedded/single-process use and tests -- see
// github.com/oarkflow/broker/cluster.NewMemoryTransport), so leader
// election, log replication and quorum commit all genuinely happen; nothing
// here is faked or short-circuited the way MemoryStore is.

import (
	"context"
	"testing"
	"time"

	bcluster "github.com/oarkflow/broker/cluster"
)

type brokerTestCluster struct {
	stores []*BrokerStore
}

func newBrokerTestCluster(t *testing.T, n int) *brokerTestCluster {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = "node-" + string(rune('1'+i))
	}

	transports := make([]*bcluster.MemoryTransport, n)
	registry := map[string]*bcluster.MemoryTransport{}
	for i, id := range ids {
		transports[i] = bcluster.NewMemoryTransport(id)
	}
	for _, tr := range transports {
		tr.Register(registry)
	}

	stores := make([]*BrokerStore, n)
	for i, id := range ids {
		s, err := NewBrokerStore(id, ids, transports[i], WithApplyTimeout(3*time.Second))
		if err != nil {
			t.Fatalf("new broker store %s: %v", id, err)
		}
		stores[i] = s
	}

	tc := &brokerTestCluster{stores: stores}
	t.Cleanup(func() {
		for _, s := range stores {
			_ = s.Close()
		}
	})
	return tc
}

// leader polls until one of the stores becomes Raft leader and returns it.
func (tc *brokerTestCluster) leader(t *testing.T) *BrokerStore {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range tc.stores {
			if s.IsLeader() {
				return s
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected in time")
	return nil
}

func (tc *brokerTestCluster) followers(leader *BrokerStore) []*BrokerStore {
	var out []*BrokerStore
	for _, s := range tc.stores {
		if s != leader {
			out = append(out, s)
		}
	}
	return out
}

func TestBrokerStoreImplementsStore(t *testing.T) {
	var _ Store = (*BrokerStore)(nil)
}

func TestBrokerStoreBindLeaseOwnership(t *testing.T) {
	tc := newBrokerTestCluster(t, 3)
	leader := tc.leader(t)
	ctx := context.Background()

	a := Coordinator{Store: leader, Node: Node{ID: "a", State: NodeActive}, TTL: time.Second}
	b := Coordinator{Store: leader, Node: Node{ID: "b", State: NodeActive}, TTL: time.Second}

	l, ok, err := a.AcquireBind(ctx, "p", "t", "b1")
	if err != nil || !ok || l.OwnerNode != "a" {
		t.Fatalf("bad acquire %+v %v %v", l, ok, err)
	}
	l2, ok, err := b.AcquireBind(ctx, "p", "t", "b1")
	if err != nil || ok || l2.OwnerNode != "a" {
		t.Fatalf("expected existing lease %+v %v %v", l2, ok, err)
	}
}

func TestBrokerStoreRenewAndRelease(t *testing.T) {
	tc := newBrokerTestCluster(t, 3)
	leader := tc.leader(t)
	ctx := context.Background()

	l := BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "a"}
	acquired, ok, err := leader.TryAcquireBind(ctx, l, time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire failed: %v %v", ok, err)
	}
	if acquired.Version != 1 {
		t.Fatalf("expected version 1, got %d", acquired.Version)
	}

	renewed, err := leader.RenewBind(ctx, acquired, time.Second)
	if err != nil {
		t.Fatalf("renew failed: %v", err)
	}
	if renewed.Version != 2 {
		t.Fatalf("expected version 2, got %d", renewed.Version)
	}

	if _, err := leader.RenewBind(ctx, BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "other"}, time.Second); err == nil {
		t.Fatalf("expected renew by non-owner to fail")
	}

	if err := leader.ReleaseBind(ctx, BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "other"}); err == nil {
		t.Fatalf("expected release by non-owner to fail")
	}

	if err := leader.ReleaseBind(ctx, renewed); err != nil {
		t.Fatalf("release failed: %v", err)
	}

	leases, err := leader.ListLeases(ctx)
	if err != nil {
		t.Fatalf("list leases: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("expected no leases after release, got %+v", leases)
	}

	l2, ok, err := leader.TryAcquireBind(ctx, BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "b"}, time.Second)
	if err != nil || !ok || l2.OwnerNode != "b" {
		t.Fatalf("expected b to acquire freed lease: %+v %v %v", l2, ok, err)
	}
}

func TestBrokerStoreDrainingLeaseCanBeStolen(t *testing.T) {
	tc := newBrokerTestCluster(t, 3)
	leader := tc.leader(t)
	ctx := context.Background()

	l := BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "a"}
	acquired, ok, err := leader.TryAcquireBind(ctx, l, 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire failed: %v %v", ok, err)
	}

	acquired.Draining = true
	if _, ok, err := leader.TryAcquireBind(ctx, acquired, 10*time.Second); err != nil || !ok {
		t.Fatalf("marking draining failed: %v %v", ok, err)
	}

	stolen, ok, err := leader.TryAcquireBind(ctx, BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "b"}, 10*time.Second)
	if err != nil || !ok || stolen.OwnerNode != "b" {
		t.Fatalf("expected b to steal draining lease: %+v %v %v", stolen, ok, err)
	}
}

func TestBrokerStoreNodeRegistryAndHeartbeat(t *testing.T) {
	tc := newBrokerTestCluster(t, 3)
	leader := tc.leader(t)
	ctx := context.Background()

	if err := leader.UpsertNode(ctx, Node{ID: "n1", Addr: "10.0.0.1:2775", State: NodeActive}); err != nil {
		t.Fatalf("upsert node: %v", err)
	}
	nodes, err := leader.ListNodes(ctx)
	if err != nil || len(nodes) != 1 || nodes[0].ID != "n1" {
		t.Fatalf("expected 1 node n1, got %+v err=%v", nodes, err)
	}

	if err := leader.UpsertNode(ctx, Node{ID: "n1", Addr: "10.0.0.1:2775", State: NodeActive}); err != nil {
		t.Fatalf("upsert node (heartbeat refresh): %v", err)
	}
	nodes, err = leader.ListNodes(ctx)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("expected node to still be present after refresh, got %+v err=%v", nodes, err)
	}
}

func TestBrokerStoreListNodesEmpty(t *testing.T) {
	tc := newBrokerTestCluster(t, 3)
	leader := tc.leader(t)
	nodes, err := leader.ListNodes(context.Background())
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected no nodes, got %+v", nodes)
	}
}

// TestBrokerStoreFollowerRejectsWrites verifies the honesty claim in
// broker_store.go's doc comment: only the Raft leader may accept writes.
func TestBrokerStoreFollowerRejectsWrites(t *testing.T) {
	tc := newBrokerTestCluster(t, 3)
	leader := tc.leader(t)
	followers := tc.followers(leader)
	if len(followers) == 0 {
		t.Fatal("expected at least one follower")
	}
	follower := followers[0]

	_, _, err := follower.TryAcquireBind(context.Background(), BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "a"}, time.Second)
	if err == nil {
		t.Fatalf("expected follower write to fail")
	}
}

// TestBrokerStoreReplicatesToFollowers is a white-box check that a
// leader-committed lease genuinely propagates through the Raft log to the
// other replicas' state machines, not just the leader's local map.
func TestBrokerStoreReplicatesToFollowers(t *testing.T) {
	tc := newBrokerTestCluster(t, 3)
	leader := tc.leader(t)
	ctx := context.Background()

	l := BindLease{ProviderID: "p", TenantID: "t", BindID: "b1", OwnerNode: "a"}
	if _, ok, err := leader.TryAcquireBind(ctx, l, 10*time.Second); err != nil || !ok {
		t.Fatalf("acquire failed: %v %v", ok, err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for _, follower := range tc.followers(leader) {
		for {
			leases := follower.sm.snapshotLeases()
			if len(leases) == 1 && leases[0].OwnerNode == "a" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("lease did not replicate to follower %s in time, got %+v", follower.id, leases)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}
