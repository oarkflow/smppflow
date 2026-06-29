package cluster

import (
	"context"
	"testing"
	"time"
)

func TestBindLeaseOwnership(t *testing.T) {
	st := NewMemoryStore()
	a := Coordinator{Store: st, Node: Node{ID: "a", State: NodeActive}, TTL: time.Second}
	b := Coordinator{Store: st, Node: Node{ID: "b", State: NodeActive}, TTL: time.Second}
	l, ok, err := a.AcquireBind(context.Background(), "p", "t", "b1")
	if err != nil || !ok || l.OwnerNode != "a" {
		t.Fatalf("bad acquire %+v %v %v", l, ok, err)
	}
	l2, ok, err := b.AcquireBind(context.Background(), "p", "t", "b1")
	if err != nil || ok || l2.OwnerNode != "a" {
		t.Fatalf("expected existing lease %+v %v %v", l2, ok, err)
	}
}
