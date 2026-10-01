package manager_test

import (
	"context"
	"testing"
	"time"

	"github.com/oarkflow/smppflow/pkg/manager"
	"github.com/oarkflow/smppflow/pkg/protocol"
	"github.com/oarkflow/smppflow/pkg/provider"
)

func TestStoreTenantLifecycle(t *testing.T) {
	ctx := context.Background()
	s := manager.NewStore()
	ten := manager.Tenant{ID: "t1", Name: "Tenant One", Status: manager.TenantActive, TPS: 100, AllowedCountries: []string{"NP"}}
	if err := s.SaveTenant(ctx, ten); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetTenant(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "Tenant One" || got.TPS != 100 {
		t.Fatalf("got %+v", got)
	}
	// Update replaces in place.
	ten.Status = manager.TenantSuspended
	if err := s.SaveTenant(ctx, ten); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetTenant(ctx, "t1")
	if got.Status != manager.TenantSuspended {
		t.Fatalf("status=%v want suspended", got.Status)
	}
	xs, err := s.ListTenants(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(xs) != 1 {
		t.Fatalf("tenants=%d want 1", len(xs))
	}
	if _, err := s.GetTenant(ctx, "missing"); err == nil {
		t.Fatal("unknown tenant must error")
	}
}

func TestStoreBinds(t *testing.T) {
	ctx := context.Background()
	s := manager.NewStore()
	b := manager.BindAccount{TenantID: "t1", SystemID: "sys", BindMode: string(protocol.BindModeTransceiver), MaxBinds: 2, TPS: 50, Enabled: true}
	if err := s.SaveBind(ctx, b); err != nil {
		t.Fatal(err)
	}
	all, err := s.ListBinds(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("binds=%d want 1", len(all))
	}
	scoped, err := s.ListBinds(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(scoped) != 1 {
		t.Fatalf("scoped binds=%d want 1", len(scoped))
	}
	other, err := s.ListBinds(ctx, "t2")
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Fatalf("binds for unknown tenant=%d want 0", len(other))
	}
}

func TestStoreProviders(t *testing.T) {
	ctx := context.Background()
	s := manager.NewStore()
	p := provider.Generic34("prov-1", "127.0.0.1:2775", "sys", "pw")
	if err := s.SaveProvider(ctx, p); err != nil {
		t.Fatal(err)
	}
	xs, err := s.ListProviders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(xs) != 1 || xs[0].ID != "prov-1" {
		t.Fatalf("providers=%+v", xs)
	}
}

func TestStoreSenderApprovals(t *testing.T) {
	ctx := context.Background()
	s := manager.NewStore()
	ap := manager.SenderApproval{TenantID: "t1", Sender: "BRAND", Country: "NP", State: manager.SenderApproved}
	if err := s.SaveSenderApproval(ctx, ap); err != nil {
		t.Fatal(err)
	}
	xs, err := s.ListSenderApprovals(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(xs) != 1 || xs[0].Sender != "BRAND" {
		t.Fatalf("approvals=%+v", xs)
	}
	other, err := s.ListSenderApprovals(ctx, "t2")
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Fatalf("cross-tenant leak: %+v", other)
	}
}

func TestStoreAudit(t *testing.T) {
	ctx := context.Background()
	s := manager.NewStore()
	for i := 0; i < 5; i++ {
		if err := s.Audit(ctx, manager.AuditLog{TenantID: "t1", ActorID: "actor", Action: "x", Resource: "y", ResourceID: "r"}); err != nil {
			t.Fatal(err)
		}
	}
	xs, err := s.ListAudit(ctx, "t1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(xs) != 3 {
		t.Fatalf("audit rows=%d want 3 (limit honoured)", len(xs))
	}
	all, err := s.ListAudit(ctx, "t1", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 5 {
		t.Fatalf("audit rows=%d want 5", len(all))
	}
}

func TestOperationsTenantAndAudit(t *testing.T) {
	ctx := context.Background()
	st := manager.NewStore()
	ops := manager.NewOperations(st, nil, nil, nil, nil)
	if err := ops.UpsertTenant(ctx, "actor-1", manager.Tenant{ID: "t1", Name: "T", Status: manager.TenantActive}); err != nil {
		t.Fatal(err)
	}
	// Upserting must write an audit entry naming the actor.
	xs, err := st.ListAudit(ctx, "t1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(xs) == 0 {
		t.Fatal("UpsertTenant must record an audit entry")
	}
	if xs[0].ActorID != "actor-1" {
		t.Fatalf("actor=%q want actor-1", xs[0].ActorID)
	}
	if err := ops.SuspendTenant(ctx, "actor-2", "t1", "nonpayment"); err != nil {
		t.Fatal(err)
	}
	got, _ := st.GetTenant(ctx, "t1")
	if got.Status != manager.TenantSuspended {
		t.Fatalf("status=%v want suspended", got.Status)
	}
}

func TestOperationsRequiresOps(t *testing.T) {
	ctx := context.Background()
	ops := manager.NewOperations(nil, nil, nil, nil, nil)
	if ops.Store == nil {
		t.Fatal("NewOperations must install a default store")
	}
	if err := ops.UpsertTenant(ctx, "a", manager.Tenant{ID: "t"}); err != nil {
		t.Fatal(err)
	}
}

func TestStoreAuditLimitZeroOrNegative(t *testing.T) {
	ctx := context.Background()
	s := manager.NewStore()
	_ = s.Audit(ctx, manager.AuditLog{TenantID: "t1", Action: "a"})
	// A non-positive limit must not panic; it should return nothing or everything
	// depending on implementation, but always a well-formed slice.
	xs, err := s.ListAudit(ctx, "t1", 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = xs
}

func TestStoreListAuditIsTenantScoped(t *testing.T) {
	ctx := context.Background()
	s := manager.NewStore()
	_ = s.Audit(ctx, manager.AuditLog{TenantID: "t1", Action: "a"})
	_ = s.Audit(ctx, manager.AuditLog{TenantID: "t2", Action: "b"})
	xs, err := s.ListAudit(ctx, "t1", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range xs {
		if a.TenantID != "t1" {
			t.Fatalf("cross-tenant audit leak: %+v", a)
		}
	}
}

func TestStoreTimestampsAreSet(t *testing.T) {
	ctx := context.Background()
	s := manager.NewStore()
	before := time.Now().Add(-time.Second)
	if err := s.SaveTenant(ctx, manager.Tenant{ID: "t1", Name: "T"}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetTenant(ctx, "t1")
	if got.CreatedAt.Before(before) {
		t.Fatalf("CreatedAt not stamped: %v", got.CreatedAt)
	}
}
