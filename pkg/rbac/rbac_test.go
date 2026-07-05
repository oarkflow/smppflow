package rbac

import "testing"

func TestPolicyTenantIsolationAndAPIKey(t *testing.T) {
	p := NewPolicy()
	dec := p.Check(Principal{ID: "u", TenantID: "t1", Roles: []Role{RoleSupport}}, "t2", "message", "read")
	if dec.Allowed {
		t.Fatalf("expected tenant isolation denial")
	}
	keys := NewKeyStore()
	k, secret, err := keys.Create("t1", "api", []string{"message:read"}, 0)
	if err != nil || k.ID == "" || secret == "" {
		t.Fatalf("create key %v", err)
	}
	pr, err := keys.Authenticate(secret)
	if err != nil {
		t.Fatalf("auth %v", err)
	}
	if !p.Check(pr, "t1", "message", "read").Allowed {
		t.Fatalf("scope should allow")
	}
	if err := keys.Disable(k.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := keys.Authenticate(secret); err == nil {
		t.Fatalf("disabled key authenticated")
	}
}
