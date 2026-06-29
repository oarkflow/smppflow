package account

import (
	"context"
	"testing"
)

func TestMemoryUsersAPIKeysAndProviderCredentials(t *testing.T) {
	ctx := context.Background()
	st := NewMemory()
	if err := st.SaveUser(ctx, User{ID: "u1", TenantID: "t1", Username: "alice", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	key, secret, err := NewAPIKey("t1", "u1", "prod")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SaveAPIKey(ctx, key); err != nil {
		t.Fatal(err)
	}
	u, k, err := st.AuthenticateAPIKey(ctx, secret)
	if err != nil {
		t.Fatal(err)
	}
	if u.Username != "alice" || k.UserID != "u1" {
		t.Fatalf("unexpected auth result: %+v %+v", u, k)
	}
	if _, _, err := st.AuthenticateAPIKey(ctx, "bad"); err != ErrInvalidCredential {
		t.Fatalf("expected invalid credential, got %v", err)
	}
	cred := ProviderCredential{TenantID: "t1", ProviderID: "p1", SystemID: "sys", Password: "secret", Enabled: true}
	if err := st.SaveProviderCredential(ctx, cred); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetProviderCredential(ctx, "t1", "p1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Password != "secret" {
		t.Fatalf("password not stored")
	}
	redacted := RedactProviderCredential(got)
	if redacted.Password == "secret" {
		t.Fatalf("credential was not redacted")
	}
}
