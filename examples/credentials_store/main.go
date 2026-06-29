package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/smppflow/pkg/account"
)

func main() {
	ctx := context.Background()
	st := account.NewMemory()
	_ = st.SaveUser(ctx, account.User{ID: "u-100", TenantID: "tenant-a", Username: "ops", Enabled: true, Roles: []string{"admin", "sender"}})
	key, secret, _ := account.NewAPIKey("tenant-a", "u-100", "ops-key")
	_ = st.SaveAPIKey(ctx, key)
	_ = st.SaveProviderCredential(ctx, account.ProviderCredential{TenantID: "tenant-a", ProviderID: "smpp-a", SystemID: "sys-a", Password: "super-secret", Enabled: true})
	user, apiKey, err := st.AuthenticateAPIKey(ctx, secret)
	if err != nil {
		panic(err)
	}
	cred, _ := st.GetProviderCredential(ctx, "tenant-a", "smpp-a")
	fmt.Println("user", user.Username, "api-key-prefix", apiKey.Prefix)
	fmt.Printf("credential %+v\n", account.RedactProviderCredential(cred))
}
